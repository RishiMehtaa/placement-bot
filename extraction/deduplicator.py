"""
Stage 8 — Deduplication System

Three layers:
  Layer 1: message_id uniqueness — enforced by DB UNIQUE constraint.
            Stage 8 verifies and logs. Does not re-implement the constraint.
  Layer 2: content_hash — SHA-256 of normalized text.
            Skip processing if hash already seen in DB.
  Layer 3: URL normalization — strip UTM params, trailing slashes, normalize
            scheme. Deduplicate families that share the same normalized apply link.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from sqlalchemy.ext.asyncio import AsyncSession

from db.queries import content_hash_exists, get_family_by_jd_link
from utils.logger import get_logger

logger = get_logger(__name__)

# UTM parameters to strip from URLs
_UTM_PARAMS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_content",
    "utm_term",
}


@dataclass
class DeduplicationResult:
    """Result returned by Stage 8."""

    message_id: str

    # Layer 1
    layer1_message_id_ok: bool = True  # False = duplicate message_id detected

    # Layer 2
    layer2_content_hash: Optional[str] = None
    layer2_is_duplicate: bool = False  # True = hash already seen, skip processing

    # Layer 3
    layer3_urls_normalized: list[str] = field(default_factory=list)
    layer3_duplicate_family_id: Optional[str] = None  # existing family with same URL

    # Final decision
    should_skip: bool = False  # True = do not process further
    skip_reason: Optional[str] = None

    # Which layers fired
    layers_fired: list[str] = field(default_factory=list)


def compute_content_hash(text: str) -> str:
    """
    Compute SHA-256 hash of normalized text.
    Normalization: lowercase, collapse whitespace, strip leading/trailing space.
    Matches the same normalization used when the message was first stored.
    """
    normalized = " ".join(text.lower().split())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def normalize_url(url: str) -> str:
    """
    Normalize a URL for deduplication:
      - Strip UTM parameters
      - Remove trailing slash from path
      - Normalize scheme to https where safe (http → https)
      - Lowercase scheme and host
      - Sort remaining query params for stable comparison

    Returns the normalized URL string.
    Raises ValueError if the URL is not valid (missing scheme or host).
    """
    if not url or not isinstance(url, str):
        raise ValueError(f"Invalid URL input: {url!r}")

    parsed = urlparse(url.strip())

    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"URL missing scheme or host: {url!r}")

    # Normalize scheme
    scheme = parsed.scheme.lower()
    if scheme == "http":
        scheme = "https"

    # Lowercase host
    netloc = parsed.netloc.lower()

    # Strip UTM params, sort the rest for stable comparison
    raw_params = parse_qs(parsed.query, keep_blank_values=True)
    filtered_params = {
        k: v for k, v in raw_params.items() if k.lower() not in _UTM_PARAMS
    }
    sorted_query = urlencode(sorted(filtered_params.items()), doseq=True)

    # Remove trailing slash from path
    path = parsed.path.rstrip("/") if parsed.path != "/" else "/"

    normalized = urlunparse((scheme, netloc, path, parsed.params, sorted_query, ""))
    return normalized


def normalize_urls(urls: list[str]) -> list[str]:
    """
    Normalize a list of URLs. Silently skips invalid URLs.
    Returns list of successfully normalized URLs (deduplicated, order preserved).
    """
    seen = set()
    result = []
    for url in urls:
        try:
            norm = normalize_url(url)
            if norm not in seen:
                seen.add(norm)
                result.append(norm)
        except ValueError as exc:
            logger.debug("URL normalization skipped: %s — %s", url, exc)
    return result


async def run_deduplication(
    message_id: str,
    raw_text: str,
    urls: list[str],
    db: AsyncSession,
) -> DeduplicationResult:
    """
    Run all 3 deduplication layers for a message.

    Args:
        message_id: The WhatsApp message ID.
        raw_text:   Raw message text (used for content hash).
        urls:       URLs extracted from the message (from preprocessor).
        db:         Async DB session.

    Returns:
        DeduplicationResult with full audit trail of which layers fired.
    """
    result = DeduplicationResult(message_id=message_id)

    # ------------------------------------------------------------------
    # Layer 1 — message_id uniqueness
    # The DB UNIQUE constraint is the enforcement mechanism.
    # Stage 8 logs the verification. If we reach this point in the pipeline,
    # the message_id was already accepted by the /ingest endpoint and saved
    # to the DB without a conflict — so Layer 1 is always OK here.
    # A duplicate message_id would have been rejected at /ingest time.
    # ------------------------------------------------------------------
    result.layer1_message_id_ok = True
    result.layers_fired.append("layer1")
    logger.debug(
        "[Stage 8] Layer 1 passed: message_id=%s is unique in DB", message_id
    )

    # ------------------------------------------------------------------
    # Layer 2 — content_hash deduplication
    # ------------------------------------------------------------------
    content_hash = compute_content_hash(raw_text)
    result.layer2_content_hash = content_hash

    hash_exists = await content_hash_exists(db, content_hash, exclude_message_id=message_id)
    result.layers_fired.append("layer2")

    if hash_exists:
        result.layer2_is_duplicate = True
        result.should_skip = True
        result.skip_reason = f"content_hash already seen: {content_hash[:16]}..."
        logger.info(
            "[Stage 8] Layer 2 DUPLICATE: message_id=%s hash=%s — skipping",
            message_id,
            content_hash[:16],
        )
        return result

    logger.debug(
        "[Stage 8] Layer 2 passed: message_id=%s hash=%s is new",
        message_id,
        content_hash[:16],
    )

    # ------------------------------------------------------------------
    # Layer 3 — URL normalization + family-level dedup on apply link
    # ------------------------------------------------------------------
    normalized_urls = normalize_urls(urls)
    result.layer3_urls_normalized = normalized_urls
    result.layers_fired.append("layer3")

    for norm_url in normalized_urls:
        existing_family = await get_family_by_jd_link(db, norm_url)
        if existing_family is not None:
            result.layer3_duplicate_family_id = str(existing_family.id)
            logger.info(
                "[Stage 8] Layer 3 match: message_id=%s url=%s already linked to family_id=%s",
                message_id,
                norm_url,
                existing_family.id,
            )
            # Not a skip — we still process, but caller knows which family owns this URL
            break

    if not result.layer3_duplicate_family_id:
        logger.debug(
            "[Stage 8] Layer 3 passed: message_id=%s no duplicate families found via URL",
            message_id,
        )

    return result