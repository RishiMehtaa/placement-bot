"""
Stage 4 — LLM Extractor
Calls Groq (llama-3.1-8b-instant) only when company or role is still None after Stage 3.
Enforces daily call cap. Caches by SHA-256 of cleaned text. Rejects low-confidence
or malformed responses and logs them to dead_letter_queue.
"""

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, date, timezone
from typing import Optional

from groq import Groq

from config.settings import settings
from extraction.preprocessor import PreprocessedMessage
from extraction.context_resolver import ContextResolvedFields
from utils.logger import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = (
    "You extract placement opportunity information from WhatsApp messages.\n"
    "These are messages from an official Indian college (SVKM's Dwarkadas. J. Sanghvi College of Engineering) placement group.\n"
    "Return ONLY valid JSON with exactly these keys: company, role, confidence, reasoning.\n"
    "confidence must be a float between 0.0 and 1.0 indicating how certain you are.\n"
    "If a field cannot be determined confidently, return null.\n"
    "Never guess. Never invent company names."
)

_PERSON_NAME_INDICATORS = re.compile(
    r"\b(sir|ma'am|maam|mr|mrs|ms|dr|prof|professor|he|she|they|contact|"
    r"please|kindly|regards|team|admin|coordinator|placement officer)\b",
    re.IGNORECASE,
)

_VALID_COMPANY_RE = re.compile(r"[A-Za-z]")

# In-memory cache: {cache_key: {"company": ..., "role": ..., "confidence": ...,
#                               "reasoning": ..., "cached_at": datetime}}
_cache: dict[str, dict] = {}

# Daily call tracker: {"date": date, "count": int}
_daily_tracker: dict = {"date": date.today(), "count": 0}


# ---------------------------------------------------------------------------
# Dataclass
# ---------------------------------------------------------------------------

@dataclass
class LLMExtractedFields:
    company: Optional[str]
    role: Optional[str]
    confidence: float
    reasoning: Optional[str]
    source: str  # "llm" | "cache" | "skipped"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _cache_key(cleaned_text: str) -> str:
    """SHA-256 of the cleaned message text — used as cache key."""
    return hashlib.sha256(cleaned_text.encode("utf-8")).hexdigest()


def _is_cache_hit(key: str) -> bool:
    """Return True if key exists in cache and has not expired."""
    if key not in _cache:
        return False
    entry = _cache[key]
    cached_at: datetime = entry["cached_at"]
    now = datetime.now(timezone.utc)
    age_hours = (now - cached_at).total_seconds() / 3600
    if age_hours > settings.LLM_CACHE_TTL_HOURS:
        del _cache[key]
        return False
    return True


def _get_from_cache(key: str) -> dict:
    return _cache[key]


def _store_in_cache(key: str, company: Optional[str], role: Optional[str],
                    confidence: float, reasoning: Optional[str]) -> None:
    _cache[key] = {
        "company": company,
        "role": role,
        "confidence": confidence,
        "reasoning": reasoning,
        "cached_at": datetime.now(timezone.utc),
    }


def _daily_limit_reached() -> bool:
    """Reset counter on new day; return True if today's limit is exhausted."""
    today = date.today()
    if _daily_tracker["date"] != today:
        _daily_tracker["date"] = today
        _daily_tracker["count"] = 0
    return _daily_tracker["count"] >= settings.LLM_DAILY_CALL_LIMIT


def _increment_daily_count() -> None:
    today = date.today()
    if _daily_tracker["date"] != today:
        _daily_tracker["date"] = today
        _daily_tracker["count"] = 0
    _daily_tracker["count"] += 1


def _is_valid_company(company: Optional[str]) -> bool:
    """
    Return False if company looks like a person name, generic noise word,
    or is too short to be a real company name.
    """
    if not company:
        return False
    company = company.strip()
    if len(company) < 2:
        return False
    if not _VALID_COMPANY_RE.search(company):
        return False
    if _PERSON_NAME_INDICATORS.search(company):
        return False
    return True


def _parse_llm_response(raw: str) -> dict:
    """
    Parse raw LLM text into a dict.
    Strips markdown fences if present. Raises ValueError on bad JSON.
    """
    cleaned = raw.strip()
    cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    cleaned = cleaned.strip()
    return json.loads(cleaned)


def _call_groq(user_text: str) -> dict:
    """
    Call Groq API and return parsed JSON dict.
    Raises ValueError on bad JSON or API error.
    """
    client = Groq(api_key=settings.LLM_API_KEY)
    response = client.chat.completions.create(
        model=settings.LLM_MODEL,
        max_tokens=settings.LLM_MAX_TOKENS,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_text},
        ],
        temperature=0,
    )
    raw_text = response.choices[0].message.content or ""
    return _parse_llm_response(raw_text)


# ---------------------------------------------------------------------------
# Dead letter logging
# ---------------------------------------------------------------------------

def _log_dead_letter(message_id: str, failure_reason: str, raw_payload: dict) -> None:
    logger.warning(
        f"LLM dead letter | message_id={message_id} | reason={failure_reason} "
        f"| payload={json.dumps(raw_payload)}"
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def extract_with_llm(
    preprocessed: PreprocessedMessage,
    context_fields: ContextResolvedFields,
) -> LLMExtractedFields:
    """
    Stage 4 entry point.

    Calls LLM only when company or role is still None after Stage 3.
    Returns LLMExtractedFields with source="skipped" if LLM is not needed.
    Returns LLMExtractedFields with source="cache" if a valid cache hit exists.
    Returns LLMExtractedFields with source="llm" after a real API call.
    Logs rejections to dead letter and returns source="skipped" on rejection.
    """
    message_id = preprocessed.message_id
    needs_company = context_fields.company is None
    needs_role = context_fields.role is None

    # If both company and role are already resolved, skip entirely
    if not needs_company and not needs_role:
        logger.info(
            f"Stage 4 | message_id={message_id} | source=skipped "
            f"| reason=company_and_role_already_resolved"
        )
        return LLMExtractedFields(
            company=None,
            role=None,
            confidence=0.0,
            reasoning=None,
            source="skipped",
        )

    key = _cache_key(preprocessed.cleaned_text)

    # Cache hit
    if _is_cache_hit(key):
        entry = _get_from_cache(key)
        logger.info(
            f"Stage 4 | message_id={message_id} | source=cache "
            f"| company={entry['company']} | role={entry['role']} "
            f"| confidence={entry['confidence']}"
        )
        return LLMExtractedFields(
            company=entry["company"] if needs_company else None,
            role=entry["role"] if needs_role else None,
            confidence=entry["confidence"],
            reasoning=entry["reasoning"],
            source="cache",
        )

    # Daily limit check
    if _daily_limit_reached():
        logger.warning(
            f"Stage 4 | message_id={message_id} | source=skipped "
            f"| reason=daily_limit_reached | limit={settings.LLM_DAILY_CALL_LIMIT}"
        )
        return LLMExtractedFields(
            company=None,
            role=None,
            confidence=0.0,
            reasoning=None,
            source="skipped",
        )

    # Build user prompt
    user_text = (
        f"Extract the company name and job role from this placement message.\n\n"
        f"Message: {preprocessed.cleaned_text}"
    )
    if context_fields.company:
        user_text += f"\n\nHint — company already known from context: {context_fields.company}"
    if context_fields.role:
        user_text += f"\n\nHint — role already known from context: {context_fields.role}"

    # Call Groq
    try:
        _increment_daily_count()
        parsed = _call_groq(user_text)
    except Exception as exc:
        failure_reason = f"llm_call_failed: {exc}"
        logger.error(f"Stage 4 | message_id={message_id} | {failure_reason}")
        _log_dead_letter(message_id, failure_reason, {"cleaned_text": preprocessed.cleaned_text})
        return LLMExtractedFields(
            company=None,
            role=None,
            confidence=0.0,
            reasoning=None,
            source="skipped",
        )

    # Validate response structure
    if not isinstance(parsed, dict):
        failure_reason = "response_not_a_dict"
        _log_dead_letter(message_id, failure_reason, {"raw": str(parsed)})
        logger.warning(f"Stage 4 | message_id={message_id} | rejected | reason={failure_reason}")
        return LLMExtractedFields(
            company=None, role=None, confidence=0.0, reasoning=None, source="skipped"
        )

    company: Optional[str] = parsed.get("company")
    role: Optional[str] = parsed.get("role")
    confidence: float = float(parsed.get("confidence", 0.0))
    reasoning: Optional[str] = parsed.get("reasoning")

    # Reject low confidence
    if confidence < 0.5:
        failure_reason = f"confidence_too_low: {confidence}"
        _log_dead_letter(message_id, failure_reason, parsed)
        logger.warning(f"Stage 4 | message_id={message_id} | rejected | reason={failure_reason}")
        return LLMExtractedFields(
            company=None, role=None, confidence=0.0, reasoning=reasoning, source="skipped"
        )

    # Validate company name
    if company is not None and not _is_valid_company(company):
        failure_reason = f"invalid_company_name: {company}"
        _log_dead_letter(message_id, failure_reason, parsed)
        logger.warning(f"Stage 4 | message_id={message_id} | rejected | reason={failure_reason}")
        company = None

    # Store valid result in cache
    _store_in_cache(key, company, role, confidence, reasoning)

    logger.info(
        f"Stage 4 | message_id={message_id} | source=llm "
        f"| company={company} | role={role} | confidence={confidence}"
    )

    return LLMExtractedFields(
        company=company if needs_company else None,
        role=role if needs_role else None,
        confidence=confidence,
        reasoning=reasoning,
        source="llm",
    )