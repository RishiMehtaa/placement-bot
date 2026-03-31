"""
Stage 2 — Regex Extractor
Extracts deadline, package, and JD link from a PreprocessedMessage.
No LLM. No guessing. Unmatched fields return None.
"""

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional
from utils.logger import get_logger
from extraction.preprocessor import PreprocessedMessage

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Output dataclass
# ---------------------------------------------------------------------------

@dataclass
class RegexExtractedFields:
    deadline_raw: Optional[str] = None
    deadline_normalized: Optional[datetime] = None
    package_raw: Optional[str] = None
    package_normalized: Optional[str] = None
    jd_link: Optional[str] = None
    confidence: float = 0.0


# ---------------------------------------------------------------------------
# Deadline patterns
# ---------------------------------------------------------------------------

# Month name lookup
MONTH_MAP = {
    "january": 1, "jan": 1,
    "february": 2, "feb": 2,
    "march": 3, "mar": 3,
    "april": 4, "apr": 4,
    "may": 5,
    "june": 6, "jun": 6,
    "july": 7, "jul": 7,
    "august": 8, "aug": 8,
    "september": 9, "sep": 9, "sept": 9,
    "october": 10, "oct": 10,
    "november": 11, "nov": 11,
    "december": 12, "dec": 12,
}

# Day-of-week for relative resolution
DOW_MAP = {
    "monday": 0, "tuesday": 1, "wednesday": 2,
    "thursday": 3, "friday": 4, "saturday": 5, "sunday": 6,
}

# Ordered list of (pattern, handler_key) tuples
# All patterns match on already-lowercased cleaned_text
DEADLINE_PATTERNS = [
    # 25/03/2025 or 25/03
    (
        r'\b(\d{1,2})[\/\-](\d{1,2})(?:[\/\-](\d{2,4}))?\b',
        "dmy_slash",
    ),
    # 25 march / 25th march
    (
        r'\b(\d{1,2})(?:st|nd|rd|th)?\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec)\b',
        "day_month",
    ),
    # march 25 / march 25th
    (
        r'\b(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec)\s+(\d{1,2})(?:st|nd|rd|th)?\b',
        "month_day",
    ),
    # tomorrow
    (
        r'\btomorrow\b',
        "tomorrow",
    ),
    # this friday / next friday
    (
        r'\b(?:this|next)\s+(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b',
        "this_dow",
    ),
    # this week
    (
        r'\bthis\s+week\b',
        "this_week",
    ),
    # by 5pm / by EOD / by midnight / by end of day
    (
        r'\bby\s+(eod|end\s+of\s+day|midnight|noon|\d{1,2}(?::\d{2})?\s*(?:am|pm))\b',
        "by_time",
    ),
]


def _current_year() -> int:
    return datetime.now(timezone.utc).year


def _resolve_deadline(match: re.Match, handler: str, text: str) -> tuple[Optional[str], Optional[datetime]]:
    """
    Given a regex match and its handler key, return (raw_string, normalized_datetime).
    All datetimes are returned as timezone-aware UTC end-of-day (23:59:59) unless time is specified.
    Returns (None, None) if normalization fails.
    """
    now = datetime.now(timezone.utc)
    raw = match.group(0)

    try:
        if handler == "dmy_slash":
            day = int(match.group(1))
            month = int(match.group(2))
            year_str = match.group(3)
            year = int(year_str) if year_str else _current_year()
            if year < 100:
                year += 2000
            # Validate ranges
            if not (1 <= day <= 31 and 1 <= month <= 12):
                return raw, None
            dt = datetime(year, month, day, 23, 59, 59, tzinfo=timezone.utc)
            return raw, dt

        elif handler == "day_month":
            day = int(match.group(1))
            month = MONTH_MAP.get(match.group(2).lower())
            if not month:
                return raw, None
            year = _current_year()
            dt = datetime(year, month, day, 23, 59, 59, tzinfo=timezone.utc)
            # If date already passed this year, assume next year
            if dt < now:
                dt = dt.replace(year=year + 1)
            return raw, dt

        elif handler == "month_day":
            month = MONTH_MAP.get(match.group(1).lower())
            day = int(match.group(2))
            if not month:
                return raw, None
            year = _current_year()
            dt = datetime(year, month, day, 23, 59, 59, tzinfo=timezone.utc)
            if dt < now:
                dt = dt.replace(year=year + 1)
            return raw, dt

        elif handler == "tomorrow":
            dt = (now + timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=0)
            return raw, dt

        elif handler == "this_dow":
            # Extract the day name (second word after "this" or "next")
            parts = raw.lower().split()
            dow_name = parts[-1]
            target_dow = DOW_MAP.get(dow_name)
            if target_dow is None:
                return raw, None
            days_ahead = (target_dow - now.weekday()) % 7
            if days_ahead == 0:
                days_ahead = 7  # "this friday" when today is friday = next friday
            dt = (now + timedelta(days=days_ahead)).replace(hour=23, minute=59, second=59, microsecond=0)
            return raw, dt

        elif handler == "this_week":
            # End of current week = this Sunday 23:59:59
            days_until_sunday = (6 - now.weekday()) % 7
            if days_until_sunday == 0:
                days_until_sunday = 7
            dt = (now + timedelta(days=days_until_sunday)).replace(hour=23, minute=59, second=59, microsecond=0)
            return raw, dt

        elif handler == "by_time":
            # We know deadline is today — time is approximate, use today EOD
            dt = now.replace(hour=23, minute=59, second=59, microsecond=0)
            return raw, dt

    except (ValueError, TypeError) as e:
        logger.warning(f"Deadline normalization failed for raw='{raw}' handler='{handler}': {e}")
        return raw, None

    return raw, None


def extract_deadline(text: str) -> tuple[Optional[str], Optional[datetime]]:
    """
    Try all deadline patterns in order. Return first successful match.
    Returns (deadline_raw, deadline_normalized).
    """
    for pattern, handler in DEADLINE_PATTERNS:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            raw, normalized = _resolve_deadline(match, handler, text)
            if raw:
                logger.debug(f"Deadline matched handler='{handler}' raw='{raw}' normalized='{normalized}'")
                return raw, normalized
    return None, None


# ---------------------------------------------------------------------------
# Package patterns
# ---------------------------------------------------------------------------

PACKAGE_PATTERNS = [
    # stipend: 30k / stipend of 25000 / stipend 30k
    (
        r'\bstipend[\s:of]+(\d[\d,]*(?:\.\d+)?)\s*(k|lpa|lakh|lakhs|per\s+annum|per\s+month|\/month|\/mo)?\b',
        "stipend",
    ),
    # 20 LPA / 12 LPA
    (
        r'\b(\d[\d,]*(?:\.\d+)?)\s*lpa\b',
        "lpa",
    ),
    # 12 lakh / 12 lakhs per annum / 12 lakhs
    (
        r'\b(\d[\d,]*(?:\.\d+)?)\s*lakh(?:s)?(?:\s+per\s+annum)?\b',
        "lakh",
    ),
    # ₹50,000/month or ₹50000/month
    (
        r'[₹rs\.]+\s*(\d[\d,]*(?:\.\d+)?)\s*(?:\/month|\/mo|per\s+month)\b',
        "inr_month",
    ),
    # 50k/month
    (
        r'\b(\d[\d,]*(?:\.\d+)?)\s*k\s*(?:\/month|\/mo|per\s+month)\b',
        "k_month",
    ),
    # CTC: 20 LPA (handles "ctc" prefix)
    (
        r'\bctc[\s:]+(\d[\d,]*(?:\.\d+)?)\s*(lpa|lakh|lakhs)?\b',
        "ctc",
    ),
    # package: 20 LPA
    (
        r'\bpackage[\s:]+(\d[\d,]*(?:\.\d+)?)\s*(lpa|lakh|lakhs)?\b',
        "package_prefix",
    ),
]


def _normalize_package(raw: str, amount_str: str, unit: Optional[str], handler: str) -> str:
    """
    Normalize package to a human-readable canonical string.
    Examples: "20 LPA", "₹50,000/month", "30k/month stipend"
    """
    # Clean amount
    amount_str = amount_str.replace(",", "").strip()
    try:
        amount = float(amount_str)
    except ValueError:
        return raw.strip()

    unit = (unit or "").lower().strip()

    if handler == "stipend":
        if unit == "k":
            return f"₹{int(amount * 1000):,}/month (stipend)"
        elif unit in ("lpa", "lakh", "lakhs", "per annum"):
            return f"{amount} LPA (stipend)"
        elif unit in ("/month", "/mo", "per month"):
            return f"₹{int(amount):,}/month (stipend)"
        else:
            # bare number — treat as monthly
            return f"₹{int(amount):,}/month (stipend)"

    elif handler == "lpa":
        return f"{amount} LPA"

    elif handler == "lakh":
        return f"{amount} LPA"

    elif handler == "inr_month":
        return f"₹{int(amount):,}/month"

    elif handler == "k_month":
        return f"₹{int(amount * 1000):,}/month"

    elif handler == "ctc":
        if unit in ("lakh", "lakhs"):
            return f"{amount} LPA (CTC)"
        return f"{amount} LPA (CTC)"

    elif handler == "package_prefix":
        if unit in ("lakh", "lakhs"):
            return f"{amount} LPA"
        return f"{amount} LPA"

    return raw.strip()


def extract_package(text: str) -> tuple[Optional[str], Optional[str]]:
    """
    Try all package patterns in order. Return first successful match.
    Returns (package_raw, package_normalized).
    """
    for pattern, handler in PACKAGE_PATTERNS:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            raw = match.group(0)
            amount_str = match.group(1)
            unit = match.group(2) if match.lastindex and match.lastindex >= 2 else None
            normalized = _normalize_package(raw, amount_str, unit, handler)
            logger.debug(f"Package matched handler='{handler}' raw='{raw}' normalized='{normalized}'")
            return raw, normalized
    return None, None


# ---------------------------------------------------------------------------
# JD link selection
# ---------------------------------------------------------------------------

# Keywords that suggest a URL is an apply/JD link
APPLY_KEYWORDS = ["apply", "form", "register", "careers", "jobs", "application", "recruit", "hiring"]


def extract_jd_link(urls: list[str]) -> Optional[str]:
    """
    From the list of URLs extracted by the preprocessor, pick the most likely JD/apply link.
    Preference order:
      1. URL containing any APPLY_KEYWORDS
      2. First URL in the list (fallback)
    Returns None if urls is empty.
    """
    if not urls:
        return None

    for url in urls:
        url_lower = url.lower()
        if any(kw in url_lower for kw in APPLY_KEYWORDS):
            logger.debug(f"JD link selected (keyword match): {url}")
            return url

    # Fallback: first URL
    logger.debug(f"JD link selected (first URL fallback): {urls[0]}")
    return urls[0]


# ---------------------------------------------------------------------------
# Confidence scoring
# ---------------------------------------------------------------------------

def _compute_confidence(fields: RegexExtractedFields) -> float:
    """
    Simple additive confidence based on how many fields were successfully extracted.
    deadline_normalized: +0.4
    package_normalized:  +0.3
    jd_link:             +0.3
    Raw-only (no normalized): half credit for deadline (+0.2)
    """
    score = 0.0

    if fields.deadline_normalized is not None:
        score += 0.4
    elif fields.deadline_raw is not None:
        score += 0.2  # partial: matched but could not normalize

    if fields.package_normalized is not None:
        score += 0.3

    if fields.jd_link is not None:
        score += 0.3

    return round(min(score, 1.0), 2)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def extract_with_regex(preprocessed: PreprocessedMessage) -> RegexExtractedFields:
    """
    Stage 2 entry point.
    Accepts a PreprocessedMessage, returns RegexExtractedFields.
    Only runs on processable messages — caller must check preprocessed.is_processable.
    """
    logger.info(f"[Stage 2] Regex extraction starting for message_id={preprocessed.message_id}")

    fields = RegexExtractedFields()

    # Extract deadline
    fields.deadline_raw, fields.deadline_normalized = extract_deadline(preprocessed.cleaned_text)

    # Extract package
    fields.package_raw, fields.package_normalized = extract_package(preprocessed.cleaned_text)

    # Extract JD link from URLs already pulled by preprocessor
    fields.jd_link = extract_jd_link(preprocessed.urls)

    # Score confidence
    fields.confidence = _compute_confidence(fields)

    logger.info(
        f"[Stage 2] Regex extraction complete for message_id={preprocessed.message_id} | "
        f"deadline_raw={fields.deadline_raw!r} | "
        f"package_raw={fields.package_raw!r} | "
        f"jd_link={fields.jd_link!r} | "
        f"confidence={fields.confidence}"
    )

    return fields