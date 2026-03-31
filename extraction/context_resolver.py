"""
Stage 3 — Context Resolver
Resolves company and role by examining surrounding messages.
Three priority levels:
  Priority 1: reply_to_id match        → confidence 0.95
  Priority 2: sliding window (last 5)  → confidence 0.60
  Priority 3: no context available     → confidence 0.30
"""

import re
from dataclasses import dataclass
from typing import Optional

from db.models import Message
from utils.logger import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Output dataclass
# ---------------------------------------------------------------------------

@dataclass
class ContextResolvedFields:
    company: Optional[str]
    role: Optional[str]
    context_source: str        # "reply" | "window" | "none"
    confidence: float


# ---------------------------------------------------------------------------
# Confidence constants
# ---------------------------------------------------------------------------

CONFIDENCE_REPLY  = 0.95
CONFIDENCE_WINDOW = 0.60
CONFIDENCE_NONE   = 0.30


# ---------------------------------------------------------------------------
# Shared boundary — stops capture before these filler words/patterns
# ---------------------------------------------------------------------------

# Non-capturing lookahead: stop before these words or end-of-string
_STOP = r'(?=\s+(?:for|at|by|from|in|on|before|next|this|apply|please|\|)|[\s]*$|[,\.;|\|])'

# A single capitalised "word token" — letters, digits, slash, hyphen, parens
_W = r'[A-Za-z0-9\/\-\(\)]+'

# 1–4 word company name (stops before _STOP boundary)
_COMPANY = rf'({_W}(?:\s+{_W}){{0,3}}?){_STOP}'

# 1–4 word role name (stops before _STOP boundary)
_ROLE = rf'({_W}(?:\s+{_W}){{0,3}}?){_STOP}'


# ---------------------------------------------------------------------------
# Company extraction patterns
# ---------------------------------------------------------------------------

COMPANY_PATTERNS = [
    # "Company Name is hiring"
    re.compile(
        r'([A-Z][A-Za-z0-9&.\-\s]{1,40}?)\s+is\s+hiring',
        re.IGNORECASE
    ),
    # "hiring at Company" — stop before for/next/before etc.
    re.compile(
        rf'hiring\s+at\s+{_COMPANY}',
        re.IGNORECASE
    ),
    # "Company hiring" (no 'is') — line start only to avoid false matches
    re.compile(
        r'^([A-Z][A-Za-z0-9&.\-]{1,40})\s+hiring',
        re.IGNORECASE | re.MULTILINE
    ),
    # "opportunity at/with Company"
    re.compile(
        rf'opportunity\s+(?:at|with)\s+{_COMPANY}',
        re.IGNORECASE
    ),
    # "placed at / offer from Company"
    re.compile(
        rf'(?:placed\s+at|offer\s+from)\s+{_COMPANY}',
        re.IGNORECASE
    ),
    # "Company: " or "Company — " at line start (header style)
    re.compile(
        r'^([A-Z][A-Za-z0-9&.\-\s]{1,40}?)\s*[:\-–—]\s',
        re.IGNORECASE | re.MULTILINE
    ),
    # "drive by/at/for Company"
    re.compile(
        rf'drive\s+(?:by|at|for)\s+{_COMPANY}',
        re.IGNORECASE
    ),
    # "recruitment drive at/by/for Company"
    re.compile(
        rf'(?:campus\s+)?recruit\w*\s+(?:drive\s+)?(?:at|by|for)\s+{_COMPANY}',
        re.IGNORECASE
    ),
    # "Company off-campus / on-campus"
    re.compile(
        r'^([A-Z][A-Za-z0-9&.\-\s]{1,40})\s+(?:off|on)[- ]campus',
        re.IGNORECASE | re.MULTILINE
    ),
    # "apply at/to/for Company"
    re.compile(
        rf'appl\w+\s+(?:to|at|for)\s+{_COMPANY}',
        re.IGNORECASE
    ),
]


# ---------------------------------------------------------------------------
# Role extraction patterns
# ---------------------------------------------------------------------------

ROLE_PATTERNS = [
    # "role: Software Engineer" — stop before at/for/|
    re.compile(
        rf'(?:role|position|designation|profile)\s*[:\-]\s*{_ROLE}',
        re.IGNORECASE
    ),
    # "for the role of Backend Developer"
    re.compile(
        rf'(?:for\s+the\s+)?role\s+of\s+{_ROLE}',
        re.IGNORECASE
    ),
    # "as a/an Product Manager"
    re.compile(
        rf'as\s+an?\s+{_ROLE}',
        re.IGNORECASE
    ),
    # FIX 2: "Company: Role | ..." — capture role after colon, stop at pipe or Apply
    # e.g. "Amazon: Software Engineer | Apply by March 31"
    re.compile(
        r'(?:[A-Z][A-Za-z0-9&.\-\s]{1,40}?)\s*:\s*([A-Z][A-Za-z0-9\s]{2,40}?)(?=\s*\||\s+(?:apply|deadline|by|before)|$)',
        re.IGNORECASE
    ),
    # "SDE intern" / "software intern"
    re.compile(
        rf'({_W}(?:\s+{_W}){{0,2}}?)\s+intern(?:ship)?',
        re.IGNORECASE
    ),
    # "job: Software Engineer"
    re.compile(
        rf'job\s*[:\-]\s*{_ROLE}',
        re.IGNORECASE
    ),
    # FIX 3 (tightened): "hiring for/hiring <Role>" — stop before freshers/graduates/candidates/year digits
    re.compile(
        rf'hiring\s+(?:for\s+)?({_W}(?:\s+{_W}){{0,3}}?)(?=\s+(?:at|for|by|from|freshers?|graduates?|candidates?|role|position|interns?|\d{{4}})|\s*$|[,\.\|])',
        re.IGNORECASE
    ),
    # "Software Engineer role/position/opening"
    re.compile(
        rf'({_W}(?:\s+{_W}){{0,3}}?)\s+(?:role|position|opening|vacancy|profile)',
        re.IGNORECASE
    ),
]


# ---------------------------------------------------------------------------
# Known noise words — matches consisting only of these are discarded
# ---------------------------------------------------------------------------

_NOISE_WORDS = {
    "a", "an", "the", "and", "or", "of", "in", "at", "for", "to",
    "is", "are", "has", "have", "be", "been", "hiring", "apply",
    "please", "kindly", "note", "dear", "all", "students", "student",
    "us", "we", "our", "your", "their", "its", "this", "that",
    "new", "latest", "recent", "good", "great", "other",
    "freshers", "fresher", "graduates", "graduate", "candidates",
    "candidate", "interns", "batch", "season", "drive",
    "next", "week", "month", "year", "soon", "today", "now",
    "applying", "register", "check", "visit", "see", "below",
}

# FIX 1: trailing noise words to strip from end of match
_TRAILING_NOISE = re.compile(
    r'\s+(?:and|or|the|for|at|by|from|in|on|of|is|are|a|an)+\s*$',
    re.IGNORECASE
)


def _clean_match(raw: str) -> Optional[str]:
    """
    Strip leading/trailing whitespace and punctuation.
    FIX 1: also strip trailing noise words (e.g. 'Google and' → 'Google').
    Returns None if result is noise-only or too short.
    """
    if not raw:
        return None

    cleaned = raw.strip()
    cleaned = re.sub(r'[,.\-–—:;|\s]+$', '', cleaned).strip()
    cleaned = re.sub(r'^[,.\-–—:;|\s]+', '', cleaned).strip()

    # FIX 1: strip trailing noise words
    cleaned = _TRAILING_NOISE.sub('', cleaned).strip()

    if not cleaned or len(cleaned) < 2:
        return None

    words = set(cleaned.lower().split())
    if words.issubset(_NOISE_WORDS):
        return None

    return cleaned


# ---------------------------------------------------------------------------
# Core extraction helpers
# ---------------------------------------------------------------------------

def _extract_company_from_text(text: str) -> Optional[str]:
    """Try all company patterns against text, return first valid match."""
    for pattern in COMPANY_PATTERNS:
        match = pattern.search(text)
        if match:
            result = _clean_match(match.group(1))
            if result:
                logger.debug(f"Company extracted: '{result}' via pattern: {pattern.pattern[:50]}")
                return result
    return None


def _extract_role_from_text(text: str) -> Optional[str]:
    """Try all role patterns against text, return first valid match."""
    for pattern in ROLE_PATTERNS:
        match = pattern.search(text)
        if match:
            result = _clean_match(match.group(1))
            if result:
                logger.debug(f"Role extracted: '{result}' via pattern: {pattern.pattern[:50]}")
                return result
    return None


def _extract_from_reply_preview(reply_preview: str) -> tuple[Optional[str], Optional[str]]:
    """
    Extract company and role from reply_to_preview text.
    """
    company = _extract_company_from_text(reply_preview)
    role = _extract_role_from_text(reply_preview)
    return company, role


def _extract_from_window(window_messages: list) -> tuple[Optional[str], Optional[str]]:
    """
    Scan up to 5 prior messages (most recent first).
    Return first company and first role found.
    """
    company: Optional[str] = None
    role: Optional[str] = None

    for msg in window_messages:
        if not msg.text:
            continue
        if company is None:
            company = _extract_company_from_text(msg.text)
        if role is None:
            role = _extract_role_from_text(msg.text)
        if company and role:
            break

    return company, role


# ---------------------------------------------------------------------------
# Main resolver
# ---------------------------------------------------------------------------

def resolve_context(
    current_message,
    window_messages: list,
) -> ContextResolvedFields:
    """
    Resolve company and role for the current message.

    Priority 1 — reply chain:
      If current_message.reply_to_preview is not None, extract from it.
      Confidence 0.95.

    Priority 2 — sliding window:
      Look at the last 5 messages before this one.
      Confidence 0.60.

    Priority 3 — no context:
      Confidence 0.30, both fields None.
    """
    message_id = current_message.message_id

    # -----------------------------------------------------------------------
    # Priority 1 — reply chain
    # -----------------------------------------------------------------------
    if current_message.reply_to_preview:
        company, role = _extract_from_reply_preview(current_message.reply_to_preview)

        if company or role:
            logger.info(
                f"[{message_id}] Stage 3 — context source: reply | "
                f"company={company} role={role} confidence={CONFIDENCE_REPLY}"
            )
            return ContextResolvedFields(
                company=company,
                role=role,
                context_source="reply",
                confidence=CONFIDENCE_REPLY,
            )
        else:
            logger.debug(
                f"[{message_id}] reply_to_preview present but no company/role extracted"
            )

    # -----------------------------------------------------------------------
    # Priority 2 — sliding window
    # -----------------------------------------------------------------------
    if window_messages:
        company, role = _extract_from_window(window_messages)

        if company or role:
            logger.info(
                f"[{message_id}] Stage 3 — context source: window | "
                f"company={company} role={role} confidence={CONFIDENCE_WINDOW}"
            )
            return ContextResolvedFields(
                company=company,
                role=role,
                context_source="window",
                confidence=CONFIDENCE_WINDOW,
            )
        else:
            logger.debug(
                f"[{message_id}] sliding window produced no company/role"
            )

    # -----------------------------------------------------------------------
    # Priority 3 — no context
    # -----------------------------------------------------------------------
    logger.info(
        f"[{message_id}] Stage 3 — context source: none | "
        f"confidence={CONFIDENCE_NONE}"
    )
    return ContextResolvedFields(
        company=None,
        role=None,
        context_source="none",
        confidence=CONFIDENCE_NONE,
    )