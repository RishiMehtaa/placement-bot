"""
Phase 6 — Preprocessing Layer
Stage 1 of the extraction pipeline.

Responsibilities:
- Extract all URLs from raw text before any cleaning
- Remove emoji characters
- Normalize whitespace
- Lowercase text
- Determine is_processable based on URL presence or placement keywords
"""

import re
from dataclasses import dataclass, field
from typing import List

from utils.logger import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Placement keywords — a message is processable if any of these appear
# ---------------------------------------------------------------------------
PLACEMENT_KEYWORDS = [
    "hiring",
    "apply",
    "deadline",
    "intern",
    "role",
    "package",
    "lpa",
    "ctc",
    "opening",
    "drive",
    "register",
    "opportunity",
    "placement",
    "recruit",
]

# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------

# Matches http/https URLs and bare www. URLs
_URL_PATTERN = re.compile(
    r"(?:https?://|www\.)"       # scheme or www
    r"[^\s\]\[<>\"'(){}|\\^`]+"  # non-whitespace, non-bracket chars
)

# Unicode emoji ranges — covers virtually all emoji blocks
_EMOJI_PATTERN = re.compile(
    "["
    "\U0001F600-\U0001F64F"  # emoticons
    "\U0001F300-\U0001F5FF"  # misc symbols and pictographs
    "\U0001F680-\U0001F6FF"  # transport and map symbols
    "\U0001F700-\U0001F77F"  # alchemical symbols
    "\U0001F780-\U0001F7FF"  # geometric shapes extended
    "\U0001F800-\U0001F8FF"  # supplemental arrows-C
    "\U0001F900-\U0001F9FF"  # supplemental symbols and pictographs
    "\U0001FA00-\U0001FA6F"  # chess symbols
    "\U0001FA70-\U0001FAFF"  # symbols and pictographs extended-A
    "\U00002702-\U000027B0"  # dingbats
    "\U000024C2-\U0001F251"  # enclosed characters
    "\U0001F1E0-\U0001F1FF"  # flags (iOS)
    "\U00002500-\U00002BEF"  # chinese char and other
    "\U00002702-\U000027B0"
    "\U00002702-\U000027B0"
    "\U000025FB-\U000025FE"
    "\U00002600-\U000026FF"
    "\U0000200D"             # zero width joiner
    "\U0000FE0F"             # variation selector-16
    "\U0000FE00-\U0000FE0F"  # variation selectors
    "\U0000FE30-\U0000FE4F"
    "\U0001F004"
    "\U0001F0CF"
    "]+",
    flags=re.UNICODE,
)


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class PreprocessedMessage:
    message_id: str
    original_text: str
    cleaned_text: str
    urls: List[str] = field(default_factory=list)
    is_processable: bool = False
    matched_keywords: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------

def extract_urls(text: str) -> List[str]:
    """
    Extract all URLs from raw text before any cleaning.
    Returns a deduplicated list preserving order of first appearance.
    """
    found = _URL_PATTERN.findall(text)
    seen = set()
    deduped = []
    for url in found:
        url = url.rstrip(".,;:!?)")  # strip trailing punctuation artifacts
        if url not in seen:
            seen.add(url)
            deduped.append(url)
    return deduped


def remove_emoji(text: str) -> str:
    """Remove all emoji characters from text."""
    return _EMOJI_PATTERN.sub("", text)


def normalize_whitespace(text: str) -> str:
    """
    Replace tabs and multiple consecutive spaces with a single space.
    Strip leading and trailing whitespace.
    """
    text = text.replace("\t", " ")
    text = re.sub(r" {2,}", " ", text)
    return text.strip()


def find_matched_keywords(text: str) -> List[str]:
    """
    Return all placement keywords found in already-lowercased cleaned text.
    Uses word-boundary matching to avoid false positives (e.g. 'apply' inside 'reapply').
    """
    matched = []
    for keyword in PLACEMENT_KEYWORDS:
        pattern = r"\b" + re.escape(keyword) + r"\b"
        if re.search(pattern, text):
            matched.append(keyword)
    return matched


def preprocess(message_id: str, raw_text: str) -> PreprocessedMessage:
    """
    Full preprocessing pipeline for a single message.

    Steps:
    1. Extract URLs from raw text (before any cleaning)
    2. Remove emoji
    3. Normalize whitespace
    4. Lowercase
    5. Find matched placement keywords
    6. Determine is_processable

    Returns a PreprocessedMessage dataclass.
    """
    logger.debug(f"[{message_id}] Preprocessing started")

    # Step 1 — extract URLs from raw text before cleaning
    urls = extract_urls(raw_text)

    # Step 2 — remove emoji
    cleaned = remove_emoji(raw_text)

    # Step 3 — normalize whitespace
    cleaned = normalize_whitespace(cleaned)

    # Step 4 — lowercase
    cleaned = cleaned.lower()

    # Step 5 — find matched keywords
    matched_keywords = find_matched_keywords(cleaned)

    # Step 6 — determine processability
    is_processable = bool(urls) or bool(matched_keywords)

    result = PreprocessedMessage(
        message_id=message_id,
        original_text=raw_text,
        cleaned_text=cleaned,
        urls=urls,
        is_processable=is_processable,
        matched_keywords=matched_keywords,
    )

    logger.info(
        f"[{message_id}] Preprocessing complete | "
        f"is_processable={is_processable} | "
        f"urls={len(urls)} | "
        f"keywords={matched_keywords}"
    )

    return result