# # """
# # Stage 5 — Normalization Layer
# # Merges outputs from all previous stages into a single NormalizedRecord.
# # Applies alias normalization, canonical formatting, and final confidence scoring.
# # """

# # import re
# # import json
# # from dataclasses import dataclass
# # from datetime import datetime, timezone
# # from pathlib import Path
# # from typing import Optional
# # from urllib.parse import urlparse, urlencode, parse_qs, urlunparse

# # from config.settings import settings
# # from extraction.preprocessor import PreprocessedMessage
# # from extraction.regex_extractor import RegexExtractedFields
# # from extraction.context_resolver import ContextResolvedFields
# # from extraction.llm_extractor import LLMExtractedFields
# # from utils.logger import get_logger

# # logger = get_logger(__name__)

# # # ---------------------------------------------------------------------------
# # # Load normalizations.json once at import time
# # # ---------------------------------------------------------------------------

# # _NORMALIZATIONS_PATH = Path(__file__).parent.parent / "config" / "normalizations.json"

# # def _load_normalizations() -> dict:
# #     try:
# #         with open(_NORMALIZATIONS_PATH, "r", encoding="utf-8") as f:
# #             data = json.load(f)
# #         logger.info(
# #             f"Normalizations loaded: "
# #             f"{len(data.get('company_aliases', {}))} company aliases, "
# #             f"{len(data.get('role_aliases', {}))} role aliases"
# #         )
# #         return data
# #     except Exception as e:
# #         logger.error(f"Failed to load normalizations.json: {e}")
# #         return {"company_aliases": {}, "role_aliases": {}}

# # _NORMALIZATIONS = _load_normalizations()


# # # ---------------------------------------------------------------------------
# # # NormalizedRecord dataclass
# # # ---------------------------------------------------------------------------

# # @dataclass
# # class NormalizedRecord:
# #     message_id: str
# #     company: Optional[str]
# #     role: Optional[str]
# #     deadline: Optional[datetime]          # timezone-aware UTC
# #     deadline_raw: Optional[str]
# #     package: Optional[str]                # canonical string e.g. "20 LPA"
# #     package_raw: Optional[str]
# #     jd_link: Optional[str]                # cleaned https:// URL
# #     notes: list[str]                      # append-only notes from pipeline
# #     confidence: float                     # final combined score 0.0–1.0
# #     company_source: Optional[str]         # "reply" | "window" | "llm" | "cache" | None
# #     role_source: Optional[str]            # "reply" | "window" | "llm" | "cache" | None
# #     is_processable: bool


# # # ---------------------------------------------------------------------------
# # # Company normalization
# # # ---------------------------------------------------------------------------

# # def _normalize_company(raw: Optional[str]) -> Optional[str]:
# #     """
# #     Apply alias normalization to a company name.
# #     Lookup is case-insensitive. Returns canonical form or title-cased original.
# #     """
# #     if not raw:
# #         return None

# #     cleaned = raw.strip()
# #     if not cleaned:
# #         return None

# #     aliases: dict = _NORMALIZATIONS.get("company_aliases", {})

# #     # Build a lowercase → canonical lookup map once per call (fast enough at this scale)
# #     lower_lookup = {k.lower(): v for k, v in aliases.items()}

# #     normalized = lower_lookup.get(cleaned.lower())
# #     if normalized:
# #         logger.debug(f"Company alias applied: '{cleaned}' → '{normalized}'")
# #         return normalized

# #     # No alias found — return title-cased version of original
# #     return cleaned.title()


# # # ---------------------------------------------------------------------------
# # # Role normalization
# # # ---------------------------------------------------------------------------

# # def _normalize_role(raw: Optional[str]) -> Optional[str]:
# #     """
# #     Apply alias normalization to a role name.
# #     Lookup is case-insensitive. Returns canonical form or title-cased original.
# #     """
# #     if not raw:
# #         return None

# #     cleaned = raw.strip()
# #     if not cleaned:
# #         return None

# #     aliases: dict = _NORMALIZATIONS.get("role_aliases", {})
# #     lower_lookup = {k.lower(): v for k, v in aliases.items()}

# #     normalized = lower_lookup.get(cleaned.lower())
# #     if normalized:
# #         logger.debug(f"Role alias applied: '{cleaned}' → '{normalized}'")
# #         return normalized

# #     return cleaned.title()


# # # ---------------------------------------------------------------------------
# # # Deadline normalization
# # # ---------------------------------------------------------------------------

# # def _normalize_deadline(deadline_normalized: Optional[datetime]) -> Optional[datetime]:
# #     """
# #     Ensure deadline is timezone-aware UTC datetime.
# #     If already timezone-aware, convert to UTC.
# #     If naive, assume UTC.
# #     Returns None if input is None.
# #     """
# #     if deadline_normalized is None:
# #         return None

# #     if deadline_normalized.tzinfo is None:
# #         # Naive datetime — assume UTC
# #         return deadline_normalized.replace(tzinfo=timezone.utc)

# #     # Already aware — convert to UTC
# #     return deadline_normalized.astimezone(timezone.utc)


# # # ---------------------------------------------------------------------------
# # # Package normalization
# # # ---------------------------------------------------------------------------

# # def _normalize_package(package_normalized: Optional[str]) -> Optional[str]:
# #     """
# #     Produce a canonical package string.
# #     Input is already normalized by regex extractor (e.g. '20 LPA', '50k/month').
# #     This stage ensures consistent casing and spacing only.
# #     Returns None if input is None or empty.
# #     """
# #     if not package_normalized:
# #         return None

# #     p = package_normalized.strip()
# #     if not p:
# #         return None

# #     # Normalize LPA casing
# #     p = re.sub(r'\blpa\b', 'LPA', p, flags=re.IGNORECASE)
# #     p = re.sub(r'\bctc\b', 'CTC', p, flags=re.IGNORECASE)

# #     return p


# # # ---------------------------------------------------------------------------
# # # JD link normalization
# # # ---------------------------------------------------------------------------

# # _UTM_PARAMS = {
# #     "utm_source", "utm_medium", "utm_campaign",
# #     "utm_term", "utm_content", "fbclid", "gclid",
# #     "ref", "referral", "source"
# # }

# # def _normalize_jd_link(url: Optional[str]) -> Optional[str]:
# #     """
# #     Normalize a JD/apply URL:
# #     - Strip UTM and tracking params
# #     - Normalize scheme to https
# #     - Remove trailing slashes from path
# #     - Return None if URL is invalid
# #     """
# #     if not url:
# #         return None

# #     url = url.strip()
# #     if not url:
# #         return None

# #     # Add scheme if missing
# #     if url.startswith("www."):
# #         url = "https://" + url
# #     elif not url.startswith(("http://", "https://")):
# #         url = "https://" + url

# #     try:
# #         parsed = urlparse(url)

# #         # Force https
# #         scheme = "https"

# #         # Strip UTM params
# #         query_params = parse_qs(parsed.query, keep_blank_values=False)
# #         cleaned_params = {
# #             k: v for k, v in query_params.items()
# #             if k.lower() not in _UTM_PARAMS
# #         }
# #         new_query = urlencode(cleaned_params, doseq=True)

# #         # Remove trailing slash from path
# #         path = parsed.path.rstrip("/") if parsed.path != "/" else parsed.path

# #         normalized = urlunparse((
# #             scheme,
# #             parsed.netloc.lower(),
# #             path,
# #             parsed.params,
# #             new_query,
# #             ""  # strip fragment
# #         ))

# #         return normalized

# #     except Exception as e:
# #         logger.warning(f"Failed to normalize URL '{url}': {e}")
# #         return None


# # # ---------------------------------------------------------------------------
# # # Confidence scoring
# # # ---------------------------------------------------------------------------

# # def _compute_final_confidence(
# #     regex_fields: RegexExtractedFields,
# #     context_fields: ContextResolvedFields,
# #     llm_fields: LLMExtractedFields,
# #     company: Optional[str],
# #     role: Optional[str],
# # ) -> float:
# #     """
# #     Compute final combined confidence score.

# #     Components:
# #     - Company resolved:      +0.30
# #     - Role resolved:         +0.25
# #     - Deadline normalized:   +0.20
# #     - Package normalized:    +0.15
# #     - JD link present:       +0.10

# #     Source penalty:
# #     - context_source = "none" AND llm source = "skipped": -0.10
# #     - llm confidence < 0.7 (when used):                  -0.05

# #     Clamped to [0.0, 1.0]
# #     """
# #     score = 0.0

# #     if company:
# #         score += 0.30
# #     if role:
# #         score += 0.25
# #     if regex_fields.deadline_normalized:
# #         score += 0.20
# #     if regex_fields.package_normalized:
# #         score += 0.15
# #     if regex_fields.jd_link:
# #         score += 0.10

# #     # Penalize low-signal messages
# #     if context_fields.context_source == "none" and llm_fields.source == "skipped":
# #         score -= 0.10

# #     # Penalize low-confidence LLM results
# #     if llm_fields.source in ("llm", "cache") and llm_fields.confidence is not None:
# #         if llm_fields.confidence < 0.7:
# #             score -= 0.05

# #     return round(max(0.0, min(1.0, score)), 4)


# # # ---------------------------------------------------------------------------
# # # Company and role source selection
# # # ---------------------------------------------------------------------------

# # def _select_company_and_role(
# #     context_fields: ContextResolvedFields,
# #     llm_fields: LLMExtractedFields,
# # ) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
# #     """
# #     Select best company and role using priority chain:
# #     1. Stage 3 reply     (confidence 0.95)
# #     2. Stage 4 LLM/cache (variable confidence, only if >= 0.5)
# #     3. Stage 3 window    (confidence 0.60)
# #     4. None

# #     Returns: (company, role, company_source, role_source)
# #     """
# #     company: Optional[str] = None
# #     role: Optional[str] = None
# #     company_source: Optional[str] = None
# #     role_source: Optional[str] = None

# #     # --- Company selection ---
# #     if context_fields.context_source == "reply" and context_fields.company:
# #         company = context_fields.company
# #         company_source = "reply"
# #     elif llm_fields.source in ("llm", "cache") and llm_fields.company and \
# #          llm_fields.confidence is not None and llm_fields.confidence >= 0.5:
# #         company = llm_fields.company
# #         company_source = llm_fields.source
# #     elif context_fields.context_source == "window" and context_fields.company:
# #         company = context_fields.company
# #         company_source = "window"

# #     # --- Role selection ---
# #     if context_fields.context_source == "reply" and context_fields.role:
# #         role = context_fields.role
# #         role_source = "reply"
# #     elif llm_fields.source in ("llm", "cache") and llm_fields.role and \
# #          llm_fields.confidence is not None and llm_fields.confidence >= 0.5:
# #         role = llm_fields.role
# #         role_source = llm_fields.source
# #     elif context_fields.context_source == "window" and context_fields.role:
# #         role = context_fields.role
# #         role_source = "window"

# #     return company, role, company_source, role_source


# # # ---------------------------------------------------------------------------
# # # Main entry point
# # # ---------------------------------------------------------------------------

# # def normalize(
# #     preprocessed: PreprocessedMessage,
# #     regex_fields: RegexExtractedFields,
# #     context_fields: ContextResolvedFields,
# #     llm_fields: LLMExtractedFields,
# # ) -> NormalizedRecord:
# #     """
# #     Stage 5 entry point.
# #     Merges all stage outputs into a single NormalizedRecord.
# #     """
# #     logger.info(f"[Stage 5] Normalizing message_id={preprocessed.message_id}")

# #     # 1. Select best company and role
# #     raw_company, raw_role, company_source, role_source = _select_company_and_role(
# #         context_fields, llm_fields
# #     )

# #     # 2. Apply alias normalization
# #     company = _normalize_company(raw_company)
# #     role = _normalize_role(raw_role)

# #     # 3. Normalize deadline
# #     deadline = _normalize_deadline(regex_fields.deadline_normalized)

# #     # 4. Normalize package
# #     package = _normalize_package(regex_fields.package_normalized)

# #     # 5. Normalize JD link
# #     jd_link = _normalize_jd_link(regex_fields.jd_link)

# #     # 6. Compute final confidence
# #     confidence = _compute_final_confidence(
# #         regex_fields, context_fields, llm_fields, company, role
# #     )

# #     # 7. Build notes list — capture anything worth preserving
# #     notes: list[str] = []
# #     if regex_fields.deadline_raw and regex_fields.deadline_raw != regex_fields.deadline_normalized:
# #         notes.append(f"deadline_raw: {regex_fields.deadline_raw}")
# #     if regex_fields.package_raw and regex_fields.package_raw != regex_fields.package_normalized:
# #         notes.append(f"package_raw: {regex_fields.package_raw}")
# #     if context_fields.context_source == "none" and llm_fields.source == "skipped":
# #         notes.append("low_signal: no company or role resolved")

# #     logger.info(
# #         f"[Stage 5] Normalized — company={company} (source={company_source}), "
# #         f"role={role} (source={role_source}), "
# #         f"deadline={deadline}, package={package}, "
# #         f"jd_link={jd_link}, confidence={confidence}"
# #     )

# #     return NormalizedRecord(
# #         message_id=preprocessed.message_id,
# #         company=company,
# #         role=role,
# #         deadline=deadline,
# #         deadline_raw=regex_fields.deadline_raw,
# #         package=package,
# #         package_raw=regex_fields.package_raw,
# #         jd_link=jd_link,
# #         notes=notes,
# #         confidence=confidence,
# #         company_source=company_source,
# #         role_source=role_source,
# #         is_processable=preprocessed.is_processable,
# #     )

# """
# Stage 5 — Normalization Layer
# Merges outputs from all previous stages into a single NormalizedRecord.
# Applies alias normalization, canonical formatting, and final confidence scoring.
# """

# import re
# import json
# from dataclasses import dataclass
# from datetime import datetime, timezone
# from pathlib import Path
# from typing import Optional
# from urllib.parse import urlparse, urlencode, parse_qs, urlunparse

# from config.settings import settings
# from extraction.preprocessor import PreprocessedMessage
# from extraction.regex_extractor import RegexExtractedFields
# from extraction.context_resolver import ContextResolvedFields
# from extraction.llm_extractor import LLMExtractedFields
# from utils.logger import get_logger

# logger = get_logger(__name__)

# # ---------------------------------------------------------------------------
# # Load normalizations.json once at import time
# # ---------------------------------------------------------------------------

# _NORMALIZATIONS_PATH = Path(__file__).parent.parent / "config" / "normalizations.json"

# def _load_normalizations() -> dict:
#     try:
#         with open(_NORMALIZATIONS_PATH, "r", encoding="utf-8") as f:
#             data = json.load(f)
#         logger.info(
#             f"Normalizations loaded: "
#             f"{len(data.get('company_aliases', {}))} company aliases, "
#             f"{len(data.get('role_aliases', {}))} role aliases"
#         )
#         return data
#     except Exception as e:
#         logger.error(f"Failed to load normalizations.json: {e}")
#         return {"company_aliases": {}, "role_aliases": {}}

# _NORMALIZATIONS = _load_normalizations()


# # ---------------------------------------------------------------------------
# # NormalizedRecord dataclass
# # ---------------------------------------------------------------------------

# @dataclass
# class NormalizedRecord:
#     message_id: str
#     company: Optional[str]
#     role: Optional[str]
#     deadline: Optional[datetime]          # timezone-aware UTC
#     deadline_raw: Optional[str]
#     package: Optional[str]                # canonical string e.g. "20 LPA"
#     package_raw: Optional[str]
#     jd_link: Optional[str]                # cleaned https:// URL
#     notes: list[str]                      # append-only notes from pipeline
#     confidence: float                     # final combined score 0.0–1.0
#     company_source: Optional[str]         # "self" | "reply" | "window" | "llm" | "cache" | None
#     role_source: Optional[str]            # "self" | "reply" | "window" | "llm" | "cache" | None
#     is_processable: bool


# # ---------------------------------------------------------------------------
# # Company normalization
# # ---------------------------------------------------------------------------

# def _normalize_company(raw: Optional[str]) -> Optional[str]:
#     """
#     Apply alias normalization to a company name.
#     Lookup is case-insensitive. Returns canonical form or title-cased original.
#     """
#     if not raw:
#         return None

#     cleaned = raw.strip()
#     if not cleaned:
#         return None

#     aliases: dict = _NORMALIZATIONS.get("company_aliases", {})

#     # Build a lowercase → canonical lookup map once per call (fast enough at this scale)
#     lower_lookup = {k.lower(): v for k, v in aliases.items()}

#     normalized = lower_lookup.get(cleaned.lower())
#     if normalized:
#         logger.debug(f"Company alias applied: '{cleaned}' → '{normalized}'")
#         return normalized

#     # No alias found — return title-cased version of original
#     return cleaned.title()


# # ---------------------------------------------------------------------------
# # Role normalization
# # ---------------------------------------------------------------------------

# def _normalize_role(raw: Optional[str]) -> Optional[str]:
#     """
#     Apply alias normalization to a role name.
#     Lookup is case-insensitive. Returns canonical form or title-cased original.
#     """
#     if not raw:
#         return None

#     cleaned = raw.strip()
#     if not cleaned:
#         return None

#     aliases: dict = _NORMALIZATIONS.get("role_aliases", {})
#     lower_lookup = {k.lower(): v for k, v in aliases.items()}

#     normalized = lower_lookup.get(cleaned.lower())
#     if normalized:
#         logger.debug(f"Role alias applied: '{cleaned}' → '{normalized}'")
#         return normalized

#     return cleaned.title()


# # ---------------------------------------------------------------------------
# # Deadline normalization
# # ---------------------------------------------------------------------------

# def _normalize_deadline(deadline_normalized: Optional[datetime]) -> Optional[datetime]:
#     """
#     Ensure deadline is timezone-aware UTC datetime.
#     If already timezone-aware, convert to UTC.
#     If naive, assume UTC.
#     Returns None if input is None.
#     """
#     if deadline_normalized is None:
#         return None

#     if deadline_normalized.tzinfo is None:
#         # Naive datetime — assume UTC
#         return deadline_normalized.replace(tzinfo=timezone.utc)

#     # Already aware — convert to UTC
#     return deadline_normalized.astimezone(timezone.utc)


# # ---------------------------------------------------------------------------
# # Package normalization
# # ---------------------------------------------------------------------------

# def _normalize_package(package_normalized: Optional[str]) -> Optional[str]:
#     """
#     Produce a canonical package string.
#     Input is already normalized by regex extractor (e.g. '20 LPA', '50k/month').
#     This stage ensures consistent casing and spacing only.
#     Returns None if input is None or empty.
#     """
#     if not package_normalized:
#         return None

#     p = package_normalized.strip()
#     if not p:
#         return None

#     # Normalize LPA casing
#     p = re.sub(r'\blpa\b', 'LPA', p, flags=re.IGNORECASE)
#     p = re.sub(r'\bctc\b', 'CTC', p, flags=re.IGNORECASE)

#     return p


# # ---------------------------------------------------------------------------
# # JD link normalization
# # ---------------------------------------------------------------------------

# _UTM_PARAMS = {
#     "utm_source", "utm_medium", "utm_campaign",
#     "utm_term", "utm_content", "fbclid", "gclid",
#     "ref", "referral", "source"
# }

# def _normalize_jd_link(url: Optional[str]) -> Optional[str]:
#     """
#     Normalize a JD/apply URL:
#     - Strip UTM and tracking params
#     - Normalize scheme to https
#     - Remove trailing slashes from path
#     - Return None if URL is invalid
#     """
#     if not url:
#         return None

#     url = url.strip()
#     if not url:
#         return None

#     # Add scheme if missing
#     if url.startswith("www."):
#         url = "https://" + url
#     elif not url.startswith(("http://", "https://")):
#         url = "https://" + url

#     try:
#         parsed = urlparse(url)

#         # Force https
#         scheme = "https"

#         # Strip UTM params
#         query_params = parse_qs(parsed.query, keep_blank_values=False)
#         cleaned_params = {
#             k: v for k, v in query_params.items()
#             if k.lower() not in _UTM_PARAMS
#         }
#         new_query = urlencode(cleaned_params, doseq=True)

#         # Remove trailing slash from path
#         path = parsed.path.rstrip("/") if parsed.path != "/" else parsed.path

#         normalized = urlunparse((
#             scheme,
#             parsed.netloc.lower(),
#             path,
#             parsed.params,
#             new_query,
#             ""  # strip fragment
#         ))

#         return normalized

#     except Exception as e:
#         logger.warning(f"Failed to normalize URL '{url}': {e}")
#         return None


# # ---------------------------------------------------------------------------
# # Confidence scoring
# # ---------------------------------------------------------------------------

# def _compute_final_confidence(
#     regex_fields: RegexExtractedFields,
#     context_fields: ContextResolvedFields,
#     llm_fields: LLMExtractedFields,
#     company: Optional[str],
#     role: Optional[str],
# ) -> float:
#     """
#     Compute final combined confidence score.

#     Components:
#     - Company resolved:      +0.30
#     - Role resolved:         +0.25
#     - Deadline normalized:   +0.20
#     - Package normalized:    +0.15
#     - JD link present:       +0.10

#     Source penalty:
#     - context_source = "none" AND llm source = "skipped": -0.10
#     - llm confidence < 0.7 (when used):                  -0.05

#     Clamped to [0.0, 1.0]
#     """
#     score = 0.0

#     if company:
#         score += 0.30
#     if role:
#         score += 0.25
#     if regex_fields.deadline_normalized:
#         score += 0.20
#     if regex_fields.package_normalized:
#         score += 0.15
#     if regex_fields.jd_link:
#         score += 0.10

#     # Penalize low-signal messages
#     if context_fields.context_source == "none" and llm_fields.source == "skipped":
#         score -= 0.10

#     # Penalize low-confidence LLM results
#     if llm_fields.source in ("llm", "cache") and llm_fields.confidence is not None:
#         if llm_fields.confidence < 0.7:
#             score -= 0.05

#     return round(max(0.0, min(1.0, score)), 4)


# # ---------------------------------------------------------------------------
# # Company and role source selection
# # ---------------------------------------------------------------------------

# # Sources ranked highest to lowest priority
# # "self"   = extracted directly from current message text (Priority 0) — confidence 0.85
# # "reply"  = extracted from reply_to_preview (Priority 1)              — confidence 0.95
# # "llm"    = extracted by LLM (Priority 2, fills gaps only)
# # "cache"  = LLM result served from cache
# # "window" = extracted from sliding window of prior messages (Priority 3) — confidence 0.60

# _SELF_SOURCES  = ("self",)
# _REPLY_SOURCES = ("reply",)
# _LLM_SOURCES   = ("llm", "cache")
# _WINDOW_SOURCES = ("window",)


# def _select_company_and_role(
#     context_fields: ContextResolvedFields,
#     llm_fields: LLMExtractedFields,
# ) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
#     """
#     Select best company and role using priority chain:

#     For each field independently:
#       1. Stage 3 self   (context_source="self",   confidence 0.85)
#       2. Stage 3 reply  (context_source="reply",  confidence 0.95)
#       3. Stage 4 LLM    (source="llm"|"cache",    confidence >= 0.5)
#       4. Stage 3 window (context_source="window", confidence 0.60)
#       5. None

#     Note: reply beats self in confidence, but self is checked first as a
#     fallback when reply_to_preview doesn't contain company/role. When
#     context_source="reply", Stage 3 already tried self first internally
#     and merged — so we just trust the returned values.

#     Returns: (company, role, company_source, role_source)
#     """
#     company: Optional[str] = None
#     role: Optional[str] = None
#     company_source: Optional[str] = None
#     role_source: Optional[str] = None

#     ctx_source = context_fields.context_source

#     # ── Company selection ────────────────────────────────────────────────────
#     # if ctx_source in ("self", "reply") and context_fields.company:
#     #     company = context_fields.company
#     #     company_source = ctx_source
#     # elif (
#     #     llm_fields.source in _LLM_SOURCES
#     #     and llm_fields.company
#     #     and llm_fields.confidence is not None
#     #     and llm_fields.confidence >= 0.5
#     # ):
#     #     company = llm_fields.company
#     #     company_source = llm_fields.source
#     # elif ctx_source == "window" and context_fields.company:
#     #     company = context_fields.company
#     #     company_source = "window"
#     # self extraction is most trusted

#     # Company: prefer LLM, fallback to context
#     if (
#         llm_fields.source in _LLM_SOURCES
#         and llm_fields.company
#         and llm_fields.confidence is not None
#         and llm_fields.confidence >= 0.5
#     ):
#         company = llm_fields.company
#         company_source = llm_fields.source
#     elif context_fields.company:
#         company = context_fields.company
#         company_source = context_fields.context_source

#     # ── Role selection ───────────────────────────────────────────────────────
#     # if ctx_source in ("self", "reply") and context_fields.role:
#     #     role = context_fields.role
#     #     role_source = ctx_source
#     # elif (
#     #     llm_fields.source in _LLM_SOURCES
#     #     and llm_fields.role
#     #     and llm_fields.confidence is not None
#     #     and llm_fields.confidence >= 0.5
#     # ):
#     #     role = llm_fields.role
#     #     role_source = llm_fields.source
#     # elif ctx_source == "window" and context_fields.role:
#     #     role = context_fields.role
#     #     role_source = "window"

#     # Role: prefer LLM, fallback to context
#     if (
#         llm_fields.source in _LLM_SOURCES
#         and llm_fields.role
#         and llm_fields.confidence is not None
#         and llm_fields.confidence >= 0.5
#     ):
#         role = llm_fields.role
#         role_source = llm_fields.source
#     elif context_fields.role:
#         role = context_fields.role
#         role_source = context_fields.context_source

#     return company, role, company_source, role_source


# # ---------------------------------------------------------------------------
# # Main entry point
# # ---------------------------------------------------------------------------

# def normalize(
#     preprocessed: PreprocessedMessage,
#     regex_fields: RegexExtractedFields,
#     context_fields: ContextResolvedFields,
#     llm_fields: LLMExtractedFields,
# ) -> NormalizedRecord:
#     """
#     Stage 5 entry point.
#     Merges all stage outputs into a single NormalizedRecord.
#     """
#     logger.info(f"[Stage 5] Normalizing message_id={preprocessed.message_id}")

#     # 1. Select best company and role
#     raw_company, raw_role, company_source, role_source = _select_company_and_role(
#         context_fields, llm_fields
#     )

#     # 2. Apply alias normalization
#     company = _normalize_company(raw_company)
#     role = _normalize_role(raw_role)

#     # 3. Normalize deadline
#     deadline = _normalize_deadline(regex_fields.deadline_normalized)

#     # 4. Normalize package
#     package = _normalize_package(regex_fields.package_normalized)

#     # 5. Normalize JD link
#     jd_link = _normalize_jd_link(regex_fields.jd_link)

#     # 6. Compute final confidence
#     confidence = _compute_final_confidence(
#         regex_fields, context_fields, llm_fields, company, role
#     )

#     # 7. Build notes list — capture anything worth preserving
#     notes: list[str] = []
#     if regex_fields.deadline_raw and regex_fields.deadline_raw != regex_fields.deadline_normalized:
#         notes.append(f"deadline_raw: {regex_fields.deadline_raw}")
#     if regex_fields.package_raw and regex_fields.package_raw != regex_fields.package_normalized:
#         notes.append(f"package_raw: {regex_fields.package_raw}")
#     if context_fields.context_source == "none" and llm_fields.source == "skipped":
#         notes.append("low_signal: no company or role resolved")

#     logger.info(
#         f"[Stage 5] Normalized — company={company} (source={company_source}), "
#         f"role={role} (source={role_source}), "
#         f"deadline={deadline}, package={package}, "
#         f"jd_link={jd_link}, confidence={confidence}"
#     )

#     return NormalizedRecord(
#         message_id=preprocessed.message_id,
#         company=company,
#         role=role,
#         deadline=deadline,
#         deadline_raw=regex_fields.deadline_raw,
#         package=package,
#         package_raw=regex_fields.package_raw,
#         jd_link=jd_link,
#         notes=notes,
#         confidence=confidence,
#         company_source=company_source,
#         role_source=role_source,
#         is_processable=preprocessed.is_processable,
#     )


"""
Stage 5 — Normalization
Thin pass-through. LLM fields take priority.
Only deadline needs format enforcement — LLM provides DD Mon YYYY,
regex result used as fallback.
No alias lookup. No company/role normalization.
"""

from dataclasses import dataclass, field
from datetime import datetime,timezone
from typing import Optional

from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class NormalizedRecord:
    # Core identity
    message_id: Optional[str] = None
    company: Optional[str] = None
    role: Optional[str] = None              # first role, for backward compat
    roles: list = field(default_factory=list)

    # Opportunity details
    # deadline: Optional[str] = None  
    deadline: Optional[datetime] = None
    deadline_raw: Optional[str] = None        # DD Mon YYYY or original string
    package: Optional[str] = None
    jd_link: Optional[str] = None           # primary link, backward compat
    jd_links: list = field(default_factory=list)
    internal_form_link: Optional[str] = None
    duration: Optional[str] = None
    start_date: Optional[str] = None
    location: Optional[str] = None

    # Eligibility
    eligible: Optional[str] = None
    eligible_reason: Optional[str] = None
    eligibility_criteria: Optional[str] = None

    # Metadata
    confidence: float = 0.0
    source: str = "unknown"                 # "llm", "regex", "context", "hybrid"


def normalize(
    preprocessed,
    regex_fields,
    context_fields,
    llm_fields,
) -> NormalizedRecord:
    """
    Merge all stage outputs into a single NormalizedRecord.
    LLM wins on all fields except deadline fallback from regex.
    """

    record = NormalizedRecord()

    record.message_id = preprocessed.message_id

    # --- Company ---
    # LLM is authoritative. Context as fallback.
    record.company = (
        llm_fields.company
        or (context_fields.company if context_fields else None)
    )

    # --- Roles ---
    record.roles = llm_fields.roles or []
    record.role = record.roles[0] if record.roles else None

    # --- Deadline ---
    # LLM provides DD Mon YYYY format. Regex as fallback only.
    # record.deadline = llm_fields.deadline or regex_fields.deadline

    record.deadline_raw = (
    llm_fields.deadline
    or regex_fields.deadline_raw
)
    record.deadline = None

    if getattr(llm_fields, "deadline_iso", None):
        try:
            record.deadline = datetime.fromisoformat(
                llm_fields.deadline_iso.replace("Z", "+00:00")
            )
        except Exception:
            logger.warning(
                f"Failed to parse llm deadline_iso={llm_fields.deadline_iso}"
            )

    if record.deadline is None:
        record.deadline = regex_fields.deadline_normalized

        

    # --- Package ---
    # LLM preferred, regex as fallback.
    record.package = llm_fields.package or regex_fields.package_normalized
    # --- Links ---
    # Merge LLM jd_links with regex-extracted urls. Deduplicate.
    all_links = list(dict.fromkeys(
        (llm_fields.jd_links or []) + (preprocessed.urls or [])
    ))
    record.jd_links = all_links
    record.jd_link = all_links[0] if all_links else None
    record.internal_form_link = llm_fields.internal_form_link

    # --- Other LLM fields ---
    record.duration = llm_fields.duration
    record.start_date = llm_fields.start_date
    record.location = llm_fields.location
    record.eligible = llm_fields.eligible
    record.eligible_reason = llm_fields.eligible_reason
    record.eligibility_criteria = llm_fields.eligibility_criteria

    # --- Confidence ---
    # Take max across all sources.
    scores = [
        llm_fields.confidence,
        context_fields.confidence if context_fields else 0.0,
    ]
    record.confidence = max(scores)

    # --- Source tag ---
    # if llm_fields.company or llm_fields.roles:
    #     record.source = "llm" if not (context_fields and context_fields.company) else "hybrid"
    # elif context_fields and context_fields.company:
    #     record.source = "context"
    # elif regex_fields.deadline or regex_fields.package:
    #     record.source = "regex"
    # else:
    #     record.source = "unknown"

    logger.debug(
        f"Normalized — company={record.company}, roles={record.roles}, "
        f"deadline={record.deadline}, eligible={record.eligible}, "
        f"confidence={record.confidence}"
    )

    return record