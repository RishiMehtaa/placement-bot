# """
# Stage 4 — LLM Extractor
# Calls Groq (llama-3.1-8b-instant) only when company or role is still None after Stage 3.
# Enforces daily call cap. Caches by SHA-256 of cleaned text. Rejects low-confidence
# or malformed responses and logs them to dead_letter_queue.
# """

# import hashlib
# import json
# import re
# from dataclasses import dataclass, field
# from datetime import datetime, date, timezone
# from typing import Optional

# from groq import Groq

# from config.settings import settings
# from extraction.preprocessor import PreprocessedMessage
# from extraction.context_resolver import ContextResolvedFields
# from utils.logger import get_logger

# logger = get_logger(__name__)

# # ---------------------------------------------------------------------------
# # Constants
# # ---------------------------------------------------------------------------

# SYSTEM_PROMPT = (
#     "You extract placement opportunity information from WhatsApp messages.\n"
#     "These are messages from an official Indian college (SVKM's Dwarkadas. J. Sanghvi College of Engineering) placement group.\n"
#     "Return ONLY valid JSON with exactly these keys: company, role, confidence, reasoning.\n"
#     "confidence must be a float between 0.0 and 1.0 indicating how certain you are.\n"
#     "If a field cannot be determined confidently, return null.\n"
#     "Never guess. Never invent company names."
# )

# _PERSON_NAME_INDICATORS = re.compile(
#     r"\b(sir|ma'am|maam|mr|mrs|ms|dr|prof|professor|he|she|they|contact|"
#     r"please|kindly|regards|team|admin|coordinator|placement officer)\b",
#     re.IGNORECASE,
# )

# _VALID_COMPANY_RE = re.compile(r"[A-Za-z]")

# # In-memory cache: {cache_key: {"company": ..., "role": ..., "confidence": ...,
# #                               "reasoning": ..., "cached_at": datetime}}
# _cache: dict[str, dict] = {}

# # Daily call tracker: {"date": date, "count": int}
# _daily_tracker: dict = {"date": date.today(), "count": 0}


# # ---------------------------------------------------------------------------
# # Dataclass
# # ---------------------------------------------------------------------------

# @dataclass
# class LLMExtractedFields:
#     company: Optional[str]
#     role: Optional[str]
#     confidence: float
#     reasoning: Optional[str]
#     source: str  # "llm" | "cache" | "skipped"


# # ---------------------------------------------------------------------------
# # Internal helpers
# # ---------------------------------------------------------------------------

# def _cache_key(cleaned_text: str) -> str:
#     """SHA-256 of the cleaned message text — used as cache key."""
#     return hashlib.sha256(cleaned_text.encode("utf-8")).hexdigest()


# def _is_cache_hit(key: str) -> bool:
#     """Return True if key exists in cache and has not expired."""
#     if key not in _cache:
#         return False
#     entry = _cache[key]
#     cached_at: datetime = entry["cached_at"]
#     now = datetime.now(timezone.utc)
#     age_hours = (now - cached_at).total_seconds() / 3600
#     if age_hours > settings.LLM_CACHE_TTL_HOURS:
#         del _cache[key]
#         return False
#     return True


# def _get_from_cache(key: str) -> dict:
#     return _cache[key]


# def _store_in_cache(key: str, company: Optional[str], role: Optional[str],
#                     confidence: float, reasoning: Optional[str]) -> None:
#     _cache[key] = {
#         "company": company,
#         "role": role,
#         "confidence": confidence,
#         "reasoning": reasoning,
#         "cached_at": datetime.now(timezone.utc),
#     }


# def _daily_limit_reached() -> bool:
#     """Reset counter on new day; return True if today's limit is exhausted."""
#     today = date.today()
#     if _daily_tracker["date"] != today:
#         _daily_tracker["date"] = today
#         _daily_tracker["count"] = 0
#     return _daily_tracker["count"] >= settings.LLM_DAILY_CALL_LIMIT


# def _increment_daily_count() -> None:
#     today = date.today()
#     if _daily_tracker["date"] != today:
#         _daily_tracker["date"] = today
#         _daily_tracker["count"] = 0
#     _daily_tracker["count"] += 1


# def _is_valid_company(company: Optional[str]) -> bool:
#     """
#     Return False if company looks like a person name, generic noise word,
#     or is too short to be a real company name.
#     """
#     if not company:
#         return False
#     company = company.strip()
#     if len(company) < 2:
#         return False
#     if not _VALID_COMPANY_RE.search(company):
#         return False
#     if _PERSON_NAME_INDICATORS.search(company):
#         return False
#     return True


# def _parse_llm_response(raw: str) -> dict:
#     """
#     Parse raw LLM text into a dict.
#     Strips markdown fences if present. Raises ValueError on bad JSON.
#     """
#     cleaned = raw.strip()
#     cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
#     cleaned = re.sub(r"\s*```$", "", cleaned)
#     cleaned = cleaned.strip()
#     return json.loads(cleaned)


# def _call_groq(user_text: str) -> dict:
#     """
#     Call Groq API and return parsed JSON dict.
#     Raises ValueError on bad JSON or API error.
#     """
#     client = Groq(api_key=settings.LLM_API_KEY)
#     response = client.chat.completions.create(
#         model=settings.LLM_MODEL,
#         max_tokens=settings.LLM_MAX_TOKENS,
#         messages=[
#             {"role": "system", "content": SYSTEM_PROMPT},
#             {"role": "user", "content": user_text},
#         ],
#         temperature=0,
#     )
#     raw_text = response.choices[0].message.content or ""
#     if not raw_text.strip():
#         raise ValueError("LLM returned empty response")
#     return _parse_llm_response(raw_text)


# # ---------------------------------------------------------------------------
# # Dead letter logging
# # ---------------------------------------------------------------------------

# def _log_dead_letter(message_id: str, failure_reason: str, raw_payload: dict) -> None:
#     logger.warning(
#         f"LLM dead letter | message_id={message_id} | reason={failure_reason} "
#         f"| payload={json.dumps(raw_payload)}"
#     )


# # ---------------------------------------------------------------------------
# # Public entry point
# # ---------------------------------------------------------------------------

# def extract_with_llm(
#     preprocessed: PreprocessedMessage,
#     context_fields: ContextResolvedFields,
# ) -> LLMExtractedFields:
#     """
#     Stage 4 entry point.

#     Calls LLM only when company or role is still None after Stage 3.
#     Returns LLMExtractedFields with source="skipped" if LLM is not needed.
#     Returns LLMExtractedFields with source="cache" if a valid cache hit exists.
#     Returns LLMExtractedFields with source="llm" after a real API call.
#     Logs rejections to dead letter and returns source="skipped" on rejection.
#     """
#     message_id = preprocessed.message_id
#     needs_company = context_fields.company is None
#     needs_role = context_fields.role is None

#     # If both company and role are already resolved, skip entirely
#     if not needs_company and not needs_role:
#         logger.info(
#             f"Stage 4 | message_id={message_id} | source=skipped "
#             f"| reason=company_and_role_already_resolved"
#         )
#         return LLMExtractedFields(
#             company=None,
#             role=None,
#             confidence=0.0,
#             reasoning=None,
#             source="skipped",
#         )

#     key = _cache_key(preprocessed.cleaned_text)

#     # Cache hit
#     if _is_cache_hit(key):
#         entry = _get_from_cache(key)
#         logger.info(
#             f"Stage 4 | message_id={message_id} | source=cache "
#             f"| company={entry['company']} | role={entry['role']} "
#             f"| confidence={entry['confidence']}"
#         )
#         return LLMExtractedFields(
#             company=entry["company"] if needs_company else None,
#             role=entry["role"] if needs_role else None,
#             confidence=entry["confidence"],
#             reasoning=entry["reasoning"],
#             source="cache",
#         )

#     # Daily limit check
#     if _daily_limit_reached():
#         logger.warning(
#             f"Stage 4 | message_id={message_id} | source=skipped "
#             f"| reason=daily_limit_reached | limit={settings.LLM_DAILY_CALL_LIMIT}"
#         )
#         return LLMExtractedFields(
#             company=None,
#             role=None,
#             confidence=0.0,
#             reasoning=None,
#             source="skipped",
#         )

#     # Build user prompt
#     user_text = (
#         f"Extract the company name and job role from this placement message.\n\n"
#         f"Message: {preprocessed.cleaned_text}"
#     )
#     if context_fields.company:
#         user_text += f"\n\nHint — company already known from context: {context_fields.company}"
#     if context_fields.role:
#         user_text += f"\n\nHint — role already known from context: {context_fields.role}"

#     # Call Groq
#     try:
#         _increment_daily_count()
#         parsed = _call_groq(user_text)
#     except Exception as exc:
#         failure_reason = f"llm_call_failed: {exc}"
#         logger.error(f"Stage 4 | message_id={message_id} | {failure_reason}")
#         _log_dead_letter(message_id, failure_reason, {"cleaned_text": preprocessed.cleaned_text})
#         return LLMExtractedFields(
#             company=None,
#             role=None,
#             confidence=0.0,
#             reasoning=None,
#             source="skipped",
#         )

#     # Validate response structure
#     if not isinstance(parsed, dict):
#         failure_reason = "response_not_a_dict"
#         _log_dead_letter(message_id, failure_reason, {"raw": str(parsed)})
#         logger.warning(f"Stage 4 | message_id={message_id} | rejected | reason={failure_reason}")
#         return LLMExtractedFields(
#             company=None, role=None, confidence=0.0, reasoning=None, source="skipped"
#         )

#     company: Optional[str] = parsed.get("company")
#     role: Optional[str] = parsed.get("role")
#     confidence: float = float(parsed.get("confidence", 0.0))
#     reasoning: Optional[str] = parsed.get("reasoning")

#     # Reject low confidence
#     if confidence < 0.5:
#         failure_reason = f"confidence_too_low: {confidence}"
#         _log_dead_letter(message_id, failure_reason, parsed)
#         logger.warning(f"Stage 4 | message_id={message_id} | rejected | reason={failure_reason}")
#         return LLMExtractedFields(
#             company=None, role=None, confidence=0.0, reasoning=reasoning, source="skipped"
#         )

#     # Validate company name
#     if company is not None and not _is_valid_company(company):
#         failure_reason = f"invalid_company_name: {company}"
#         _log_dead_letter(message_id, failure_reason, parsed)
#         logger.warning(f"Stage 4 | message_id={message_id} | rejected | reason={failure_reason}")
#         company = None

#     # Store valid result in cache
#     _store_in_cache(key, company, role, confidence, reasoning)

#     logger.info(
#         f"Stage 4 | message_id={message_id} | source=llm "
#         f"| company={company} | role={role} | confidence={confidence}"
#     )

#     return LLMExtractedFields(
#         company=company if needs_company else None,
#         role=role if needs_role else None,
#         confidence=confidence,
#         reasoning=reasoning,
#         source="llm",
#     )


"""
Stage 4 — LLM Extraction
Uses Groq llama-3.1-8b-instant to extract all placement fields + eligibility in one call.
"""

import json
import hashlib
import time
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime, timedelta

from config.settings import settings
from utils.logger import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Candidate profile — used for eligibility check inside the LLM prompt
# ---------------------------------------------------------------------------

CANDIDATE_PROFILE = {
    "branch": "Computer Engineering (CE)",
    "cgpa": 9.12,
    "12th_percent": 86.67,
    "10th_percent": 95.2,
    "backlogs": 0,
    "batch": 2027,
}

# ---------------------------------------------------------------------------
# LLM result dataclass
# ---------------------------------------------------------------------------

@dataclass
class LLMExtractedFields:
    company: Optional[str] = None
    roles: list = field(default_factory=list)
    duration: Optional[str] = None
    jd_links: list = field(default_factory=list)
    internal_form_link: Optional[str] = None
    start_date: Optional[str] = None
    location: Optional[str] = None
    package: Optional[str] = None
    deadline: Optional[str] = None        # DD Mon YYYY format
    eligibility_criteria: Optional[str] = None
    eligible: Optional[str] = None        # "Yes", "No", "Maybe"
    eligible_reason: Optional[str] = None
    confidence: float = 0.0
    reasoning: Optional[str] = None
    from_cache: bool = False

    @property
    def role(self) -> Optional[str]:
        """Backward compatibility — returns first role or None."""
        return self.roles[0] if self.roles else None


# ---------------------------------------------------------------------------
# Simple in-memory cache
# ---------------------------------------------------------------------------

_cache: dict = {}


def _cache_key(text: str) -> str:
    return hashlib.sha256(text.strip().lower().encode()).hexdigest()


def _get_cached(text: str) -> Optional[LLMExtractedFields]:
    key = _cache_key(text)
    if key not in _cache:
        return None
    entry = _cache[key]
    ttl_seconds = settings.LLM_CACHE_TTL_HOURS * 3600
    if time.time() - entry["ts"] > ttl_seconds:
        del _cache[key]
        return None
    result = entry["result"]
    result.from_cache = True
    return result


def _set_cached(text: str, result: LLMExtractedFields) -> None:
    _cache[_cache_key(text)] = {"ts": time.time(), "result": result}


# ---------------------------------------------------------------------------
# Daily call counter
# ---------------------------------------------------------------------------

_daily_calls: dict = {"date": None, "count": 0}


def _check_and_increment_daily_limit() -> bool:
    today = datetime.utcnow().date().isoformat()
    if _daily_calls["date"] != today:
        _daily_calls["date"] = today
        _daily_calls["count"] = 0
    if _daily_calls["count"] >= settings.LLM_DAILY_CALL_LIMIT:
        logger.warning(f"LLM daily call limit reached: {settings.LLM_DAILY_CALL_LIMIT}")
        return False
    _daily_calls["count"] += 1
    return True


# ---------------------------------------------------------------------------
# Today's date helper — needed for relative deadline resolution
# ---------------------------------------------------------------------------

def _today_context() -> str:
    now = datetime.utcnow()
    return now.strftime("%d %b %Y")  # e.g. "08 Apr 2026"


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You extract placement opportunity information from WhatsApp messages.
These are messages from an Indian college placement group.
Return ONLY valid JSON. No markdown. No explanation.
If a field cannot be determined confidently, return null.
Never guess. Never invent company names. do not hallucinate. do not write anything in json that is not mentioned in the message.
note:
for extracting roles, go through the message and extract all roles. sometimes format can be like - 'Role: Intern....JD: Data Analyst, ML Dev....Apply here: link' — in this case extract both 'Data Analyst Intern' and 'ML Dev Intern' as roles. Do not just extract 'Intern' as role such a case (this was an example testcase).
branches, if mentioned in eligibility criteria are from [iot, mech, extc, it, comps, aids, aiml, cse (ds)].
for checking eligibility, make sure to compare the eligibility criteria and the candidate profile. check all fields in the candidate profile against the eligibility criteria and then only determine if eligible or not. dont just string match, use common sense.
give confidence basedon extracted fields and not eligibility.

Today's date is {today}.

Candidate profile for eligibility check:
- Branch: {branch}
- CGPA: {cgpa}
- 12th: {twelth}%
- 10th: {tenth}%
- Backlogs: {backlogs}
- Batch: {batch}

Extract these fields and return exactly this JSON structure:
{{
  "company": "string or null",
  "roles": ["list of role strings, empty list if none"],
  "duration": "internship/full-time duration string or null",
  "jd_links": ["list of JD/apply link strings, empty list if none"],
  "internal_form_link": "internal registration form link or null",
  "start_date": "start date string or null",
  "location": "city/remote/hybrid or null",
  "package": "CTC or stipend string or null",
  "deadline": "application deadline in DD Mon YYYY format or null. Resolve relative dates like tomorrow or this Friday to actual dates using today's date.",
  "eligibility_criteria": "raw eligibility criteria string from message or null",
  "eligible": "Yes or No or Maybe or null — based on candidate profile vs eligibility criteria. if no eligibility criteria is given then eligible should be null",
  "eligible_reason": "one sentence explaining why eligible/not/maybe or null. if eligible is null, then write: Eligibility criteria not given",
  "confidence": 0.0,
  "reasoning": "one sentence explaining extraction confidence or null"
}}"""


# ---------------------------------------------------------------------------
# Core extraction function
# ---------------------------------------------------------------------------

def extract_with_llm(
    preprocessed,
    context_fields,
) -> LLMExtractedFields:
    """
    Stage 4 — Extract all placement fields + eligibility using Groq LLM.
    Returns LLMExtractedFields with all fields populated where possible.
    Falls back to empty result on any failure.
    """

    if not settings.LLM_API_KEY:
        logger.debug("LLM_API_KEY not set, skipping LLM extraction")
        return LLMExtractedFields()

    raw_text = preprocessed.cleaned_text or ""
    if not raw_text.strip():
        return LLMExtractedFields()

    # Cache check
    cached = _get_cached(raw_text)
    if cached:
        logger.debug(f"LLM cache hit for message")
        return cached

    # Daily limit check
    if not _check_and_increment_daily_limit():
        return LLMExtractedFields()

    # Build system prompt with today's date and candidate profile
    system = SYSTEM_PROMPT.format(
        today=_today_context(),
        branch=CANDIDATE_PROFILE["branch"],
        cgpa=CANDIDATE_PROFILE["cgpa"],
        twelth=CANDIDATE_PROFILE["12th_percent"],
        tenth=CANDIDATE_PROFILE["10th_percent"],
        backlogs=CANDIDATE_PROFILE["backlogs"],
        batch=CANDIDATE_PROFILE["batch"],
    )

    # Build user message — include reply context if available
    user_content = raw_text
    if context_fields and context_fields.company:
        user_content = f"[Context: this message is related to {context_fields.company}]\n\n{raw_text}"

    try:
        import httpx

        headers = {
            "Authorization": f"Bearer {settings.LLM_API_KEY}",
            "Content-Type": "application/json",
        }

        payload = {
            "model": settings.LLM_MODEL,
            "max_tokens": settings.LLM_MAX_TOKENS,
            "messages": [
                {"role": "user", "content": user_content}
            ],
            "system": system,
        }

        # Groq uses OpenAI-compatible endpoint
        if settings.LLM_PROVIDER == "groq":
            url = "https://api.groq.com/openai/v1/chat/completions"
            # Groq uses messages array with system role
            payload = {
                "model": settings.LLM_MODEL,
                "max_tokens": settings.LLM_MAX_TOKENS,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user_content},
                ],
            }
        else:
            url = "https://api.openai.com/v1/messages"

        with httpx.Client(timeout=15.0) as client:
            response = client.post(url, headers=headers, json=payload)
            response.raise_for_status()

        data = response.json()

        logger.info("RAW LLM RESPONSE: %r", data)

        # Extract text from response
        if settings.LLM_PROVIDER == "groq":
            raw_json = data["choices"][0]["message"]["content"].strip()
        else:
            raw_json = data["content"][0]["text"].strip()

        # Strip markdown fences if present
        if raw_json.startswith("```"):
            lines = raw_json.split("\n")
            raw_json = "\n".join(lines[1:-1]) if len(lines) > 2 else raw_json

        parsed = json.loads(raw_json)

        # Validate confidence
        # confidence = float(parsed.get("confidence", 0.0))
        # if confidence < 0.5:
        #     logger.warning(f"LLM confidence too low: {confidence}, discarding result")
        #     return LLMExtractedFields()
        # Keep extraction if at least one useful field exists,
        # even when model confidence is low.
        has_any_field = any([
            parsed.get("company"),
            parsed.get("roles"),
            parsed.get("duration"),
            parsed.get("jd_links"),
            parsed.get("internal_form_link"),
            parsed.get("start_date"),
            parsed.get("location"),
            parsed.get("package"),
            parsed.get("deadline"),
            parsed.get("eligibility_criteria"),
            parsed.get("eligible"),
        ])

        if not has_any_field:
            logger.warning("LLM returned no useful fields, discarding result")
            return LLMExtractedFields()

        # Ignore model-provided confidence for gating.
        # Compute our own confidence from number of extracted fields.
        field_count = sum(bool(x) for x in [
            parsed.get("company"),
            parsed.get("roles"),
            parsed.get("duration"),
            parsed.get("jd_links"),
            parsed.get("internal_form_link"),
            parsed.get("start_date"),
            parsed.get("location"),
            parsed.get("package"),
            parsed.get("deadline"),
        ])

        confidence = min(1.0, 0.3 + 0.1 * field_count)

        # Validate company — reject person names and generic words
        company = parsed.get("company")
        if company and _looks_like_person_name(company):
            logger.warning(f"LLM returned suspicious company name: {company}, discarding")
            company = None

        result = LLMExtractedFields(
            company=company,
            roles=parsed.get("roles") or [],
            duration=parsed.get("duration"),
            jd_links=parsed.get("jd_links") or [],
            internal_form_link=parsed.get("internal_form_link"),
            start_date=parsed.get("start_date"),
            location=parsed.get("location"),
            package=parsed.get("package"),
            deadline=parsed.get("deadline"),
            eligibility_criteria=parsed.get("eligibility_criteria"),
            eligible=parsed.get("eligible"),
            eligible_reason=parsed.get("eligible_reason"),
            confidence=confidence,
            reasoning=parsed.get("reasoning"),
        )

        _set_cached(raw_text, result)
        logger.info(
            f"LLM extracted — company={result.company}, roles={result.roles}, "
            f"duration={result.duration}, jd_links={result.jd_links}, location={result.location}, package={result.package}, "
            f"deadline={result.deadline}, eligible={result.eligible}, confidence={result.confidence}"
        )
        return result

    except json.JSONDecodeError as e:
        logger.error(f"LLM returned non-JSON response: {e}")
        return LLMExtractedFields()
    except Exception as e:
        logger.error(f"LLM extraction failed: {e}")
        return LLMExtractedFields()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_PERSON_NAME_INDICATORS = {
    "mr", "mrs", "ms", "dr", "prof", "sir", "dear", "hi", "hello",
    "team", "everyone", "all", "students", "candidates", "folks",
}

def _looks_like_person_name(name: str) -> bool:
    """Heuristic: reject obvious non-company strings."""
    if not name:
        return False
    lower = name.lower().strip()
    if lower in _PERSON_NAME_INDICATORS:
        return True
    # Single common first name with no company indicators
    words = lower.split()
    if len(words) == 1 and len(lower) < 4:
        return True
    return False