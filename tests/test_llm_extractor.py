"""
Unit tests for Stage 4 — LLM Extractor.
All OpenAI API calls are mocked. No real API calls are made.
"""

import hashlib
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
import pytest

from extraction.context_resolver import ContextResolvedFields
from extraction.llm_extractor import (
    LLMExtractedFields,
    _cache,
    _cache_key,
    _daily_tracker,
    _is_valid_company,
    _parse_llm_response,
    extract_with_llm,
)
from extraction.preprocessor import PreprocessedMessage


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def make_preprocessed(
    message_id: str = "msg_001",
    cleaned_text: str = "amazon is hiring software engineers apply now",
    urls: list = None,
    is_processable: bool = True,
) -> PreprocessedMessage:
    return PreprocessedMessage(
        message_id=message_id,
        original_text=cleaned_text,
        cleaned_text=cleaned_text,
        urls=urls or [],
        is_processable=is_processable,
        matched_keywords=["hiring", "apply"],
    )


def make_context(
    company: str = None,
    role: str = None,
    source: str = "none",
    confidence: float = 0.30,
) -> ContextResolvedFields:
    return ContextResolvedFields(
        company=company,
        role=role,
        context_source=source,
        confidence=confidence,
    )


def make_openai_response(company: str, role: str, confidence: float, reasoning: str = "test") -> MagicMock:
    """Build a mock that mimics openai ChatCompletion response structure."""
    mock_response = MagicMock()
    mock_response.choices[0].message.content = json.dumps({
        "company": company,
        "role": role,
        "confidence": confidence,
        "reasoning": reasoning,
    })
    return mock_response


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def clear_cache():
    _cache.clear()


def reset_daily_tracker():
    from datetime import date
    _daily_tracker["date"] = date.today()
    _daily_tracker["count"] = 0


# ---------------------------------------------------------------------------
# Tests — skipped (both already resolved)
# ---------------------------------------------------------------------------

class TestSkippedBothResolved:
    def test_skipped_when_both_company_and_role_known(self):
        pre = make_preprocessed()
        ctx = make_context(company="Google", role="SDE", source="reply", confidence=0.95)
        result = extract_with_llm(pre, ctx)
        assert result.source == "skipped"
        assert result.company is None
        assert result.role is None
        assert result.confidence == 0.0

    def test_skipped_does_not_call_openai(self):
        pre = make_preprocessed()
        ctx = make_context(company="Microsoft", role="PM", source="reply", confidence=0.95)
        with patch("extraction.llm_extractor._call_groq") as mock_call:
            extract_with_llm(pre, ctx)
            mock_call.assert_not_called()


# ---------------------------------------------------------------------------
# Tests — cache
# ---------------------------------------------------------------------------

class TestCache:
    def setup_method(self):
        clear_cache()
        reset_daily_tracker()

    def test_cache_hit_returns_cached_result(self):
        pre = make_preprocessed(cleaned_text="infosys hiring data engineer apply now")
        ctx = make_context()
        key = _cache_key(pre.cleaned_text)
        _cache[key] = {
            "company": "Infosys",
            "role": "Data Engineer",
            "confidence": 0.85,
            "reasoning": "cached",
            "cached_at": datetime.now(timezone.utc),
        }
        result = extract_with_llm(pre, ctx)
        assert result.source == "cache"
        assert result.company == "Infosys"
        assert result.role == "Data Engineer"
        assert result.confidence == 0.85

    def test_expired_cache_is_not_used(self):
        pre = make_preprocessed(cleaned_text="tcs hiring freshers apply now")
        ctx = make_context()
        key = _cache_key(pre.cleaned_text)
        from datetime import timedelta
        _cache[key] = {
            "company": "TCS",
            "role": "Trainee",
            "confidence": 0.80,
            "reasoning": "old",
            "cached_at": datetime.now(timezone.utc) - timedelta(hours=25),
        }
        mock_response = make_openai_response("TCS", "Trainee", 0.80)
        with patch("extraction.llm_extractor._call_groq", return_value=json.loads(
            mock_response.choices[0].message.content
        )):
            result = extract_with_llm(pre, ctx)
        assert result.source == "llm"

    def test_cache_key_is_sha256_of_cleaned_text(self):
        text = "wipro hiring backend developer"
        expected = hashlib.sha256(text.encode("utf-8")).hexdigest()
        assert _cache_key(text) == expected

    def test_cache_only_returns_needed_fields(self):
        """If only company is needed, role from cache is not returned."""
        pre = make_preprocessed(cleaned_text="cognizant hiring apply now")
        ctx = make_context(role="SDE")  # role already known
        key = _cache_key(pre.cleaned_text)
        _cache[key] = {
            "company": "Cognizant",
            "role": "Developer",
            "confidence": 0.80,
            "reasoning": "cached",
            "cached_at": datetime.now(timezone.utc),
        }
        result = extract_with_llm(pre, ctx)
        assert result.company == "Cognizant"
        assert result.role is None  # role was already known, not needed from cache


# ---------------------------------------------------------------------------
# Tests — daily limit
# ---------------------------------------------------------------------------

class TestDailyLimit:
    def setup_method(self):
        clear_cache()
        reset_daily_tracker()

    def test_daily_limit_blocks_llm_call(self):
        from config.settings import settings
        _daily_tracker["count"] = settings.LLM_DAILY_CALL_LIMIT
        pre = make_preprocessed(cleaned_text="accenture hiring java developer")
        ctx = make_context()
        with patch("extraction.llm_extractor._call_groq") as mock_call:
            result = extract_with_llm(pre, ctx)
            mock_call.assert_not_called()
        assert result.source == "skipped"

    def test_daily_limit_resets_on_new_day(self):
        from datetime import date, timedelta
        _daily_tracker["date"] = date.today() - timedelta(days=1)
        _daily_tracker["count"] = 9999
        pre = make_preprocessed(cleaned_text="deloitte hiring analyst apply now")
        ctx = make_context()
        mock_response = make_openai_response("Deloitte", "Analyst", 0.80)
        with patch("extraction.llm_extractor._call_groq", return_value=json.loads(
            mock_response.choices[0].message.content
        )):
            result = extract_with_llm(pre, ctx)
        assert result.source == "llm"


# ---------------------------------------------------------------------------
# Tests — response parsing
# ---------------------------------------------------------------------------

class TestParseLLMResponse:
    def test_valid_json_parsed_correctly(self):
        raw = '{"company": "Google", "role": "SDE", "confidence": 0.9, "reasoning": "clear"}'
        result = _parse_llm_response(raw)
        assert result["company"] == "Google"
        assert result["confidence"] == 0.9

    def test_json_with_markdown_fences_stripped(self):
        raw = '```json\n{"company": "Amazon", "role": "SDE2", "confidence": 0.85, "reasoning": "ok"}\n```'
        result = _parse_llm_response(raw)
        assert result["company"] == "Amazon"

    def test_json_with_plain_fences_stripped(self):
        raw = '```\n{"company": "Meta", "role": "PM", "confidence": 0.75, "reasoning": "fine"}\n```'
        result = _parse_llm_response(raw)
        assert result["company"] == "Meta"

    def test_invalid_json_raises_value_error(self):
        with pytest.raises(Exception):
            _parse_llm_response("this is not json at all")

    def test_empty_response_raises(self):
        with pytest.raises(Exception):
            _parse_llm_response("")


# ---------------------------------------------------------------------------
# Tests — company validation
# ---------------------------------------------------------------------------

class TestIsValidCompany:
    def test_valid_company_names(self):
        assert _is_valid_company("Google") is True
        assert _is_valid_company("Tata Consultancy Services") is True
        assert _is_valid_company("Amazon Web Services") is True
        assert _is_valid_company("JP Morgan") is True
        assert _is_valid_company("3M") is True

    def test_person_name_indicators_rejected(self):
        assert _is_valid_company("Mr. Sharma") is False
        assert _is_valid_company("Please contact admin") is False
        assert _is_valid_company("sir") is False
        assert _is_valid_company("Dr. Patel") is False

    def test_too_short_rejected(self):
        assert _is_valid_company("A") is False
        assert _is_valid_company("") is False

    def test_none_rejected(self):
        assert _is_valid_company(None) is False

    def test_no_letters_rejected(self):
        assert _is_valid_company("123") is False


# ---------------------------------------------------------------------------
# Tests — full extract_with_llm flow
# ---------------------------------------------------------------------------

class TestExtractWithLLM:
    def setup_method(self):
        clear_cache()
        reset_daily_tracker()

    def test_successful_llm_call_returns_llm_source(self):
        pre = make_preprocessed(cleaned_text="wipro is hiring python developer apply before march 31")
        ctx = make_context()
        with patch("extraction.llm_extractor._call_groq", return_value={
            "company": "Wipro",
            "role": "Python Developer",
            "confidence": 0.88,
            "reasoning": "clear mention",
        }):
            result = extract_with_llm(pre, ctx)
        assert result.source == "llm"
        assert result.company == "Wipro"
        assert result.role == "Python Developer"
        assert result.confidence == 0.88

    def test_low_confidence_returns_skipped(self):
        pre = make_preprocessed(cleaned_text="someone hiring something somewhere apply")
        ctx = make_context()
        with patch("extraction.llm_extractor._call_groq", return_value={
            "company": "Unknown",
            "role": None,
            "confidence": 0.3,
            "reasoning": "unclear",
        }):
            result = extract_with_llm(pre, ctx)
        assert result.source == "skipped"
        assert result.company is None

    def test_invalid_company_name_rejected(self):
        pre = make_preprocessed(cleaned_text="please contact sir for more details apply now")
        ctx = make_context()
        with patch("extraction.llm_extractor._call_groq", return_value={
            "company": "sir",
            "role": "Developer",
            "confidence": 0.75,
            "reasoning": "extracted sir as company",
        }):
            result = extract_with_llm(pre, ctx)
        assert result.company is None

    def test_llm_api_failure_returns_skipped(self):
        pre = make_preprocessed(cleaned_text="hexaware hiring data scientist apply now")
        ctx = make_context()
        with patch("extraction.llm_extractor._call_groq", side_effect=Exception("API timeout")):
            result = extract_with_llm(pre, ctx)
        assert result.source == "skipped"
        assert result.company is None

    def test_only_company_missing_triggers_llm(self):
        pre = make_preprocessed(cleaned_text="hiring data engineer apply now")
        ctx = make_context(role="Data Engineer")  # role known, company missing
        with patch("extraction.llm_extractor._call_groq", return_value={
            "company": "Snowflake",
            "role": "Data Engineer",
            "confidence": 0.82,
            "reasoning": "extracted company",
        }) as mock_call:
            result = extract_with_llm(pre, ctx)
            mock_call.assert_called_once()
        assert result.company == "Snowflake"
        assert result.role is None  # role was already known

    def test_only_role_missing_triggers_llm(self):
        pre = make_preprocessed(cleaned_text="google hiring apply now")
        ctx = make_context(company="Google")  # company known, role missing
        with patch("extraction.llm_extractor._call_groq", return_value={
            "company": "Google",
            "role": "SWE",
            "confidence": 0.90,
            "reasoning": "extracted role",
        }):
            result = extract_with_llm(pre, ctx)
        assert result.role == "SWE"
        assert result.company is None  # company was already known

    def test_result_is_stored_in_cache_after_llm_call(self):
        pre = make_preprocessed(cleaned_text="capgemini hiring java backend developer apply")
        ctx = make_context()
        with patch("extraction.llm_extractor._call_groq", return_value={
            "company": "Capgemini",
            "role": "Java Developer",
            "confidence": 0.85,
            "reasoning": "clear",
        }):
            extract_with_llm(pre, ctx)
        key = _cache_key(pre.cleaned_text)
        assert key in _cache
        assert _cache[key]["company"] == "Capgemini"

    def test_non_dict_response_returns_skipped(self):
        pre = make_preprocessed(cleaned_text="some company hiring now apply")
        ctx = make_context()
        with patch("extraction.llm_extractor._call_groq", return_value=["not", "a", "dict"]):
            result = extract_with_llm(pre, ctx)
        assert result.source == "skipped"

    def test_hint_not_added_when_context_is_none(self):
        """When context has no company/role, no hint lines appear in user prompt."""
        pre = make_preprocessed(cleaned_text="ltimindtree hiring sde apply now")
        ctx = make_context()  # both None
        captured_args = {}
        def mock_call(user_text):
            captured_args["user_text"] = user_text
            return {"company": "LTIMindtree", "role": "SDE", "confidence": 0.85, "reasoning": "ok"}
        with patch("extraction.llm_extractor._call_groq", side_effect=mock_call):
            extract_with_llm(pre, ctx)
        assert "Hint" not in captured_args["user_text"]