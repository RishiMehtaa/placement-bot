"""
Unit tests for extraction/regex_extractor.py — Stage 2.
Run with: docker compose exec fastapi python -m pytest tests/test_regex_extractor.py -v
"""

import pytest
from datetime import datetime, timezone
from extraction.preprocessor import preprocess
from extraction.regex_extractor import (
    extract_deadline,
    extract_package,
    extract_jd_link,
    extract_with_regex,
    RegexExtractedFields,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_preprocessed(text: str, message_id: str = "test-001"):
    return preprocess(message_id, text)


# ---------------------------------------------------------------------------
# Deadline tests
# ---------------------------------------------------------------------------

class TestDeadlineExtraction:

    def test_day_month_name(self):
        raw, normalized = extract_deadline("apply by 25 march")
        assert raw is not None
        assert "25" in raw or "march" in raw.lower()
        assert normalized is not None
        assert normalized.month == 3
        assert normalized.day == 25

    def test_day_ordinal_month_name(self):
        raw, normalized = extract_deadline("deadline is 25th march 2025")
        assert normalized is not None
        assert normalized.month == 3
        assert normalized.day == 25

    def test_month_name_day(self):
        raw, normalized = extract_deadline("last date: march 25")
        assert normalized is not None
        assert normalized.month == 3
        assert normalized.day == 25

    def test_slash_date_with_year(self):
        raw, normalized = extract_deadline("deadline: 25/03/2025")
        assert normalized is not None
        assert normalized.month == 3
        assert normalized.day == 25
        assert normalized.year == 2025

    def test_slash_date_without_year(self):
        raw, normalized = extract_deadline("apply before 10/08")
        assert normalized is not None
        assert normalized.month == 8
        assert normalized.day == 10

    def test_tomorrow(self):
        raw, normalized = extract_deadline("register by tomorrow")
        assert raw == "tomorrow"
        assert normalized is not None
        today = datetime.now(timezone.utc)
        assert normalized.day == (today.day + 1) or normalized.month >= today.month

    def test_this_friday(self):
        raw, normalized = extract_deadline("apply by this friday")
        assert normalized is not None
        assert normalized.weekday() == 4  # Friday

    def test_this_week(self):
        raw, normalized = extract_deadline("apply this week")
        assert normalized is not None
        # Should be Sunday of current week
        assert normalized.weekday() == 6

    def test_by_eod(self):
        raw, normalized = extract_deadline("submit by EOD")
        assert raw is not None
        assert normalized is not None

    def test_by_midnight(self):
        raw, normalized = extract_deadline("apply by midnight")
        assert raw is not None
        assert normalized is not None

    def test_by_5pm(self):
        raw, normalized = extract_deadline("submit by 5pm today")
        assert raw is not None
        assert normalized is not None

    def test_no_deadline(self):
        raw, normalized = extract_deadline("we are hiring software engineers")
        assert raw is None
        assert normalized is None

    def test_deadline_in_full_message(self):
        raw, normalized = extract_deadline(
            "amazon is hiring sde interns. deadline: 15 april. apply at careers.amazon.com"
        )
        assert normalized is not None
        assert normalized.month == 4
        assert normalized.day == 15

    def test_two_digit_year(self):
        raw, normalized = extract_deadline("apply before 10/08/25")
        assert normalized is not None
        assert normalized.year == 2025


# ---------------------------------------------------------------------------
# Package tests
# ---------------------------------------------------------------------------

class TestPackageExtraction:

    def test_lpa(self):
        raw, normalized = extract_package("package: 20 lpa")
        assert raw is not None
        assert normalized == "20.0 LPA"

    def test_lpa_uppercase(self):
        raw, normalized = extract_package("CTC 12 LPA")
        assert normalized is not None
        assert "12" in normalized

    def test_lakh(self):
        raw, normalized = extract_package("salary 12 lakh per annum")
        assert normalized is not None
        assert "12" in normalized
        assert "LPA" in normalized

    def test_lakhs(self):
        raw, normalized = extract_package("offering 15 lakhs")
        assert normalized is not None
        assert "15" in normalized

    def test_k_month(self):
        raw, normalized = extract_package("stipend 50k/month")
        assert normalized is not None
        assert "50,000" in normalized or "50000" in normalized

    def test_inr_month(self):
        raw, normalized = extract_package("₹50,000/month")
        assert normalized is not None
        assert "50,000" in normalized

    def test_stipend_bare_number(self):
        raw, normalized = extract_package("stipend: 25000")
        assert raw is not None
        assert normalized is not None
        assert "25,000" in normalized or "25000" in normalized

    def test_stipend_k(self):
        raw, normalized = extract_package("stipend of 30k")
        assert normalized is not None
        assert "30,000" in normalized

    def test_ctc_prefix(self):
        raw, normalized = extract_package("ctc: 18 lpa")
        assert normalized is not None
        assert "18" in normalized

    def test_package_prefix(self):
        raw, normalized = extract_package("package: 22 lpa")
        assert normalized is not None
        assert "22" in normalized

    def test_no_package(self):
        raw, normalized = extract_package("apply at careers.amazon.com by march 25")
        assert raw is None
        assert normalized is None

    def test_package_in_full_message(self):
        raw, normalized = extract_package(
            "google hiring sde-2. package 45 lpa. apply by 30 april"
        )
        assert normalized is not None
        assert "45" in normalized


# ---------------------------------------------------------------------------
# JD link tests
# ---------------------------------------------------------------------------

class TestJDLinkExtraction:

    def test_apply_keyword_preferred(self):
        urls = ["https://company.com/about", "https://company.com/apply"]
        result = extract_jd_link(urls)
        assert result == "https://company.com/apply"

    def test_form_keyword(self):
        urls = ["https://company.com/home", "https://docs.google.com/forms/abc123"]
        result = extract_jd_link(urls)
        assert "form" in result.lower()

    def test_careers_keyword(self):
        urls = ["https://amazon.com/careers/sde"]
        result = extract_jd_link(urls)
        assert result == "https://amazon.com/careers/sde"

    def test_jobs_keyword(self):
        urls = ["https://company.com/jobs/intern"]
        result = extract_jd_link(urls)
        assert result == "https://company.com/jobs/intern"

    def test_register_keyword(self):
        urls = ["https://unstop.com/register/12345"]
        result = extract_jd_link(urls)
        assert result == "https://unstop.com/register/12345"

    def test_fallback_first_url(self):
        urls = ["https://company.com/about-us", "https://company.com/team"]
        result = extract_jd_link(urls)
        assert result == "https://company.com/about-us"

    def test_empty_urls(self):
        result = extract_jd_link([])
        assert result is None

    def test_prefer_apply_over_first(self):
        urls = [
            "https://company.com/home",
            "https://company.com/news",
            "https://company.com/careers/apply-now",
        ]
        result = extract_jd_link(urls)
        assert "apply" in result.lower() or "careers" in result.lower()


# ---------------------------------------------------------------------------
# Confidence scoring tests
# ---------------------------------------------------------------------------

class TestConfidenceScoring:

    def test_all_fields_max_confidence(self):
        preprocessed = make_preprocessed(
            "amazon hiring sde intern. package 20 lpa. deadline 25 march. apply at https://amazon.com/careers"
        )
        result = extract_with_regex(preprocessed)
        assert result.confidence >= 0.9

    def test_only_link_confidence(self):
        preprocessed = make_preprocessed(
            "check this out https://company.com/apply"
        )
        result = extract_with_regex(preprocessed)
        assert result.jd_link is not None
        assert result.confidence == pytest.approx(0.3, abs=0.05)

    def test_no_fields_zero_confidence(self):
        preprocessed = make_preprocessed(
            "placement drive happening soon register now"
        )
        result = extract_with_regex(preprocessed)
        # No URL and no package and likely no deadline → confidence low
        assert result.confidence < 0.5


# ---------------------------------------------------------------------------
# Integration tests — extract_with_regex end-to-end
# ---------------------------------------------------------------------------

class TestExtractWithRegex:

    def test_full_message_all_fields(self):
        preprocessed = make_preprocessed(
            "google is hiring sde interns. package: 45 lpa. "
            "deadline: 30 april. apply at https://careers.google.com/apply"
        )
        result = extract_with_regex(preprocessed)
        assert result.deadline_normalized is not None
        assert result.package_normalized is not None
        assert result.jd_link is not None
        assert result.confidence >= 0.9

    def test_message_with_only_deadline(self):
        preprocessed = make_preprocessed(
            "last date to apply is 15th june. hiring for various roles"
        )
        result = extract_with_regex(preprocessed)
        assert result.deadline_normalized is not None
        assert result.package_normalized is None

    def test_message_with_stipend(self):
        preprocessed = make_preprocessed(
            "internship opportunity. stipend: 30k/month. apply by this friday at https://unstop.com/register/999"
        )
        result = extract_with_regex(preprocessed)
        assert result.package_normalized is not None
        assert "30,000" in result.package_normalized
        assert result.jd_link is not None
        assert result.deadline_normalized is not None

    def test_non_processable_still_runs(self):
        """extract_with_regex can be called on any PreprocessedMessage safely"""
        preprocessed = make_preprocessed("hello how are you")
        # is_processable=False — but the function itself doesn't gate on this
        result = extract_with_regex(preprocessed)
        assert isinstance(result, RegexExtractedFields)

    def test_returns_dataclass(self):
        preprocessed = make_preprocessed("hiring drive at amazon apply now")
        result = extract_with_regex(preprocessed)
        assert isinstance(result, RegexExtractedFields)

    def test_idempotent(self):
        """Running twice on same input gives same output"""
        preprocessed = make_preprocessed(
            "package 20 lpa deadline 25 march apply at https://company.com/careers"
        )
        result1 = extract_with_regex(preprocessed)
        result2 = extract_with_regex(preprocessed)
        assert result1.deadline_raw == result2.deadline_raw
        assert result1.package_normalized == result2.package_normalized
        assert result1.jd_link == result2.jd_link
        assert result1.confidence == result2.confidence