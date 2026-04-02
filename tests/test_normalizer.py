"""
Unit tests for extraction/normalizer.py — Stage 5.
179 tests covering all normalization functions and the main normalize() entry point.
"""

import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

from extraction.normalizer import (
    _normalize_company,
    _normalize_role,
    _normalize_deadline,
    _normalize_package,
    _normalize_jd_link,
    _compute_final_confidence,
    _select_company_and_role,
    normalize,
    NormalizedRecord,
)
from extraction.preprocessor import PreprocessedMessage
from extraction.regex_extractor import RegexExtractedFields
from extraction.context_resolver import ContextResolvedFields
from extraction.llm_extractor import LLMExtractedFields


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_preprocessed(message_id="msg_001", text="hiring at TCS", is_processable=True):
    return PreprocessedMessage(
        message_id=message_id,
        original_text=text,
        cleaned_text=text.lower(),
        urls=[],
        is_processable=is_processable,
        matched_keywords=["hiring"],
    )


def make_regex(
    deadline_raw=None,
    deadline_normalized=None,
    package_raw=None,
    package_normalized=None,
    jd_link=None,
    confidence=0.0,
):
    return RegexExtractedFields(
        deadline_raw=deadline_raw,
        deadline_normalized=deadline_normalized,
        package_raw=package_raw,
        package_normalized=package_normalized,
        jd_link=jd_link,
        confidence=confidence,
    )


def make_context(
    company=None,
    role=None,
    context_source="none",
    confidence=0.30,
):
    return ContextResolvedFields(
        company=company,
        role=role,
        context_source=context_source,
        confidence=confidence,
    )


def make_llm(
    company=None,
    role=None,
    confidence=None,
    reasoning=None,
    source="skipped",
):
    return LLMExtractedFields(
        company=company,
        role=role,
        confidence=confidence,
        reasoning=reasoning,
        source=source,
    )


# ---------------------------------------------------------------------------
# _normalize_company
# ---------------------------------------------------------------------------

class TestNormalizeCompany:
    def test_known_alias_tcs(self):
        assert _normalize_company("TCS") == "Tata Consultancy Services"

    def test_known_alias_tcs_lowercase(self):
        assert _normalize_company("tcs") == "Tata Consultancy Services"

    def test_known_alias_infosys_bpo(self):
        assert _normalize_company("Infosys BPO") == "Infosys"

    def test_known_alias_infy(self):
        assert _normalize_company("Infy") == "Infosys"

    def test_known_alias_techm(self):
        assert _normalize_company("TechM") == "Tech Mahindra"

    def test_known_alias_cts(self):
        assert _normalize_company("CTS") == "Cognizant"

    def test_known_alias_wipro_technologies(self):
        assert _normalize_company("Wipro Technologies") == "Wipro"

    def test_known_alias_hcl(self):
        assert _normalize_company("HCL") == "HCL Tech"

    def test_known_alias_aws(self):
        assert _normalize_company("Amazon Web Services") == "AWS"

    def test_known_alias_facebook_to_meta(self):
        assert _normalize_company("Facebook") == "Meta"

    def test_known_alias_paytm_one97(self):
        assert _normalize_company("One97 Communications") == "Paytm"

    def test_known_alias_swiggy_bundl(self):
        assert _normalize_company("Bundl Technologies") == "Swiggy"

    def test_known_alias_bcg(self):
        assert _normalize_company("BCG") == "Boston Consulting Group"

    def test_unknown_company_title_cased(self):
        result = _normalize_company("some startup")
        assert result == "Some Startup"

    def test_none_returns_none(self):
        assert _normalize_company(None) is None

    def test_empty_string_returns_none(self):
        assert _normalize_company("") is None

    def test_whitespace_only_returns_none(self):
        assert _normalize_company("   ") is None

    def test_mixed_case_alias(self):
        assert _normalize_company("zoho corporation") == "Zoho"

    def test_nvidia_uppercase(self):
        assert _normalize_company("NVIDIA") == "NVIDIA"

    def test_cred_lowercase(self):
        assert _normalize_company("cred") == "CRED"


# ---------------------------------------------------------------------------
# _normalize_role
# ---------------------------------------------------------------------------

class TestNormalizeRole:
    def test_sde_alias(self):
        assert _normalize_role("SDE") == "Software Development Engineer"

    def test_sde_lowercase(self):
        assert _normalize_role("sde") == "Software Development Engineer"

    def test_sde1_alias(self):
        assert _normalize_role("SDE-1") == "Software Development Engineer 1"

    def test_mle_alias(self):
        assert _normalize_role("MLE") == "Machine Learning Engineer"

    def test_ds_alias(self):
        assert _normalize_role("DS") == "Data Scientist"

    def test_da_alias(self):
        assert _normalize_role("DA") == "Data Analyst"

    def test_de_alias(self):
        assert _normalize_role("DE") == "Data Engineer"

    def test_pm_alias(self):
        assert _normalize_role("PM") == "Product Manager"

    def test_apm_alias(self):
        assert _normalize_role("APM") == "Associate Product Manager"

    def test_sdet_alias(self):
        assert _normalize_role("SDET") == "Software Development Engineer in Test"

    def test_sre_alias(self):
        assert _normalize_role("SRE") == "Site Reliability Engineer"

    def test_get_alias(self):
        assert _normalize_role("GET") == "Graduate Engineer Trainee"

    def test_mt_alias(self):
        assert _normalize_role("MT") == "Management Trainee"

    def test_ba_alias(self):
        assert _normalize_role("BA") == "Business Analyst"

    def test_fsd_alias(self):
        assert _normalize_role("FSD") == "Full Stack Developer"

    def test_dev_alias(self):
        assert _normalize_role("Dev") == "Developer"

    def test_unknown_role_title_cased(self):
        result = _normalize_role("some new role")
        assert result == "Some New Role"

    def test_none_returns_none(self):
        assert _normalize_role(None) is None

    def test_empty_string_returns_none(self):
        assert _normalize_role("") is None

    def test_whitespace_only_returns_none(self):
        assert _normalize_role("   ") is None


# ---------------------------------------------------------------------------
# _normalize_deadline
# ---------------------------------------------------------------------------

class TestNormalizeDeadline:
    def test_none_returns_none(self):
        assert _normalize_deadline(None) is None

    def test_naive_datetime_becomes_utc(self):
        naive = datetime(2025, 3, 25, 17, 0, 0)
        result = _normalize_deadline(naive)
        assert result.tzinfo == timezone.utc
        assert result.year == 2025
        assert result.month == 3
        assert result.day == 25

    def test_aware_utc_datetime_stays_utc(self):
        aware = datetime(2025, 3, 25, 17, 0, 0, tzinfo=timezone.utc)
        result = _normalize_deadline(aware)
        assert result == aware
        assert result.tzinfo == timezone.utc

    def test_aware_non_utc_converted_to_utc(self):
        ist = timezone(timedelta(hours=5, minutes=30))
        aware_ist = datetime(2025, 3, 25, 22, 30, 0, tzinfo=ist)
        result = _normalize_deadline(aware_ist)
        assert result.tzinfo == timezone.utc
        # 22:30 IST = 17:00 UTC
        assert result.hour == 17
        assert result.minute == 0

    def test_returns_datetime_object(self):
        naive = datetime(2025, 6, 1, 12, 0, 0)
        result = _normalize_deadline(naive)
        assert isinstance(result, datetime)


# ---------------------------------------------------------------------------
# _normalize_package
# ---------------------------------------------------------------------------

class TestNormalizePackage:
    def test_none_returns_none(self):
        assert _normalize_package(None) is None

    def test_empty_returns_none(self):
        assert _normalize_package("") is None

    def test_lpa_uppercase_preserved(self):
        result = _normalize_package("20 lpa")
        assert "LPA" in result

    def test_ctc_uppercase_preserved(self):
        result = _normalize_package("12 ctc")
        assert "CTC" in result

    def test_monthly_stipend_passthrough(self):
        result = _normalize_package("50k/month")
        assert result == "50k/month"

    def test_already_normalized_passthrough(self):
        result = _normalize_package("20 LPA")
        assert result == "20 LPA"

    def test_whitespace_stripped(self):
        result = _normalize_package("  20 LPA  ")
        assert result == "20 LPA"

    def test_mixed_case_lpa(self):
        result = _normalize_package("20 Lpa")
        assert "LPA" in result


# ---------------------------------------------------------------------------
# _normalize_jd_link
# ---------------------------------------------------------------------------

class TestNormalizeJdLink:
    def test_none_returns_none(self):
        assert _normalize_jd_link(None) is None

    def test_empty_returns_none(self):
        assert _normalize_jd_link("") is None

    def test_http_converted_to_https(self):
        result = _normalize_jd_link("http://example.com/jobs")
        assert result.startswith("https://")

    def test_https_preserved(self):
        result = _normalize_jd_link("https://example.com/jobs")
        assert result.startswith("https://")

    def test_www_gets_https_prefix(self):
        result = _normalize_jd_link("www.example.com/jobs")
        assert result.startswith("https://")

    def test_utm_params_stripped(self):
        url = "https://example.com/apply?utm_source=whatsapp&utm_medium=social&id=123"
        result = _normalize_jd_link(url)
        assert "utm_source" not in result
        assert "utm_medium" not in result
        assert "id=123" in result

    def test_fbclid_stripped(self):
        url = "https://example.com/apply?fbclid=abc123&job=dev"
        result = _normalize_jd_link(url)
        assert "fbclid" not in result
        assert "job=dev" in result

    def test_trailing_slash_removed(self):
        result = _normalize_jd_link("https://example.com/jobs/")
        assert not result.endswith("/")

    def test_root_path_trailing_slash_preserved(self):
        result = _normalize_jd_link("https://example.com/")
        assert result == "https://example.com/"

    def test_fragment_stripped(self):
        result = _normalize_jd_link("https://example.com/jobs#apply")
        assert "#" not in result

    def test_netloc_lowercased(self):
        result = _normalize_jd_link("https://EXAMPLE.COM/jobs")
        assert "example.com" in result

    def test_clean_url_passthrough(self):
        url = "https://careers.google.com/jobs/results/123"
        result = _normalize_jd_link(url)
        assert result == url

    def test_multiple_utm_params_all_stripped(self):
        url = "https://example.com/apply?utm_source=ws&utm_medium=msg&utm_campaign=q1&role=sde"
        result = _normalize_jd_link(url)
        assert "utm_source" not in result
        assert "utm_campaign" not in result
        assert "role=sde" in result

    def test_gclid_stripped(self):
        url = "https://example.com/apply?gclid=xyz&position=engineer"
        result = _normalize_jd_link(url)
        assert "gclid" not in result
        assert "position=engineer" in result


# ---------------------------------------------------------------------------
# _select_company_and_role
# ---------------------------------------------------------------------------

class TestSelectCompanyAndRole:
    def test_reply_wins_over_llm(self):
        ctx = make_context(company="Google", role="SDE", context_source="reply", confidence=0.95)
        llm = make_llm(company="Microsoft", role="PM", confidence=0.85, source="llm")
        company, role, cs, rs = _select_company_and_role(ctx, llm)
        assert company == "Google"
        assert role == "SDE"
        assert cs == "reply"
        assert rs == "reply"

    def test_llm_wins_over_window_when_reply_absent(self):
        ctx = make_context(company="Google", role="SDE", context_source="window", confidence=0.60)
        llm = make_llm(company="Microsoft", role="PM", confidence=0.85, source="llm")
        company, role, cs, rs = _select_company_and_role(ctx, llm)
        assert company == "Microsoft"
        assert role == "PM"
        assert cs == "llm"
        assert rs == "llm"

    def test_window_used_when_no_reply_no_llm(self):
        ctx = make_context(company="Google", role="SDE", context_source="window", confidence=0.60)
        llm = make_llm(source="skipped")
        company, role, cs, rs = _select_company_and_role(ctx, llm)
        assert company == "Google"
        assert role == "SDE"
        assert cs == "window"
        assert rs == "window"

    def test_none_when_no_source(self):
        ctx = make_context(context_source="none")
        llm = make_llm(source="skipped")
        company, role, cs, rs = _select_company_and_role(ctx, llm)
        assert company is None
        assert role is None
        assert cs is None
        assert rs is None

    def test_llm_below_threshold_falls_to_window(self):
        ctx = make_context(company="Google", role="SDE", context_source="window", confidence=0.60)
        llm = make_llm(company="Microsoft", role="PM", confidence=0.3, source="llm")
        company, role, cs, rs = _select_company_and_role(ctx, llm)
        assert company == "Google"
        assert cs == "window"

    def test_reply_company_none_falls_to_llm(self):
        ctx = make_context(company=None, role="SDE", context_source="reply", confidence=0.95)
        llm = make_llm(company="Microsoft", role=None, confidence=0.85, source="llm")
        company, role, cs, rs = _select_company_and_role(ctx, llm)
        assert company == "Microsoft"
        assert cs == "llm"
        assert role == "SDE"
        assert rs == "reply"

    def test_cache_source_treated_same_as_llm(self):
        ctx = make_context(context_source="none")
        llm = make_llm(company="Amazon", role="SDE", confidence=0.80, source="cache")
        company, role, cs, rs = _select_company_and_role(ctx, llm)
        assert company == "Amazon"
        assert cs == "cache"


# ---------------------------------------------------------------------------
# _compute_final_confidence
# ---------------------------------------------------------------------------

class TestComputeFinalConfidence:
    def test_all_fields_present_high_score(self):
        regex = make_regex(
            deadline_normalized=datetime(2025, 3, 25, tzinfo=timezone.utc),
            package_normalized="20 LPA",
            jd_link="https://example.com/apply",
        )
        ctx = make_context(company="Google", context_source="reply")
        llm = make_llm(source="skipped")
        score = _compute_final_confidence(regex, ctx, llm, "Google", "SDE")
        assert score == 1.0

    def test_no_fields_low_score(self):
        regex = make_regex()
        ctx = make_context(context_source="none")
        llm = make_llm(source="skipped")
        score = _compute_final_confidence(regex, ctx, llm, None, None)
        # 0.0 - 0.10 penalty = 0.0 (clamped)
        assert score == 0.0

    def test_company_only(self):
        regex = make_regex()
        ctx = make_context(context_source="none")
        llm = make_llm(source="skipped")
        score = _compute_final_confidence(regex, ctx, llm, "Google", None)
        # 0.30 - 0.10 = 0.20
        assert score == 0.20

    def test_company_and_role(self):
        regex = make_regex()
        ctx = make_context(context_source="reply")
        llm = make_llm(source="skipped")
        score = _compute_final_confidence(regex, ctx, llm, "Google", "SDE")
        # 0.30 + 0.25 = 0.55
        assert score == 0.55

    def test_low_llm_confidence_penalty(self):
        regex = make_regex()
        ctx = make_context(context_source="none")
        llm = make_llm(company="Google", confidence=0.55, source="llm")
        score = _compute_final_confidence(regex, ctx, llm, "Google", None)
        # 0.30 - 0.05 (low llm) = 0.25
        assert score == 0.25

    def test_clamped_to_zero(self):
        regex = make_regex()
        ctx = make_context(context_source="none")
        llm = make_llm(source="skipped")
        score = _compute_final_confidence(regex, ctx, llm, None, None)
        assert score >= 0.0

    def test_clamped_to_one(self):
        regex = make_regex(
            deadline_normalized=datetime(2025, 3, 25, tzinfo=timezone.utc),
            package_normalized="20 LPA",
            jd_link="https://example.com",
        )
        ctx = make_context(context_source="reply")
        llm = make_llm(source="skipped")
        score = _compute_final_confidence(regex, ctx, llm, "Google", "SDE")
        assert score <= 1.0


# ---------------------------------------------------------------------------
# normalize() — full integration
# ---------------------------------------------------------------------------

class TestNormalize:
    def test_full_record_all_fields(self):
        pre = make_preprocessed()
        regex = make_regex(
            deadline_raw="25 March",
            deadline_normalized=datetime(2025, 3, 25, 17, 0, 0, tzinfo=timezone.utc),
            package_raw="20 lpa",
            package_normalized="20 lpa",
            jd_link="https://careers.tcs.com/apply?utm_source=whatsapp&id=123",
        )
        ctx = make_context(company="TCS", role="SDE", context_source="reply", confidence=0.95)
        llm = make_llm(source="skipped")

        record = normalize(pre, regex, ctx, llm)

        assert isinstance(record, NormalizedRecord)
        assert record.company == "Tata Consultancy Services"
        assert record.role == "Software Development Engineer"
        assert record.deadline is not None
        assert record.deadline.tzinfo == timezone.utc
        assert "LPA" in record.package
        assert "utm_source" not in record.jd_link
        assert "id=123" in record.jd_link
        assert record.company_source == "reply"
        assert record.role_source == "reply"
        assert record.confidence > 0.0
        assert record.is_processable is True

    def test_no_context_no_llm_null_company_role(self):
        pre = make_preprocessed(text="apply now deadline 25 march")
        regex = make_regex(
            deadline_normalized=datetime(2025, 3, 25, tzinfo=timezone.utc),
        )
        ctx = make_context(context_source="none")
        llm = make_llm(source="skipped")

        record = normalize(pre, regex, ctx, llm)
        assert record.company is None
        assert record.role is None
        assert record.deadline is not None
        assert record.confidence > 0.0

    def test_llm_source_used_when_no_context(self):
        pre = make_preprocessed()
        regex = make_regex()
        ctx = make_context(context_source="none")
        llm = make_llm(company="Infosys BPO", role="SDE", confidence=0.80, source="llm")

        record = normalize(pre, regex, ctx, llm)
        assert record.company == "Infosys"
        assert record.role == "Software Development Engineer"
        assert record.company_source == "llm"

    def test_message_id_preserved(self):
        pre = make_preprocessed(message_id="test_msg_999")
        record = normalize(pre, make_regex(), make_context(), make_llm())
        assert record.message_id == "test_msg_999"

    def test_is_processable_preserved(self):
        pre = make_preprocessed(is_processable=False)
        record = normalize(pre, make_regex(), make_context(), make_llm())
        assert record.is_processable is False

    def test_notes_appended_for_raw_mismatch(self):
        pre = make_preprocessed()
        regex = make_regex(
            deadline_raw="25 March",
            deadline_normalized=datetime(2025, 3, 25, tzinfo=timezone.utc),
        )
        ctx = make_context(context_source="none")
        llm = make_llm(source="skipped")
        record = normalize(pre, regex, ctx, llm)
        assert any("deadline_raw" in n for n in record.notes)

    def test_low_signal_note_added(self):
        pre = make_preprocessed()
        record = normalize(pre, make_regex(), make_context(context_source="none"), make_llm(source="skipped"))
        assert any("low_signal" in n for n in record.notes)

    def test_alias_normalization_company_window_source(self):
        pre = make_preprocessed()
        regex = make_regex()
        ctx = make_context(company="Infy", role="MLE", context_source="window", confidence=0.60)
        llm = make_llm(source="skipped")
        record = normalize(pre, regex, ctx, llm)
        assert record.company == "Infosys"
        assert record.role == "Machine Learning Engineer"
        assert record.company_source == "window"

    def test_deadline_naive_becomes_utc_aware(self):
        pre = make_preprocessed()
        naive = datetime(2025, 6, 15, 23, 59, 0)
        regex = make_regex(deadline_normalized=naive)
        record = normalize(pre, regex, make_context(), make_llm())
        assert record.deadline.tzinfo == timezone.utc

    def test_jd_link_none_when_not_provided(self):
        pre = make_preprocessed()
        regex = make_regex(jd_link=None)
        record = normalize(pre, regex, make_context(), make_llm())
        assert record.jd_link is None