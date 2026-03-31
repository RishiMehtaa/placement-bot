"""
Unit tests for Stage 3 — Context Resolver.
Tests run outside Docker using pure Python — no DB required.
All DB interactions are tested via mocked Message objects.
"""

import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from extraction.context_resolver import (
    resolve_context,
    _extract_company_from_text,
    _extract_role_from_text,
    _extract_from_reply_preview,
    _extract_from_window,
    _clean_match,
    CONFIDENCE_REPLY,
    CONFIDENCE_WINDOW,
    CONFIDENCE_NONE,
    ContextResolvedFields,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_message(
    message_id: str = "msg_001",
    text: str = "",
    reply_to_preview: str = None,
    timestamp: datetime = None,
    sender: str = "91XXXXXXXXXX@s.whatsapp.net",
) -> MagicMock:
    """Create a mock Message ORM object."""
    msg = MagicMock()
    msg.message_id = message_id
    msg.text = text
    msg.reply_to_preview = reply_to_preview
    msg.timestamp = timestamp or datetime(2025, 3, 15, 10, 0, 0, tzinfo=timezone.utc)
    msg.sender = sender
    return msg


# ---------------------------------------------------------------------------
# _clean_match tests
# ---------------------------------------------------------------------------

class TestCleanMatch:
    def test_basic_string_returned(self):
        assert _clean_match("Google") == "Google"

    def test_strips_whitespace(self):
        assert _clean_match("  Infosys  ") == "Infosys"

    def test_strips_trailing_punctuation(self):
        assert _clean_match("Amazon.") == "Amazon"
        assert _clean_match("TCS,") == "TCS"

    def test_strips_trailing_noise_words(self):
        result = _clean_match("Google and")
        assert result == "Google"

    def test_returns_none_for_empty(self):
        assert _clean_match("") is None

    def test_returns_none_for_single_char(self):
        assert _clean_match("A") is None

    def test_returns_none_for_all_noise(self):
        assert _clean_match("hiring") is None
        assert _clean_match("and or the") is None

    def test_preserves_ampersand(self):
        assert _clean_match("Tata & Sons") == "Tata & Sons"

    def test_preserves_dots_in_name(self):
        result = _clean_match("D.E. Shaw")
        assert result == "D.E. Shaw"


# ---------------------------------------------------------------------------
# _extract_company_from_text tests
# ---------------------------------------------------------------------------

class TestExtractCompany:
    def test_is_hiring_pattern(self):
        result = _extract_company_from_text("Google is hiring for SDE role")
        assert result == "Google"

    def test_hiring_at_pattern(self):
        result = _extract_company_from_text("Opportunity hiring at Infosys for freshers")
        assert result == "Infosys"

    def test_opportunity_at_pattern(self):
        result = _extract_company_from_text("Great opportunity at Microsoft for interns")
        assert result == "Microsoft"

    def test_header_colon_pattern(self):
        result = _extract_company_from_text("Amazon: SDE Intern | 12 LPA | Apply by 25 March")
        assert result == "Amazon"

    def test_header_dash_pattern(self):
        result = _extract_company_from_text("Flipkart — Software Engineer | Deadline: 30th March")
        assert result == "Flipkart"

    def test_drive_by_pattern(self):
        result = _extract_company_from_text("Campus drive by Wipro next week")
        assert result == "Wipro"

    def test_no_company_returns_none(self):
        result = _extract_company_from_text("apply before 25 march deadline 20 lpa")
        assert result is None

    def test_off_campus_pattern(self):
        result = _extract_company_from_text("Zomato off-campus hiring 2025")
        assert result == "Zomato"

    def test_apply_at_pattern(self):
        result = _extract_company_from_text("Apply at Swiggy before March 31st")
        assert result == "Swiggy"

    def test_multiword_company(self):
        result = _extract_company_from_text("Tata Consultancy Services is hiring freshers")
        assert result is not None
        assert "Tata" in result


# ---------------------------------------------------------------------------
# _extract_role_from_text tests
# ---------------------------------------------------------------------------

class TestExtractRole:
    def test_role_colon_pattern(self):
        result = _extract_role_from_text("Role: Software Engineer")
        assert result == "Software Engineer"

    def test_position_colon_pattern(self):
        result = _extract_role_from_text("Position: Data Analyst")
        assert result == "Data Analyst"

    def test_as_a_pattern(self):
        result = _extract_role_from_text("Join us as a Product Manager")
        assert result == "Product Manager"

    def test_for_the_role_of(self):
        result = _extract_role_from_text("Applying for the role of Backend Developer")
        assert result == "Backend Developer"

    def test_intern_pattern(self):
        result = _extract_role_from_text("SDE intern position open at Razorpay")
        assert result is not None
        assert "SDE" in result

    def test_software_intern(self):
        result = _extract_role_from_text("Software intern opening deadline 25 March")
        assert result is not None

    def test_no_role_returns_none(self):
        result = _extract_role_from_text("apply before 25 march 20 lpa google.com/apply")
        assert result is None

    def test_designation_pattern(self):
        result = _extract_role_from_text("Designation: Machine Learning Engineer")
        assert result == "Machine Learning Engineer"

    def test_profile_pattern(self):
        result = _extract_role_from_text("Profile: Full Stack Developer")
        assert result == "Full Stack Developer"


# ---------------------------------------------------------------------------
# _extract_from_reply_preview tests
# ---------------------------------------------------------------------------

class TestExtractFromReplyPreview:
    def test_company_from_preview(self):
        company, role = _extract_from_reply_preview(
            "Google is hiring for SDE | 25 LPA | Apply by 30 March"
        )
        assert company == "Google"

    def test_role_from_preview(self):
        company, role = _extract_from_reply_preview(
            "Role: Data Analyst | Deadline: 25 March"
        )
        assert role == "Data Analyst"

    def test_both_from_preview(self):
        company, role = _extract_from_reply_preview(
            "Amazon: Software Engineer | Apply by March 31"
        )
        assert company == "Amazon"
        assert role is not None

    def test_no_match_returns_none_none(self):
        company, role = _extract_from_reply_preview(
            "apply before the deadline link in bio"
        )
        assert company is None
        assert role is None

    def test_empty_preview(self):
        company, role = _extract_from_reply_preview("")
        assert company is None
        assert role is None


# ---------------------------------------------------------------------------
# _extract_from_window tests
# ---------------------------------------------------------------------------

class TestExtractFromWindow:
    def test_company_from_first_window_message(self):
        msgs = [
            make_message("w1", "Flipkart is hiring Software Engineers"),
            make_message("w2", "Apply before 25 March"),
        ]
        company, role = _extract_from_window(msgs)
        assert company == "Flipkart"

    def test_role_from_second_window_message(self):
        msgs = [
            make_message("w1", "Apply before 25 March deadline"),
            make_message("w2", "Role: Backend Engineer at Paytm"),
        ]
        company, role = _extract_from_window(msgs)
        assert role == "Backend Engineer"

    def test_both_from_different_messages(self):
        msgs = [
            make_message("w1", "Ola is hiring freshers this season"),
            make_message("w2", "Position: Data Scientist apply now"),
        ]
        company, role = _extract_from_window(msgs)
        assert company == "Ola"
        assert role == "Data Scientist"

    def test_empty_window_returns_none_none(self):
        company, role = _extract_from_window([])
        assert company is None
        assert role is None

    def test_window_with_no_matches(self):
        msgs = [
            make_message("w1", "please apply before the deadline"),
            make_message("w2", "link is in the description"),
        ]
        company, role = _extract_from_window(msgs)
        assert company is None
        assert role is None

    def test_stops_after_finding_both(self):
        msgs = [
            make_message("w1", "Microsoft is hiring SDE interns"),
            make_message("w2", "Infosys off-campus drive"),
            make_message("w3", "Role: DevOps Engineer"),
        ]
        company, role = _extract_from_window(msgs)
        # Should pick up Microsoft from first message
        assert company == "Microsoft"


# ---------------------------------------------------------------------------
# resolve_context — Priority 1: reply
# ---------------------------------------------------------------------------

class TestResolveContextReply:
    def test_reply_with_company_returns_reply_source(self):
        current = make_message(
            "msg_001",
            text="yes I will apply",
            reply_to_preview="Google is hiring SDE 25 LPA deadline 30 March",
        )
        result = resolve_context(current, window_messages=[])
        assert result.context_source == "reply"
        assert result.confidence == CONFIDENCE_REPLY
        assert result.company == "Google"

    def test_reply_with_role_returns_reply_source(self):
        current = make_message(
            "msg_002",
            text="link?",
            reply_to_preview="Role: Data Analyst apply before 25 March",
        )
        result = resolve_context(current, window_messages=[])
        assert result.context_source == "reply"
        assert result.confidence == CONFIDENCE_REPLY
        assert result.role == "Data Analyst"

    def test_reply_preview_no_match_falls_through_to_window(self):
        window = [make_message("w1", "Amazon is hiring SDE interns")]
        current = make_message(
            "msg_003",
            text="any update?",
            reply_to_preview="please check the link above",
        )
        result = resolve_context(current, window_messages=window)
        # reply preview had no company/role, falls through to window
        assert result.context_source == "window"
        assert result.confidence == CONFIDENCE_WINDOW

    def test_no_reply_preview_skips_to_window(self):
        window = [make_message("w1", "Meesho is hiring backend engineers")]
        current = make_message("msg_004", text="deadline?", reply_to_preview=None)
        result = resolve_context(current, window_messages=window)
        assert result.context_source == "window"


# ---------------------------------------------------------------------------
# resolve_context — Priority 2: window
# ---------------------------------------------------------------------------

class TestResolveContextWindow:
    def test_window_returns_window_source(self):
        window = [
            make_message("w1", "Swiggy is hiring for the role of SDE 2"),
        ]
        current = make_message("msg_005", text="link please", reply_to_preview=None)
        result = resolve_context(current, window_messages=window)
        assert result.context_source == "window"
        assert result.confidence == CONFIDENCE_WINDOW
        assert result.company == "Swiggy"

    def test_window_multiple_messages_scanned(self):
        window = [
            make_message("w1", "deadline is 30 march apply fast"),
            make_message("w2", "Razorpay is hiring frontend developers"),
        ]
        current = make_message("msg_006", text="salary?", reply_to_preview=None)
        result = resolve_context(current, window_messages=window)
        assert result.context_source == "window"
        assert result.company == "Razorpay"

    def test_window_partial_match_still_returns_window(self):
        # Only company found, role is None — still window source
        window = [make_message("w1", "CRED is hiring freshers 2025")]
        current = make_message("msg_007", text="any slots left?", reply_to_preview=None)
        result = resolve_context(current, window_messages=window)
        assert result.context_source == "window"
        assert result.company == "CRED"
        assert result.role is None


# ---------------------------------------------------------------------------
# resolve_context — Priority 3: none
# ---------------------------------------------------------------------------

class TestResolveContextNone:
    def test_no_reply_no_window_returns_none_source(self):
        current = make_message("msg_008", text="deadline 25 march 20 lpa", reply_to_preview=None)
        result = resolve_context(current, window_messages=[])
        assert result.context_source == "none"
        assert result.confidence == CONFIDENCE_NONE
        assert result.company is None
        assert result.role is None

    def test_empty_window_no_match_returns_none(self):
        window = [
            make_message("w1", "apply before deadline please"),
            make_message("w2", "link is https://forms.google.com/abc"),
        ]
        current = make_message("msg_009", text="20 lpa ctc 25 march", reply_to_preview=None)
        result = resolve_context(current, window_messages=window)
        assert result.context_source == "none"
        assert result.confidence == CONFIDENCE_NONE


# ---------------------------------------------------------------------------
# ContextResolvedFields dataclass shape
# ---------------------------------------------------------------------------

class TestContextResolvedFieldsShape:
    def test_has_required_fields(self):
        result = ContextResolvedFields(
            company="Google",
            role="SDE",
            context_source="reply",
            confidence=0.95,
        )
        assert result.company == "Google"
        assert result.role == "SDE"
        assert result.context_source == "reply"
        assert result.confidence == 0.95

    def test_none_fields_allowed(self):
        result = ContextResolvedFields(
            company=None,
            role=None,
            context_source="none",
            confidence=0.30,
        )
        assert result.company is None
        assert result.role is None


