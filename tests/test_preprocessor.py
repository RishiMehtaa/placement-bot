"""
Unit tests for extraction/preprocessor.py — Phase 6

Run inside the fastapi container:
  docker compose exec fastapi python -m pytest tests/test_preprocessor.py -v
"""

import pytest
from extraction.preprocessor import (
    preprocess,
    extract_urls,
    remove_emoji,
    normalize_whitespace,
    find_matched_keywords,
    PreprocessedMessage,
)


# ---------------------------------------------------------------------------
# extract_urls
# ---------------------------------------------------------------------------

class TestExtractUrls:

    def test_extracts_https_url(self):
        text = "Apply here: https://careers.google.com/jobs/123"
        urls = extract_urls(text)
        assert urls == ["https://careers.google.com/jobs/123"]

    def test_extracts_http_url(self):
        text = "Form: http://forms.gle/abc123"
        urls = extract_urls(text)
        assert urls == ["http://forms.gle/abc123"]

    def test_extracts_www_url(self):
        text = "Visit www.example.com for details"
        urls = extract_urls(text)
        assert urls == ["www.example.com"]

    def test_extracts_multiple_urls(self):
        text = "Apply: https://apply.com and JD: https://jd.com/role"
        urls = extract_urls(text)
        assert len(urls) == 2
        assert "https://apply.com" in urls
        assert "https://jd.com/role" in urls

    def test_deduplicates_urls(self):
        text = "https://apply.com and again https://apply.com"
        urls = extract_urls(text)
        assert urls == ["https://apply.com"]

    def test_no_urls_returns_empty(self):
        text = "Hiring for SDE role, deadline 25 March"
        urls = extract_urls(text)
        assert urls == []

    def test_strips_trailing_punctuation(self):
        text = "Apply at https://apply.com."
        urls = extract_urls(text)
        assert urls == ["https://apply.com"]

    def test_url_with_query_params(self):
        text = "Form: https://forms.gle/abc?ref=whatsapp&source=group"
        urls = extract_urls(text)
        assert len(urls) == 1
        assert "https://forms.gle/abc?ref=whatsapp&source=group" in urls[0]


# ---------------------------------------------------------------------------
# remove_emoji
# ---------------------------------------------------------------------------

class TestRemoveEmoji:

    def test_removes_common_emoji(self):
        text = "Apply now 🚀 Deadline tomorrow 📅"
        result = remove_emoji(text)
        assert "🚀" not in result
        assert "📅" not in result

    def test_preserves_text_around_emoji(self):
        text = "Hello 👋 World"
        result = remove_emoji(text)
        assert "Hello" in result
        assert "World" in result

    def test_no_emoji_unchanged(self):
        text = "Hiring for SDE role at Google"
        result = remove_emoji(text)
        assert result == text

    def test_flag_emoji_removed(self):
        text = "India 🇮🇳 placement drive"
        result = remove_emoji(text)
        assert "🇮🇳" not in result
        assert "placement drive" in result


# ---------------------------------------------------------------------------
# normalize_whitespace
# ---------------------------------------------------------------------------

class TestNormalizeWhitespace:

    def test_collapses_multiple_spaces(self):
        text = "Apply   now   for   this   role"
        result = normalize_whitespace(text)
        assert result == "Apply now for this role"

    def test_replaces_tab_with_space(self):
        text = "Company:\tGoogle\tRole:\tSDE"
        result = normalize_whitespace(text)
        assert "\t" not in result
        assert "Company: Google Role: SDE" == result

    def test_strips_leading_trailing(self):
        text = "  hiring now  "
        result = normalize_whitespace(text)
        assert result == "hiring now"

    def test_mixed_whitespace(self):
        text = "  Apply\t\t  here   now  "
        result = normalize_whitespace(text)
        assert result == "Apply here now"


# ---------------------------------------------------------------------------
# find_matched_keywords
# ---------------------------------------------------------------------------

class TestFindMatchedKeywords:

    def test_finds_single_keyword(self):
        text = "hiring for sde role"
        matched = find_matched_keywords(text)
        assert "hiring" in matched

    def test_finds_multiple_keywords(self):
        text = "apply before deadline for this placement opportunity"
        matched = find_matched_keywords(text)
        assert "apply" in matched
        assert "deadline" in matched
        assert "placement" in matched
        assert "opportunity" in matched

    def test_no_keywords_returns_empty(self):
        text = "good morning everyone have a nice day"
        matched = find_matched_keywords(text)
        assert matched == []

    def test_case_insensitive(self):
        # find_matched_keywords expects already-lowercased text
        text = "hiring for intern position"
        matched = find_matched_keywords(text)
        assert "hiring" in matched
        assert "intern" in matched

    def test_no_false_positive_substring(self):
        # "reapply" should NOT match "apply" due to word boundary
        text = "please reapply after some time"
        matched = find_matched_keywords(text)
        assert "apply" not in matched

    def test_lpa_keyword(self):
        text = "package offered is 12 lpa"
        matched = find_matched_keywords(text)
        assert "lpa" in matched


# ---------------------------------------------------------------------------
# preprocess — integration tests
# ---------------------------------------------------------------------------

class TestPreprocess:

    def test_full_placement_message(self):
        text = (
            "🚀 Hiring Alert! Google is recruiting for SDE Intern role.\n"
            "Package: 20 LPA\n"
            "Deadline: 25 March 2025\n"
            "Apply here: https://careers.google.com/apply\n"
            "Register now!"
        )
        result = preprocess("msg_001", text)

        assert isinstance(result, PreprocessedMessage)
        assert result.message_id == "msg_001"
        assert result.is_processable is True
        assert "https://careers.google.com/apply" in result.urls
        assert "🚀" not in result.cleaned_text
        assert result.cleaned_text == result.cleaned_text.lower()
        assert "  " not in result.cleaned_text
        assert len(result.matched_keywords) > 0

    def test_non_processable_message(self):
        text = "Good morning everyone! Hope you all have a great day ahead."
        result = preprocess("msg_002", text)

        assert result.is_processable is False
        assert result.urls == []
        assert result.matched_keywords == []

    def test_url_alone_makes_processable(self):
        text = "Check this out: https://somelink.com"
        result = preprocess("msg_003", text)

        assert result.is_processable is True
        assert "https://somelink.com" in result.urls

    def test_keyword_alone_makes_processable(self):
        text = "deadline is tomorrow for the drive"
        result = preprocess("msg_004", text)

        assert result.is_processable is True
        assert "deadline" in result.matched_keywords
        assert "drive" in result.matched_keywords

    def test_urls_extracted_before_cleaning(self):
        # URL must survive emoji removal and cleaning
        text = "🔥 Apply: https://apply.company.com/role?id=123 🔥"
        result = preprocess("msg_005", text)

        assert "https://apply.company.com/role?id=123" in result.urls
        assert "🔥" not in result.cleaned_text

    def test_cleaned_text_is_lowercase(self):
        text = "HIRING NOW at Google for SDE Role. Apply by 25 MARCH."
        result = preprocess("msg_006", text)

        assert result.cleaned_text == result.cleaned_text.lower()

    def test_original_text_preserved(self):
        text = "🚀 Hiring! Apply now. Deadline: 25 March"
        result = preprocess("msg_007", text)

        assert result.original_text == text  # original must be untouched

    def test_empty_string_not_processable(self):
        result = preprocess("msg_008", "")
        assert result.is_processable is False
        assert result.cleaned_text == ""
        assert result.urls == []

    def test_only_emoji_not_processable(self):
        result = preprocess("msg_009", "🚀🎉👋🔥")
        assert result.is_processable is False
        assert result.cleaned_text == "" or result.cleaned_text.strip() == ""

    def test_whitespace_normalized_in_output(self):
        text = "apply   now   for   this   opening"
        result = preprocess("msg_010", text)
        assert "  " not in result.cleaned_text