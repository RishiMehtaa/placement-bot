"""
Unit tests for Stage 8 — Deduplication System.
Tests all three layers and the DeduplicationResult dataclass.
"""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from extraction.deduplicator import (
    DeduplicationResult,
    compute_content_hash,
    normalize_url,
    normalize_urls,
    run_deduplication,
)


# ===========================================================================
# compute_content_hash
# ===========================================================================


class TestComputeContentHash:
    def test_basic_hash_is_string(self):
        result = compute_content_hash("Hello World")
        assert isinstance(result, str)
        assert len(result) == 64  # SHA-256 hex digest

    def test_same_text_same_hash(self):
        assert compute_content_hash("Hello") == compute_content_hash("Hello")

    def test_different_text_different_hash(self):
        assert compute_content_hash("Hello") != compute_content_hash("World")

    def test_case_insensitive(self):
        assert compute_content_hash("Hello World") == compute_content_hash("HELLO WORLD")

    def test_whitespace_normalized(self):
        assert compute_content_hash("Hello   World") == compute_content_hash("Hello World")

    def test_leading_trailing_whitespace_stripped(self):
        assert compute_content_hash("  Hello  ") == compute_content_hash("Hello")

    def test_tabs_collapsed(self):
        assert compute_content_hash("Hello\tWorld") == compute_content_hash("Hello World")

    def test_newlines_collapsed(self):
        assert compute_content_hash("Hello\nWorld") == compute_content_hash("Hello World")

    def test_empty_string(self):
        result = compute_content_hash("")
        assert isinstance(result, str)
        assert len(result) == 64

    def test_unicode_text(self):
        result = compute_content_hash("Tata Consultancy Services hiring ₹20 LPA")
        assert isinstance(result, str)
        assert len(result) == 64


# ===========================================================================
# normalize_url
# ===========================================================================


class TestNormalizeUrl:
    def test_basic_url_unchanged(self):
        result = normalize_url("https://example.com/jobs")
        assert result == "https://example.com/jobs"

    def test_http_upgraded_to_https(self):
        result = normalize_url("http://example.com/jobs")
        assert result == "https://example.com/jobs"

    def test_trailing_slash_removed(self):
        result = normalize_url("https://example.com/jobs/")
        assert result == "https://example.com/jobs"

    def test_root_slash_preserved(self):
        result = normalize_url("https://example.com/")
        assert "example.com" in result

    def test_utm_source_stripped(self):
        result = normalize_url("https://example.com/jobs?utm_source=whatsapp")
        assert "utm_source" not in result

    def test_utm_medium_stripped(self):
        result = normalize_url("https://example.com/jobs?utm_medium=social")
        assert "utm_medium" not in result

    def test_utm_campaign_stripped(self):
        result = normalize_url("https://example.com/jobs?utm_campaign=placement")
        assert "utm_campaign" not in result

    def test_utm_content_stripped(self):
        result = normalize_url("https://example.com/jobs?utm_content=link")
        assert "utm_content" not in result

    def test_utm_term_stripped(self):
        result = normalize_url("https://example.com/jobs?utm_term=intern")
        assert "utm_term" not in result

    def test_non_utm_params_preserved(self):
        result = normalize_url("https://example.com/jobs?ref=123&role=swe")
        assert "ref=123" in result
        assert "role=swe" in result

    def test_utm_and_non_utm_mixed(self):
        result = normalize_url(
            "https://example.com/apply?ref=campus&utm_source=whatsapp&utm_medium=chat"
        )
        assert "ref=campus" in result
        assert "utm_source" not in result
        assert "utm_medium" not in result

    def test_host_lowercased(self):
        result = normalize_url("https://EXAMPLE.COM/jobs")
        assert "example.com" in result

    def test_scheme_lowercased(self):
        result = normalize_url("HTTPS://example.com/jobs")
        assert result.startswith("https://")

    def test_params_sorted_for_stability(self):
        url_a = normalize_url("https://example.com/jobs?b=2&a=1")
        url_b = normalize_url("https://example.com/jobs?a=1&b=2")
        assert url_a == url_b

    def test_missing_scheme_raises_value_error(self):
        with pytest.raises(ValueError):
            normalize_url("example.com/jobs")

    def test_empty_string_raises_value_error(self):
        with pytest.raises(ValueError):
            normalize_url("")

    def test_none_raises_value_error(self):
        with pytest.raises(ValueError):
            normalize_url(None)

    def test_same_url_different_utm_same_result(self):
        url_a = normalize_url("https://example.com/apply?utm_source=wa&utm_medium=chat")
        url_b = normalize_url("https://example.com/apply?utm_source=email")
        assert url_a == url_b

    def test_http_and_https_same_path_same_result(self):
        url_a = normalize_url("http://example.com/apply")
        url_b = normalize_url("https://example.com/apply")
        assert url_a == url_b


# ===========================================================================
# normalize_urls
# ===========================================================================


class TestNormalizeUrls:
    def test_empty_list(self):
        assert normalize_urls([]) == []

    def test_single_valid_url(self):
        result = normalize_urls(["https://example.com/jobs"])
        assert len(result) == 1

    def test_invalid_url_silently_skipped(self):
        result = normalize_urls(["not-a-url", "https://example.com/jobs"])
        assert len(result) == 1
        assert "example.com" in result[0]

    def test_duplicates_removed(self):
        result = normalize_urls([
            "https://example.com/jobs",
            "https://example.com/jobs/",
        ])
        assert len(result) == 1

    def test_utm_variants_deduplicated(self):
        result = normalize_urls([
            "https://example.com/apply?utm_source=wa",
            "https://example.com/apply?utm_source=email",
        ])
        assert len(result) == 1

    def test_order_preserved_for_valid_urls(self):
        result = normalize_urls([
            "https://alpha.com/jobs",
            "https://beta.com/jobs",
        ])
        assert "alpha.com" in result[0]
        assert "beta.com" in result[1]

    def test_all_invalid_returns_empty(self):
        result = normalize_urls(["not-a-url", "also-bad", "ftp-no-host"])
        assert result == []


# ===========================================================================
# run_deduplication — Layer 1
# ===========================================================================


class TestRunDeduplicationLayer1:
    @pytest.mark.asyncio
    async def test_layer1_always_ok_when_message_reaches_pipeline(self):
        db = AsyncMock()
        with patch("extraction.deduplicator.content_hash_exists", return_value=False), \
             patch("extraction.deduplicator.get_family_by_jd_link", return_value=None):
            result = await run_deduplication(
                message_id="msg_001",
                raw_text="Hiring at TCS apply now",
                urls=[],
                db=db,
            )
        assert result.layer1_message_id_ok is True
        assert "layer1" in result.layers_fired

    @pytest.mark.asyncio
    async def test_layer1_fired_before_layer2(self):
        db = AsyncMock()
        with patch("extraction.deduplicator.content_hash_exists", return_value=False), \
             patch("extraction.deduplicator.get_family_by_jd_link", return_value=None):
            result = await run_deduplication(
                message_id="msg_001",
                raw_text="Some text",
                urls=[],
                db=db,
            )
        assert result.layers_fired.index("layer1") < result.layers_fired.index("layer2")


# ===========================================================================
# run_deduplication — Layer 2
# ===========================================================================


class TestRunDeduplicationLayer2:
    @pytest.mark.asyncio
    async def test_layer2_passes_when_hash_is_new(self):
        db = AsyncMock()
        with patch("extraction.deduplicator.content_hash_exists", return_value=False), \
             patch("extraction.deduplicator.get_family_by_jd_link", return_value=None):
            result = await run_deduplication(
                message_id="msg_001",
                raw_text="TCS hiring software engineers",
                urls=[],
                db=db,
            )
        assert result.layer2_is_duplicate is False
        assert result.should_skip is False

    @pytest.mark.asyncio
    async def test_layer2_skips_when_hash_exists(self):
        db = AsyncMock()
        with patch("extraction.deduplicator.content_hash_exists", return_value=True):
            result = await run_deduplication(
                message_id="msg_002",
                raw_text="TCS hiring software engineers",
                urls=[],
                db=db,
            )
        assert result.layer2_is_duplicate is True
        assert result.should_skip is True
        assert result.skip_reason is not None
        assert "content_hash" in result.skip_reason

    @pytest.mark.asyncio
    async def test_layer2_sets_content_hash(self):
        db = AsyncMock()
        text = "Wipro is hiring data engineers"
        expected_hash = compute_content_hash(text)
        with patch("extraction.deduplicator.content_hash_exists", return_value=False), \
             patch("extraction.deduplicator.get_family_by_jd_link", return_value=None):
            result = await run_deduplication(
                message_id="msg_003",
                raw_text=text,
                urls=[],
                db=db,
            )
        assert result.layer2_content_hash == expected_hash

    @pytest.mark.asyncio
    async def test_layer2_duplicate_returns_early_skips_layer3(self):
        db = AsyncMock()
        with patch("extraction.deduplicator.content_hash_exists", return_value=True):
            result = await run_deduplication(
                message_id="msg_004",
                raw_text="duplicate text",
                urls=["https://example.com/apply"],
                db=db,
            )
        assert "layer3" not in result.layers_fired

    @pytest.mark.asyncio
    async def test_layer2_fired_in_layers_fired(self):
        db = AsyncMock()
        with patch("extraction.deduplicator.content_hash_exists", return_value=False), \
             patch("extraction.deduplicator.get_family_by_jd_link", return_value=None):
            result = await run_deduplication(
                message_id="msg_005",
                raw_text="New unique text",
                urls=[],
                db=db,
            )
        assert "layer2" in result.layers_fired


# ===========================================================================
# run_deduplication — Layer 3
# ===========================================================================


class TestRunDeduplicationLayer3:
    @pytest.mark.asyncio
    async def test_layer3_no_urls_no_match(self):
        db = AsyncMock()
        with patch("extraction.deduplicator.content_hash_exists", return_value=False), \
             patch("extraction.deduplicator.get_family_by_jd_link", return_value=None):
            result = await run_deduplication(
                message_id="msg_006",
                raw_text="Apply now for the role",
                urls=[],
                db=db,
            )
        assert result.layer3_duplicate_family_id is None
        assert result.should_skip is False

    @pytest.mark.asyncio
    async def test_layer3_url_matches_existing_family(self):
        db = AsyncMock()
        mock_family = MagicMock()
        mock_family.id = "family-uuid-123"
        with patch("extraction.deduplicator.content_hash_exists", return_value=False), \
             patch("extraction.deduplicator.get_family_by_jd_link", return_value=mock_family):
            result = await run_deduplication(
                message_id="msg_007",
                raw_text="Apply at https://example.com/apply",
                urls=["https://example.com/apply"],
                db=db,
            )
        assert result.layer3_duplicate_family_id == "family-uuid-123"
        assert result.should_skip is False  # Layer 3 does NOT skip, just informs

    @pytest.mark.asyncio
    async def test_layer3_url_no_existing_family(self):
        db = AsyncMock()
        with patch("extraction.deduplicator.content_hash_exists", return_value=False), \
             patch("extraction.deduplicator.get_family_by_jd_link", return_value=None):
            result = await run_deduplication(
                message_id="msg_008",
                raw_text="Apply at https://newcompany.com/apply",
                urls=["https://newcompany.com/apply"],
                db=db,
            )
        assert result.layer3_duplicate_family_id is None

    @pytest.mark.asyncio
    async def test_layer3_normalizes_urls_before_lookup(self):
        db = AsyncMock()
        with patch("extraction.deduplicator.content_hash_exists", return_value=False), \
             patch("extraction.deduplicator.get_family_by_jd_link", return_value=None) as mock_lookup:
            result = await run_deduplication(
                message_id="msg_009",
                raw_text="Apply here",
                urls=["https://example.com/apply?utm_source=wa&utm_medium=chat"],
                db=db,
            )
        # Verify the URL passed to DB lookup is normalized (no UTM params)
        call_args = mock_lookup.call_args[0][1]
        assert "utm_source" not in call_args
        assert "utm_medium" not in call_args

    @pytest.mark.asyncio
    async def test_layer3_invalid_url_silently_skipped(self):
        db = AsyncMock()
        with patch("extraction.deduplicator.content_hash_exists", return_value=False), \
             patch("extraction.deduplicator.get_family_by_jd_link", return_value=None):
            result = await run_deduplication(
                message_id="msg_010",
                raw_text="Apply here",
                urls=["not-a-valid-url"],
                db=db,
            )
        assert result.layer3_duplicate_family_id is None
        assert result.should_skip is False

    @pytest.mark.asyncio
    async def test_layer3_fired_in_layers_fired(self):
        db = AsyncMock()
        with patch("extraction.deduplicator.content_hash_exists", return_value=False), \
             patch("extraction.deduplicator.get_family_by_jd_link", return_value=None):
            result = await run_deduplication(
                message_id="msg_011",
                raw_text="Some message",
                urls=[],
                db=db,
            )
        assert "layer3" in result.layers_fired

    @pytest.mark.asyncio
    async def test_layer3_stores_normalized_urls_in_result(self):
        db = AsyncMock()
        with patch("extraction.deduplicator.content_hash_exists", return_value=False), \
             patch("extraction.deduplicator.get_family_by_jd_link", return_value=None):
            result = await run_deduplication(
                message_id="msg_012",
                raw_text="Apply now",
                urls=["http://example.com/apply/?utm_source=wa"],
                db=db,
            )
        assert len(result.layer3_urls_normalized) == 1
        assert "utm_source" not in result.layer3_urls_normalized[0]
        assert result.layer3_urls_normalized[0].startswith("https://")


# ===========================================================================
# run_deduplication — DeduplicationResult dataclass
# ===========================================================================


class TestDeduplicationResult:
    def test_default_values(self):
        result = DeduplicationResult(message_id="msg_000")
        assert result.message_id == "msg_000"
        assert result.layer1_message_id_ok is True
        assert result.layer2_content_hash is None
        assert result.layer2_is_duplicate is False
        assert result.layer3_urls_normalized == []
        assert result.layer3_duplicate_family_id is None
        assert result.should_skip is False
        assert result.skip_reason is None
        assert result.layers_fired == []

    def test_should_skip_false_by_default(self):
        result = DeduplicationResult(message_id="msg_001")
        assert result.should_skip is False

    def test_layers_fired_is_list(self):
        result = DeduplicationResult(message_id="msg_002")
        assert isinstance(result.layers_fired, list)

    def test_layer3_urls_normalized_is_list(self):
        result = DeduplicationResult(message_id="msg_003")
        assert isinstance(result.layer3_urls_normalized, list)


# ===========================================================================
# Idempotency
# ===========================================================================


class TestIdempotency:
    @pytest.mark.asyncio
    async def test_running_twice_same_result(self):
        """Running the same message twice must produce the same DeduplicationResult."""
        db = AsyncMock()
        kwargs = dict(
            message_id="msg_idem",
            raw_text="TCS hiring batch 2025",
            urls=["https://tcs.com/apply"],
            db=db,
        )
        with patch("extraction.deduplicator.content_hash_exists", return_value=False), \
             patch("extraction.deduplicator.get_family_by_jd_link", return_value=None):
            result_1 = await run_deduplication(**kwargs)

        with patch("extraction.deduplicator.content_hash_exists", return_value=False), \
             patch("extraction.deduplicator.get_family_by_jd_link", return_value=None):
            result_2 = await run_deduplication(**kwargs)

        assert result_1.layer2_content_hash == result_2.layer2_content_hash
        assert result_1.should_skip == result_2.should_skip
        assert result_1.layers_fired == result_2.layers_fired

    @pytest.mark.asyncio
    async def test_duplicate_hash_second_run_skips(self):
        """Second run with same text but hash_exists=True must skip."""
        db = AsyncMock()
        with patch("extraction.deduplicator.content_hash_exists", return_value=True):
            result = await run_deduplication(
                message_id="msg_second",
                raw_text="same text as before",
                urls=[],
                db=db,
            )
        assert result.should_skip is True
        assert result.layer2_is_duplicate is True