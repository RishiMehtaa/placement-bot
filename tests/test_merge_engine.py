"""
Unit tests for Stage 7 — Merge Engine (extraction/merge_engine.py)
Uses SQLite in-memory DB via aiosqlite.
"""

import pytest
import uuid
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4
from extraction.merge_engine import merge_into_family, MergeResult, _is_valid_url, _ensure_utc
from extraction.normalizer import NormalizedRecord
from extraction.family_resolver import FamilyResolutionResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# def make_record(**kwargs) -> NormalizedRecord:
#     defaults = dict(
#         company=None,
#         role=None,
#         deadline=None,
#         package=None,
#         jd_link=None,
#         notes=[],
#         confidence=0.5,
#         source_message_id="msg_test_001",
#     )
#     defaults.update(kwargs)
#     return NormalizedRecord(**defaults)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_record(
    message_id: str = None,
    company: str = None,
    role: str = None,
    deadline=None,
    package: str = None,
    jd_link: str = None,
    notes: list[str] | None = None,
    confidence: float = 0.75,
    is_processable: bool = True,
) -> NormalizedRecord:
    return NormalizedRecord(
        message_id=message_id or f"msg-{uuid4().hex[:8]}",
        company=company,
        role=role,
        deadline=deadline,
        deadline_raw=None,
        package=package,
        package_raw=None,
        jd_link=jd_link,
        notes=notes or [],
        confidence=confidence,
        company_source="test",
        role_source="test",
        is_processable=is_processable,
    )



def make_resolution(is_new=False, family_id=None) -> FamilyResolutionResult:
    return FamilyResolutionResult(
        family_id=family_id or str(uuid.uuid4()),
        company="Test Corp",
        role="SDE",
        is_new_family=is_new,
        contribution_role="anchor",
        matched_on="company_and_role",
    )


def make_family(**kwargs):
    """Create a mock family object."""
    fam = MagicMock()
    fam.company = kwargs.get("company", "Test Corp")
    fam.role = kwargs.get("role", "SDE")
    fam.deadline = kwargs.get("deadline", None)
    fam.package = kwargs.get("package", None)
    fam.jd_link = kwargs.get("jd_link", None)
    fam.notes = kwargs.get("notes", [])
    fam.confidence = kwargs.get("confidence", 0.5)
    return fam


# ---------------------------------------------------------------------------
# _is_valid_url
# ---------------------------------------------------------------------------

def test_valid_url_https():
    assert _is_valid_url("https://example.com/apply") is True

def test_valid_url_http():
    assert _is_valid_url("http://example.com") is True

def test_invalid_url_none():
    assert _is_valid_url(None) is False

def test_invalid_url_empty():
    assert _is_valid_url("") is False

def test_invalid_url_no_scheme():
    assert _is_valid_url("example.com/apply") is False

def test_invalid_url_ftp():
    assert _is_valid_url("ftp://example.com") is False


# ---------------------------------------------------------------------------
# _ensure_utc
# ---------------------------------------------------------------------------

def test_ensure_utc_naive_datetime():
    naive = datetime(2025, 3, 25, 10, 0, 0)
    result = _ensure_utc(naive)
    assert result.tzinfo == timezone.utc

def test_ensure_utc_aware_datetime():
    aware = datetime(2025, 3, 25, 10, 0, 0, tzinfo=timezone.utc)
    result = _ensure_utc(aware)
    assert result.tzinfo == timezone.utc

def test_ensure_utc_none():
    assert _ensure_utc(None) is None


# ---------------------------------------------------------------------------
# New family — skip merge
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_new_family_skips_merge():
    resolution = make_resolution(is_new=True)
    record = make_record(company="Infosys", role="SDE")
    db = AsyncMock()

    result = await merge_into_family(record, resolution, db)

    assert result.was_merged is False
    assert result.updated_fields == []


# ---------------------------------------------------------------------------
# Family not found in DB
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_family_not_found_skips_merge():
    resolution = make_resolution(is_new=False)
    record = make_record(company="Infosys")
    db = AsyncMock()

    with patch("extraction.merge_engine.get_family_by_id", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = None
        result = await merge_into_family(record, resolution, db)

    assert result.was_merged is False


# ---------------------------------------------------------------------------
# Company merge rules
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_company_not_overwritten_if_exists():
    resolution = make_resolution(is_new=False)
    record = make_record(company="Infosys")
    family = make_family(company="TCS")
    db = AsyncMock()

    with patch("extraction.merge_engine.get_family_by_id", new_callable=AsyncMock) as mock_get, \
         patch("extraction.merge_engine.update_family", new_callable=AsyncMock) as mock_update:
        mock_get.return_value = family
        result = await merge_into_family(record, resolution, db)

    assert "company" not in result.updated_fields
    assert "company" in result.skipped_fields


@pytest.mark.asyncio
async def test_company_filled_if_null():
    resolution = make_resolution(is_new=False)
    record = make_record(company="Infosys")
    family = make_family(company=None)
    db = AsyncMock()

    with patch("extraction.merge_engine.get_family_by_id", new_callable=AsyncMock) as mock_get, \
         patch("extraction.merge_engine.update_family", new_callable=AsyncMock) as mock_update:
        mock_get.return_value = family
        result = await merge_into_family(record, resolution, db)
        call_args = mock_update.call_args[0][2]

    assert "company" in result.updated_fields
    assert call_args["company"] == "Infosys"


@pytest.mark.asyncio
async def test_company_null_in_record_not_written():
    resolution = make_resolution(is_new=False)
    record = make_record(company=None)
    family = make_family(company=None)
    db = AsyncMock()

    with patch("extraction.merge_engine.get_family_by_id", new_callable=AsyncMock) as mock_get, \
         patch("extraction.merge_engine.update_family", new_callable=AsyncMock) as mock_update:
        mock_get.return_value = family
        result = await merge_into_family(record, resolution, db)

    assert "company" not in result.updated_fields


# ---------------------------------------------------------------------------
# Role merge rules
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_role_not_overwritten_if_exists():
    resolution = make_resolution(is_new=False)
    record = make_record(role="Data Engineer")
    family = make_family(role="SDE")
    db = AsyncMock()

    with patch("extraction.merge_engine.get_family_by_id", new_callable=AsyncMock) as mock_get, \
         patch("extraction.merge_engine.update_family", new_callable=AsyncMock) as mock_update:
        mock_get.return_value = family
        result = await merge_into_family(record, resolution, db)

    assert "role" not in result.updated_fields
    assert "role" in result.skipped_fields


@pytest.mark.asyncio
async def test_role_filled_if_null():
    resolution = make_resolution(is_new=False)
    record = make_record(role="Data Engineer")
    family = make_family(role=None)
    db = AsyncMock()

    with patch("extraction.merge_engine.get_family_by_id", new_callable=AsyncMock) as mock_get, \
         patch("extraction.merge_engine.update_family", new_callable=AsyncMock) as mock_update:
        mock_get.return_value = family
        result = await merge_into_family(record, resolution, db)
        call_args = mock_update.call_args[0][2]

    assert "role" in result.updated_fields
    assert call_args["role"] == "Data Engineer"


# ---------------------------------------------------------------------------
# Deadline merge rules
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_deadline_updated_if_later():
    resolution = make_resolution(is_new=False)
    existing_dl = datetime(2025, 3, 25, tzinfo=timezone.utc)
    new_dl = datetime(2025, 3, 30, tzinfo=timezone.utc)
    record = make_record(deadline=new_dl)
    family = make_family(deadline=existing_dl)
    db = AsyncMock()

    with patch("extraction.merge_engine.get_family_by_id", new_callable=AsyncMock) as mock_get, \
         patch("extraction.merge_engine.update_family", new_callable=AsyncMock) as mock_update:
        mock_get.return_value = family
        result = await merge_into_family(record, resolution, db)

    assert "deadline" in result.updated_fields


@pytest.mark.asyncio
async def test_deadline_not_updated_if_earlier():
    resolution = make_resolution(is_new=False)
    existing_dl = datetime(2025, 3, 30, tzinfo=timezone.utc)
    new_dl = datetime(2025, 3, 25, tzinfo=timezone.utc)
    record = make_record(deadline=new_dl)
    family = make_family(deadline=existing_dl)
    db = AsyncMock()

    with patch("extraction.merge_engine.get_family_by_id", new_callable=AsyncMock) as mock_get, \
         patch("extraction.merge_engine.update_family", new_callable=AsyncMock) as mock_update:
        mock_get.return_value = family
        result = await merge_into_family(record, resolution, db)

    assert "deadline" not in result.updated_fields
    assert "deadline" in result.skipped_fields


@pytest.mark.asyncio
async def test_deadline_set_if_family_has_none():
    resolution = make_resolution(is_new=False)
    new_dl = datetime(2025, 3, 30, tzinfo=timezone.utc)
    record = make_record(deadline=new_dl)
    family = make_family(deadline=None)
    db = AsyncMock()

    with patch("extraction.merge_engine.get_family_by_id", new_callable=AsyncMock) as mock_get, \
         patch("extraction.merge_engine.update_family", new_callable=AsyncMock) as mock_update:
        mock_get.return_value = family
        result = await merge_into_family(record, resolution, db)

    assert "deadline" in result.updated_fields


@pytest.mark.asyncio
async def test_deadline_naive_datetime_handled():
    """Naive datetimes must be handled without raising an exception."""
    resolution = make_resolution(is_new=False)
    naive_dl = datetime(2025, 3, 30)  # no tzinfo
    existing_dl = datetime(2025, 3, 25, tzinfo=timezone.utc)
    record = make_record(deadline=naive_dl)
    family = make_family(deadline=existing_dl)
    db = AsyncMock()

    with patch("extraction.merge_engine.get_family_by_id", new_callable=AsyncMock) as mock_get, \
         patch("extraction.merge_engine.update_family", new_callable=AsyncMock) as mock_update:
        mock_get.return_value = family
        result = await merge_into_family(record, resolution, db)

    assert "deadline" in result.updated_fields  # 30 > 25


# ---------------------------------------------------------------------------
# Package merge rules
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_package_updated_if_non_null():
    resolution = make_resolution(is_new=False)
    record = make_record(package="20 LPA")
    family = make_family(package="15 LPA")
    db = AsyncMock()

    with patch("extraction.merge_engine.get_family_by_id", new_callable=AsyncMock) as mock_get, \
         patch("extraction.merge_engine.update_family", new_callable=AsyncMock) as mock_update:
        mock_get.return_value = family
        result = await merge_into_family(record, resolution, db)
        call_args = mock_update.call_args[0][2]

    assert "package" in result.updated_fields
    assert call_args["package"] == "20 LPA"


@pytest.mark.asyncio
async def test_package_not_updated_if_null():
    resolution = make_resolution(is_new=False)
    record = make_record(package=None)
    family = make_family(package="15 LPA")
    db = AsyncMock()

    with patch("extraction.merge_engine.get_family_by_id", new_callable=AsyncMock) as mock_get, \
         patch("extraction.merge_engine.update_family", new_callable=AsyncMock) as mock_update:
        mock_get.return_value = family
        result = await merge_into_family(record, resolution, db)

    assert "package" not in result.updated_fields


# ---------------------------------------------------------------------------
# jd_link merge rules
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_jd_link_set_if_family_null_and_valid():
    resolution = make_resolution(is_new=False)
    record = make_record(jd_link="https://apply.example.com/job/123")
    family = make_family(jd_link=None)
    db = AsyncMock()

    with patch("extraction.merge_engine.get_family_by_id", new_callable=AsyncMock) as mock_get, \
         patch("extraction.merge_engine.update_family", new_callable=AsyncMock) as mock_update:
        mock_get.return_value = family
        result = await merge_into_family(record, resolution, db)
        call_args = mock_update.call_args[0][2]

    assert "jd_link" in result.updated_fields
    assert call_args["jd_link"] == "https://apply.example.com/job/123"


@pytest.mark.asyncio
async def test_jd_link_not_overwritten_if_exists():
    resolution = make_resolution(is_new=False)
    record = make_record(jd_link="https://new-link.com")
    family = make_family(jd_link="https://existing-link.com")
    db = AsyncMock()

    with patch("extraction.merge_engine.get_family_by_id", new_callable=AsyncMock) as mock_get, \
         patch("extraction.merge_engine.update_family", new_callable=AsyncMock) as mock_update:
        mock_get.return_value = family
        result = await merge_into_family(record, resolution, db)

    assert "jd_link" not in result.updated_fields
    assert "jd_link" in result.skipped_fields


@pytest.mark.asyncio
async def test_jd_link_invalid_url_not_written():
    resolution = make_resolution(is_new=False)
    record = make_record(jd_link="not-a-url")
    family = make_family(jd_link=None)
    db = AsyncMock()

    with patch("extraction.merge_engine.get_family_by_id", new_callable=AsyncMock) as mock_get, \
         patch("extraction.merge_engine.update_family", new_callable=AsyncMock) as mock_update:
        mock_get.return_value = family
        result = await merge_into_family(record, resolution, db)

    assert "jd_link" not in result.updated_fields


@pytest.mark.asyncio
async def test_jd_link_null_in_record_not_written():
    resolution = make_resolution(is_new=False)
    record = make_record(jd_link=None)
    family = make_family(jd_link=None)
    db = AsyncMock()

    with patch("extraction.merge_engine.get_family_by_id", new_callable=AsyncMock) as mock_get, \
         patch("extraction.merge_engine.update_family", new_callable=AsyncMock) as mock_update:
        mock_get.return_value = family
        result = await merge_into_family(record, resolution, db)

    assert "jd_link" not in result.updated_fields


# ---------------------------------------------------------------------------
# Notes merge rules
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_notes_appended_not_replaced():
    resolution = make_resolution(is_new=False)
    record = make_record(notes=["New note about the role"])
    family = make_family(notes=["Original note"])
    db = AsyncMock()

    with patch("extraction.merge_engine.get_family_by_id", new_callable=AsyncMock) as mock_get, \
         patch("extraction.merge_engine.update_family", new_callable=AsyncMock) as mock_update:
        mock_get.return_value = family
        result = await merge_into_family(record, resolution, db)
        call_args = mock_update.call_args[0][2]

    assert "notes" in result.updated_fields
    assert "Original note" in call_args["notes"]
    assert "New note about the role" in call_args["notes"]
    assert len(call_args["notes"]) == 2


@pytest.mark.asyncio
async def test_notes_duplicate_not_added():
    resolution = make_resolution(is_new=False)
    record = make_record(notes=["Same note"])
    family = make_family(notes=["Same note"])
    db = AsyncMock()

    with patch("extraction.merge_engine.get_family_by_id", new_callable=AsyncMock) as mock_get, \
         patch("extraction.merge_engine.update_family", new_callable=AsyncMock) as mock_update:
        mock_get.return_value = family
        result = await merge_into_family(record, resolution, db)

    assert "notes" not in result.updated_fields
    assert "notes" in result.skipped_fields


@pytest.mark.asyncio
async def test_notes_empty_list_not_written():
    resolution = make_resolution(is_new=False)
    record = make_record(notes=[])
    family = make_family(notes=["Existing note"])
    db = AsyncMock()

    with patch("extraction.merge_engine.get_family_by_id", new_callable=AsyncMock) as mock_get, \
         patch("extraction.merge_engine.update_family", new_callable=AsyncMock) as mock_update:
        mock_get.return_value = family
        result = await merge_into_family(record, resolution, db)

    assert "notes" not in result.updated_fields


@pytest.mark.asyncio
async def test_notes_written_to_empty_family():
    resolution = make_resolution(is_new=False)
    record = make_record(notes=["First note"])
    family = make_family(notes=[])
    db = AsyncMock()

    with patch("extraction.merge_engine.get_family_by_id", new_callable=AsyncMock) as mock_get, \
         patch("extraction.merge_engine.update_family", new_callable=AsyncMock) as mock_update:
        mock_get.return_value = family
        result = await merge_into_family(record, resolution, db)
        call_args = mock_update.call_args[0][2]

    assert "notes" in result.updated_fields
    assert call_args["notes"] == ["First note"]


# ---------------------------------------------------------------------------
# Confidence merge rules
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_confidence_updated_if_higher():
    resolution = make_resolution(is_new=False)
    record = make_record(confidence=0.9)
    family = make_family(confidence=0.6)
    db = AsyncMock()

    with patch("extraction.merge_engine.get_family_by_id", new_callable=AsyncMock) as mock_get, \
         patch("extraction.merge_engine.update_family", new_callable=AsyncMock) as mock_update:
        mock_get.return_value = family
        result = await merge_into_family(record, resolution, db)
        call_args = mock_update.call_args[0][2]

    assert "confidence" in result.updated_fields
    assert call_args["confidence"] == 0.9


@pytest.mark.asyncio
async def test_confidence_not_updated_if_lower():
    resolution = make_resolution(is_new=False)
    record = make_record(confidence=0.4)
    family = make_family(confidence=0.9)
    db = AsyncMock()

    with patch("extraction.merge_engine.get_family_by_id", new_callable=AsyncMock) as mock_get, \
         patch("extraction.merge_engine.update_family", new_callable=AsyncMock) as mock_update:
        mock_get.return_value = family
        result = await merge_into_family(record, resolution, db)

    assert "confidence" not in result.updated_fields
    assert "confidence" in result.skipped_fields


@pytest.mark.asyncio
async def test_confidence_updated_if_family_has_none():
    resolution = make_resolution(is_new=False)
    record = make_record(confidence=0.7)
    family = make_family(confidence=None)
    db = AsyncMock()

    with patch("extraction.merge_engine.get_family_by_id", new_callable=AsyncMock) as mock_get, \
         patch("extraction.merge_engine.update_family", new_callable=AsyncMock) as mock_update:
        mock_get.return_value = family
        result = await merge_into_family(record, resolution, db)

    assert "confidence" in result.updated_fields


# ---------------------------------------------------------------------------
# No updates — update_family not called
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_no_updates_means_update_family_not_called():
    """If nothing changes, update_family should not be called at all."""
    resolution = make_resolution(is_new=False)
    record = make_record(
        company=None,
        role=None,
        deadline=None,
        package=None,
        jd_link=None,
        notes=[],
        confidence=0.3,
    )
    family = make_family(
        company="TCS",
        role="SDE",
        deadline=None,
        package=None,
        jd_link=None,
        notes=[],
        confidence=0.5,
    )
    db = AsyncMock()

    with patch("extraction.merge_engine.get_family_by_id", new_callable=AsyncMock) as mock_get, \
         patch("extraction.merge_engine.update_family", new_callable=AsyncMock) as mock_update:
        mock_get.return_value = family
        result = await merge_into_family(record, resolution, db)

    assert result.was_merged is False
    mock_update.assert_not_called()


# ---------------------------------------------------------------------------
# MergeResult structure
# ---------------------------------------------------------------------------

def test_merge_result_dataclass_defaults():
    r = MergeResult(family_id="abc", was_merged=False)
    assert r.updated_fields == []
    assert r.skipped_fields == []


def test_merge_result_with_fields():
    r = MergeResult(
        family_id="abc",
        was_merged=True,
        updated_fields=["package", "deadline"],
        skipped_fields=["company"],
    )
    assert len(r.updated_fields) == 2
    assert "package" in r.updated_fields
    assert "company" in r.skipped_fields


# ---------------------------------------------------------------------------
# Multiple fields updated in one call
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_multiple_fields_updated_together():
    resolution = make_resolution(is_new=False)
    new_dl = datetime(2025, 4, 15, tzinfo=timezone.utc)
    record = make_record(
        package="25 LPA",
        deadline=new_dl,
        jd_link="https://apply.tcs.com/job/456",
        confidence=0.95,
    )
    family = make_family(
        package=None,
        deadline=None,
        jd_link=None,
        confidence=0.5,
    )
    db = AsyncMock()

    with patch("extraction.merge_engine.get_family_by_id", new_callable=AsyncMock) as mock_get, \
         patch("extraction.merge_engine.update_family", new_callable=AsyncMock) as mock_update:
        mock_get.return_value = family
        result = await merge_into_family(record, resolution, db)
        call_args = mock_update.call_args[0][2]

    assert result.was_merged is True
    assert "package" in result.updated_fields
    assert "deadline" in result.updated_fields
    assert "jd_link" in result.updated_fields
    assert "confidence" in result.updated_fields
    assert call_args["package"] == "25 LPA"
    assert call_args["jd_link"] == "https://apply.tcs.com/job/456"