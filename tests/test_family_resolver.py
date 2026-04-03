"""
Unit tests for Stage 6 — Family Resolver.
Uses an in-memory SQLite database via SQLAlchemy async.
243 existing tests must continue to pass — zero regressions.

NOTE: SQLite does not support PostgreSQL ARRAY type.
We override the families.notes column to use JSON for the test DB only.
Production db/models.py is unchanged — PostgreSQL ARRAY remains there.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import AsyncGenerator
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import Column, JSON, Text, VARCHAR, Boolean, Integer, Float
from sqlalchemy import String, DateTime
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.pool import StaticPool
from sqlalchemy.dialects.postgresql import JSONB
from db import queries
from extraction.family_resolver import resolve_family, FamilyResolutionResult
from extraction.normalizer import NormalizedRecord


# ---------------------------------------------------------------------------
# SQLite-compatible Base — overrides ARRAY(Text()) → JSON for notes column
# We import only what we need from db.models and rebuild families + related
# tables using SQLite-safe types.
# ---------------------------------------------------------------------------

# Import the real models so all tables except families use the real definitions
from db.models import (
    Base as RealBase,
    Message,
    MessageFamilyMap,
    SheetsSync,
    DeadLetterQueue,
    QueueItem,
)

# We need a separate metadata for SQLite that replaces ARRAY with JSON.
# Simplest approach: monkeypatch the notes column on the real Family model
# before create_all, then restore it after. This is test-only and isolated
# per engine since SQLite schema is dropped after each test.

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ARRAY
from db.models import Family


# def _patch_family_for_sqlite():
#     """Replace ARRAY(Text()) with JSON on the Family.notes column for SQLite."""
#     col = Family.__table__.c.notes
#     col.type = sa.JSON()


# def _unpatch_family_for_postgres():
#     """Restore ARRAY(Text()) on the Family.notes column after SQLite tests."""
#     col = Family.__table__.c.notes
#     col.type = ARRAY(Text())

def _patch_family_for_sqlite():
    """Replace PostgreSQL-only types with SQLite-compatible equivalents."""
    from db.models import Family, DeadLetterQueue
    Family.__table__.c.notes.type = sa.JSON()
    DeadLetterQueue.__table__.c.raw_payload.type = sa.JSON()


def _unpatch_family_for_postgres():
    """Restore PostgreSQL-only types after SQLite tests."""
    from db.models import Family, DeadLetterQueue
    Family.__table__.c.notes.type = ARRAY(Text())
    DeadLetterQueue.__table__.c.raw_payload.type = JSONB()


# ---------------------------------------------------------------------------
# In-memory SQLite engine for tests — no Docker required
# ---------------------------------------------------------------------------

TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"


@pytest_asyncio.fixture(scope="function")
async def db() -> AsyncGenerator[AsyncSession, None]:
    _patch_family_for_sqlite()

    engine = create_async_engine(
        TEST_DATABASE_URL,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    async with engine.begin() as conn:
        await conn.run_sync(RealBase.metadata.create_all)

    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        yield session

    async with engine.begin() as conn:
        await conn.run_sync(RealBase.metadata.drop_all)
    await engine.dispose()

    _unpatch_family_for_postgres()


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
        notes=[],
        confidence=confidence,
        company_source="test",
        role_source="test",
        is_processable=is_processable,
    )


async def seed_message(db: AsyncSession, message_id: str) -> None:
    """Insert a minimal message row so FK constraints pass."""
    from sqlalchemy import insert
    stmt = insert(Message).values(
        message_id=message_id,
        text="test message",
        timestamp=datetime.now(timezone.utc),
        sender="1234567890@s.whatsapp.net",
        content_hash=f"hash-{message_id}",
        processed=False,
        process_attempts=0,
    )
    await db.execute(stmt)
    await db.commit()


# ---------------------------------------------------------------------------
# Group 1: company + role both present — new family
# ---------------------------------------------------------------------------

class TestCompanyAndRoleNewFamily:

    @pytest.mark.asyncio
    async def test_creates_new_family_when_none_exists(self, db):
        msg_id = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg_id)
        record = make_record(message_id=msg_id, company="Google", role="SDE")
        result = await resolve_family(record, db)

        assert result.is_new_family is True
        assert result.company == "Google"
        assert result.role == "SDE"
        assert result.contribution_role == "anchor"
        assert result.matched_on == "company_and_role"
        assert result.family_id is not None

    @pytest.mark.asyncio
    async def test_new_family_is_persisted(self, db):
        msg_id = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg_id)
        record = make_record(message_id=msg_id, company="Microsoft", role="PM")
        result = await resolve_family(record, db)

        fetched = await queries.get_family_by_id(db, result.family_id)
        assert fetched is not None
        assert fetched.company == "Microsoft"
        assert fetched.role == "PM"

    @pytest.mark.asyncio
    async def test_message_mapped_as_anchor_for_new_family(self, db):
        msg_id = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg_id)
        record = make_record(message_id=msg_id, company="Amazon", role="SDE-2")
        result = await resolve_family(record, db)

        mapping = await queries.get_message_family_mapping(db, msg_id)
        assert mapping is not None
        assert str(mapping.family_id) == str(result.family_id)
        assert mapping.contribution_role == "anchor"

    @pytest.mark.asyncio
    async def test_family_confidence_stored(self, db):
        msg_id = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg_id)
        record = make_record(message_id=msg_id, company="Wipro", role="Analyst", confidence=0.88)
        result = await resolve_family(record, db)

        fetched = await queries.get_family_by_id(db, result.family_id)
        assert abs(fetched.confidence - 0.88) < 0.001

    @pytest.mark.asyncio
    async def test_family_jd_link_stored(self, db):
        msg_id = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg_id)
        record = make_record(
            message_id=msg_id,
            company="Infosys",
            role="Developer",
            jd_link="https://careers.infosys.com/job/123",
        )
        result = await resolve_family(record, db)

        fetched = await queries.get_family_by_id(db, result.family_id)
        assert fetched.jd_link == "https://careers.infosys.com/job/123"


# ---------------------------------------------------------------------------
# Group 2: company + role both present — existing family match
# ---------------------------------------------------------------------------

class TestCompanyAndRoleExistingFamily:

    @pytest.mark.asyncio
    async def test_matches_existing_family(self, db):
        msg1 = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg1)
        record1 = make_record(message_id=msg1, company="TCS", role="Engineer")
        r1 = await resolve_family(record1, db)
        assert r1.is_new_family is True

        msg2 = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg2)
        record2 = make_record(message_id=msg2, company="TCS", role="Engineer")
        r2 = await resolve_family(record2, db)

        assert r2.is_new_family is False
        assert str(r2.family_id) == str(r1.family_id)
        assert r2.contribution_role == "context"
        assert r2.matched_on == "company_and_role"

    @pytest.mark.asyncio
    async def test_match_is_case_insensitive(self, db):
        msg1 = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg1)
        record1 = make_record(message_id=msg1, company="Google", role="SDE")
        r1 = await resolve_family(record1, db)

        msg2 = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg2)
        record2 = make_record(message_id=msg2, company="google", role="sde")
        r2 = await resolve_family(record2, db)

        assert r2.is_new_family is False
        assert str(r2.family_id) == str(r1.family_id)

    @pytest.mark.asyncio
    async def test_second_message_mapped_as_context(self, db):
        msg1 = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg1)
        await resolve_family(make_record(message_id=msg1, company="Meta", role="PM"), db)

        msg2 = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg2)
        await resolve_family(make_record(message_id=msg2, company="Meta", role="PM"), db)

        mapping = await queries.get_message_family_mapping(db, msg2)
        assert mapping.contribution_role == "context"

    @pytest.mark.asyncio
    async def test_different_role_creates_new_family(self, db):
        msg1 = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg1)
        r1 = await resolve_family(make_record(message_id=msg1, company="Apple", role="SDE"), db)

        msg2 = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg2)
        r2 = await resolve_family(make_record(message_id=msg2, company="Apple", role="Designer"), db)

        assert r2.is_new_family is True
        assert str(r2.family_id) != str(r1.family_id)

    @pytest.mark.asyncio
    async def test_different_company_creates_new_family(self, db):
        msg1 = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg1)
        r1 = await resolve_family(make_record(message_id=msg1, company="Google", role="SDE"), db)

        msg2 = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg2)
        r2 = await resolve_family(make_record(message_id=msg2, company="Amazon", role="SDE"), db)

        assert r2.is_new_family is True
        assert str(r2.family_id) != str(r1.family_id)

    @pytest.mark.asyncio
    async def test_three_messages_all_map_to_same_family(self, db):
        family_id = None
        for i in range(3):
            msg = f"msg-{uuid4().hex[:8]}"
            await seed_message(db, msg)
            result = await resolve_family(
                make_record(message_id=msg, company="Zoho", role="SDE"), db
            )
            if family_id is None:
                family_id = result.family_id
            else:
                assert str(result.family_id) == str(family_id)


# ---------------------------------------------------------------------------
# Group 3: company present, role absent
# ---------------------------------------------------------------------------

class TestCompanyOnlyNoRole:

    @pytest.mark.asyncio
    async def test_creates_partial_family_when_none_exists(self, db):
        msg_id = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg_id)
        record = make_record(message_id=msg_id, company="HCL", role=None)
        result = await resolve_family(record, db)

        assert result.is_new_family is True
        assert result.company == "HCL"
        assert result.role is None
        assert result.contribution_role == "anchor"
        assert result.matched_on == "company_only"

    @pytest.mark.asyncio
    async def test_matches_existing_family_by_company_only(self, db):
        msg1 = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg1)
        r1 = await resolve_family(make_record(message_id=msg1, company="HCL", role=None), db)

        msg2 = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg2)
        r2 = await resolve_family(make_record(message_id=msg2, company="HCL", role=None), db)

        assert r2.is_new_family is False
        assert str(r2.family_id) == str(r1.family_id)
        assert r2.matched_on == "company_only"

    @pytest.mark.asyncio
    async def test_company_only_match_is_case_insensitive(self, db):
        msg1 = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg1)
        await resolve_family(make_record(message_id=msg1, company="HCL", role=None), db)

        msg2 = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg2)
        r2 = await resolve_family(make_record(message_id=msg2, company="hcl", role=None), db)

        assert r2.is_new_family is False
        assert r2.matched_on == "company_only"

    @pytest.mark.asyncio
    async def test_company_only_message_mapped_as_context_on_match(self, db):
        msg1 = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg1)
        await resolve_family(make_record(message_id=msg1, company="Accenture", role=None), db)

        msg2 = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg2)
        await resolve_family(make_record(message_id=msg2, company="Accenture", role=None), db)

        mapping = await queries.get_message_family_mapping(db, msg2)
        assert mapping.contribution_role == "context"

    @pytest.mark.asyncio
    async def test_company_only_creates_new_family_for_different_company(self, db):
        msg1 = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg1)
        r1 = await resolve_family(make_record(message_id=msg1, company="TCS", role=None), db)

        msg2 = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg2)
        r2 = await resolve_family(make_record(message_id=msg2, company="Wipro", role=None), db)

        assert r2.is_new_family is True
        assert str(r2.family_id) != str(r1.family_id)


# ---------------------------------------------------------------------------
# Group 4: company absent — no anchor possible
# ---------------------------------------------------------------------------

class TestNoCompany:

    @pytest.mark.asyncio
    async def test_unmapped_when_no_families_exist(self, db):
        msg_id = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg_id)
        record = make_record(message_id=msg_id, company=None, role=None)
        result = await resolve_family(record, db)

        assert result.contribution_role == "unmapped"
        assert result.family_id is None
        assert result.matched_on == "none"
        assert result.is_new_family is False

    @pytest.mark.asyncio
    async def test_attaches_to_most_recent_family_when_one_exists(self, db):
        msg1 = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg1)
        r1 = await resolve_family(make_record(message_id=msg1, company="Google", role="SDE"), db)

        msg2 = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg2)
        r2 = await resolve_family(make_record(message_id=msg2, company=None, role=None), db)

        assert r2.is_new_family is False
        assert str(r2.family_id) == str(r1.family_id)
        assert r2.contribution_role == "context"
        assert r2.matched_on == "none"

    @pytest.mark.asyncio
    async def test_no_company_message_mapped_as_context(self, db):
        msg1 = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg1)
        await resolve_family(make_record(message_id=msg1, company="Flipkart", role="SDE"), db)

        msg2 = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg2)
        await resolve_family(make_record(message_id=msg2, company=None, role=None), db)

        mapping = await queries.get_message_family_mapping(db, msg2)
        assert mapping is not None
        assert mapping.contribution_role == "context"

    @pytest.mark.asyncio
    async def test_unmapped_result_has_no_family_id(self, db):
        msg_id = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg_id)
        record = make_record(message_id=msg_id, company=None, role=None)
        result = await resolve_family(record, db)

        assert result.family_id is None

    @pytest.mark.asyncio
    async def test_attaches_to_most_recent_of_multiple_families(self, db):
        msg1 = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg1)
        await resolve_family(make_record(message_id=msg1, company="Old Co", role="SDE"), db)

        msg2 = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg2)
        r2 = await resolve_family(make_record(message_id=msg2, company="New Co", role="PM"), db)

        msg3 = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg3)
        r3 = await resolve_family(make_record(message_id=msg3, company=None, role=None), db)

        assert str(r3.family_id) == str(r2.family_id)


# ---------------------------------------------------------------------------
# Group 5: FamilyResolutionResult correctness
# ---------------------------------------------------------------------------

class TestResolutionResultFields:

    @pytest.mark.asyncio
    async def test_result_is_dataclass(self, db):
        msg_id = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg_id)
        record = make_record(message_id=msg_id, company="Razorpay", role="SDE")
        result = await resolve_family(record, db)
        assert isinstance(result, FamilyResolutionResult)

    @pytest.mark.asyncio
    async def test_result_has_all_fields(self, db):
        msg_id = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg_id)
        record = make_record(message_id=msg_id, company="CRED", role="PM")
        result = await resolve_family(record, db)

        assert hasattr(result, "family_id")
        assert hasattr(result, "company")
        assert hasattr(result, "role")
        assert hasattr(result, "is_new_family")
        assert hasattr(result, "contribution_role")
        assert hasattr(result, "matched_on")

    @pytest.mark.asyncio
    async def test_matched_on_company_and_role_for_full_match(self, db):
        msg_id = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg_id)
        result = await resolve_family(
            make_record(message_id=msg_id, company="Swiggy", role="SDE"), db
        )
        assert result.matched_on == "company_and_role"

    @pytest.mark.asyncio
    async def test_matched_on_company_only_for_partial(self, db):
        msg_id = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg_id)
        result = await resolve_family(
            make_record(message_id=msg_id, company="Zomato", role=None), db
        )
        assert result.matched_on == "company_only"

    @pytest.mark.asyncio
    async def test_matched_on_none_for_no_company(self, db):
        msg_id = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg_id)
        result = await resolve_family(
            make_record(message_id=msg_id, company=None, role=None), db
        )
        assert result.matched_on == "none"


# ---------------------------------------------------------------------------
# Group 6: idempotency — running same message twice
# ---------------------------------------------------------------------------

class TestIdempotency:

    @pytest.mark.asyncio
    async def test_same_message_id_does_not_create_duplicate_family(self, db):
        msg_id = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg_id)
        record = make_record(message_id=msg_id, company="Paytm", role="SDE")

        r1 = await resolve_family(record, db)
        r2 = await resolve_family(record, db)

        assert str(r1.family_id) == str(r2.family_id)

    @pytest.mark.asyncio
    async def test_second_call_returns_is_new_family_false(self, db):
        msg_id = f"msg-{uuid4().hex[:8]}"
        await seed_message(db, msg_id)
        record = make_record(message_id=msg_id, company="Ola", role="PM")

        await resolve_family(record, db)
        r2 = await resolve_family(record, db)

        assert r2.is_new_family is False