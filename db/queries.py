# db/queries.py
import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, insert

from db.models import (
    Message, Family, MessageFamilyMap,
    SheetsSync, DeadLetterQueue, compute_content_hash
)
from utils.logger import get_logger

logger = get_logger(__name__)


# ── Message queries ───────────────────────────────────────────────────────────

async def message_exists(db: AsyncSession, message_id: str) -> bool:
    result = await db.execute(
        select(Message.message_id).where(Message.message_id == message_id)
    )
    return result.scalar_one_or_none() is not None


async def content_hash_exists(db: AsyncSession, content_hash: str) -> bool:
    result = await db.execute(
        select(Message.message_id).where(Message.content_hash == content_hash)
    )
    return result.scalar_one_or_none() is not None


async def save_message(db: AsyncSession, payload: dict) -> tuple[bool, str]:
    """
    Save a message to the database.
    Returns (saved: bool, reason: str)
    """
    message_id = payload["message_id"]
    text = payload["text"]
    content_hash = compute_content_hash(text)

    # Layer 1: message_id deduplication
    if await message_exists(db, message_id):
        logger.info({"message_id": message_id}, "Skipped — duplicate message_id")
        return False, "duplicate_message_id"

    # Layer 2: content hash deduplication
    if await content_hash_exists(db, content_hash):
        logger.info(
            {"message_id": message_id, "content_hash": content_hash},
            "Skipped — duplicate content hash"
        )
        return False, "duplicate_content_hash"

    # Parse timestamp
    timestamp = payload.get("timestamp")
    if isinstance(timestamp, str):
        timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    elif not isinstance(timestamp, datetime):
        timestamp = datetime.now(timezone.utc)

    message = Message(
        message_id=message_id,
        text=text,
        timestamp=timestamp,
        sender=payload.get("sender"),
        reply_to_id=payload.get("reply_to_id"),
        reply_to_preview=payload.get("reply_to_preview"),
        content_hash=content_hash,
        processed=False,
        process_attempts=0,
    )

    db.add(message)
    await db.commit()

    logger.info({"message_id": message_id}, "Message saved")
    return True, "saved"


async def get_unprocessed_messages(
    db: AsyncSession, limit: int = 100
) -> list[Message]:
    result = await db.execute(
        select(Message)
        .where(Message.processed == False)
        .order_by(Message.timestamp.asc())
        .limit(limit)
    )
    return result.scalars().all()


async def mark_message_processed(db: AsyncSession, message_id: str):
    await db.execute(
        update(Message)
        .where(Message.message_id == message_id)
        .values(processed=True)
    )
    await db.commit()


async def increment_process_attempts(db: AsyncSession, message_id: str):
    result = await db.execute(
        select(Message.process_attempts)
        .where(Message.message_id == message_id)
    )
    current = result.scalar_one_or_none() or 0
    await db.execute(
        update(Message)
        .where(Message.message_id == message_id)
        .values(process_attempts=current + 1)
    )
    await db.commit()


# ── Family queries ────────────────────────────────────────────────────────────

async def create_family(db: AsyncSession, data: dict) -> Family:
    family = Family(
        company=data.get("company"),
        role=data.get("role"),
        deadline=data.get("deadline"),
        package=data.get("package"),
        jd_link=data.get("jd_link"),
        notes=data.get("notes", []),
        confidence=data.get("confidence"),
    )
    db.add(family)
    await db.commit()
    await db.refresh(family)
    logger.info({"family_id": str(family.id)}, "Family created")
    return family


async def get_family(
    db: AsyncSession, family_id: uuid.UUID
) -> Optional[Family]:
    result = await db.execute(
        select(Family).where(Family.id == family_id)
    )
    return result.scalar_one_or_none()


async def update_family(
    db: AsyncSession, family_id: uuid.UUID, updates: dict
) -> Optional[Family]:
    """
    Merge updates into an existing family following merge rules:
    - Never overwrite a non-null field with null
    - Deadline: update only if new value is later
    - Package: update if new value is non-null
    - jd_link: update only if new URL is non-null
    - notes: always append
    - confidence: take max
    """
    family = await get_family(db, family_id)
    if not family:
        return None

    if updates.get("company") and not family.company:
        family.company = updates["company"]

    if updates.get("role") and not family.role:
        family.role = updates["role"]

    # Deadline: only update if new deadline is later
    new_deadline = updates.get("deadline")
    if new_deadline:
        if not family.deadline or new_deadline > family.deadline:
            family.deadline = new_deadline

    if updates.get("package"):
        family.package = updates["package"]

    if updates.get("jd_link") and not family.jd_link:
        family.jd_link = updates["jd_link"]

    # Notes: always append, never replace
    new_notes = updates.get("notes", [])
    if new_notes:
        existing = family.notes or []
        family.notes = existing + [n for n in new_notes if n not in existing]

    # Confidence: always take the max
    new_confidence = updates.get("confidence")
    if new_confidence is not None:
        family.confidence = max(family.confidence or 0.0, new_confidence)

    family.updated_at = datetime.now(timezone.utc)
    await db.commit()
    await db.refresh(family)
    return family


async def get_all_families(
    db: AsyncSession, limit: int = 100, offset: int = 0
) -> list[Family]:
    result = await db.execute(
        select(Family)
        .order_by(Family.created_at.desc())
        .limit(limit)
        .offset(offset)
    )
    return result.scalars().all()


# ── MessageFamilyMap queries ──────────────────────────────────────────────────

async def map_message_to_family(
    db: AsyncSession,
    message_id: str,
    family_id: uuid.UUID,
    contribution_role: str = "anchor",
):
    mapping = MessageFamilyMap(
        message_id=message_id,
        family_id=family_id,
        contribution_role=contribution_role,
    )
    db.add(mapping)
    await db.commit()
    logger.info(
        {
            "message_id": message_id,
            "family_id": str(family_id),
            "role": contribution_role,
        },
        "Message mapped to family"
    )


# ── SheetsSync queries ────────────────────────────────────────────────────────

async def upsert_sheets_sync(
    db: AsyncSession,
    family_id: uuid.UUID,
    sheets_row_id: Optional[str],
    sync_status: str,
):
    result = await db.execute(
        select(SheetsSync).where(SheetsSync.family_id == family_id)
    )
    existing = result.scalar_one_or_none()

    if existing:
        existing.last_synced_at = datetime.now(timezone.utc)
        existing.sheets_row_id = sheets_row_id
        existing.sync_status = sync_status
    else:
        sync = SheetsSync(
            family_id=family_id,
            last_synced_at=datetime.now(timezone.utc),
            sheets_row_id=sheets_row_id,
            sync_status=sync_status,
        )
        db.add(sync)

    await db.commit()


async def get_failed_syncs(db: AsyncSession) -> list[SheetsSync]:
    result = await db.execute(
        select(SheetsSync).where(SheetsSync.sync_status == "failed")
    )
    return result.scalars().all()


# ── DeadLetterQueue queries ───────────────────────────────────────────────────

async def add_to_dead_letter(
    db: AsyncSession,
    message_id: Optional[str],
    failure_reason: str,
    raw_payload: Optional[dict] = None,
):
    entry = DeadLetterQueue(
        message_id=message_id,
        failure_reason=failure_reason,
        failed_at=datetime.now(timezone.utc),
        raw_payload=raw_payload,
    )
    db.add(entry)
    await db.commit()
    logger.warning(
        {"message_id": message_id, "reason": failure_reason},
        "Added to dead letter queue"
    )


async def get_dead_letter_entries(
    db: AsyncSession, limit: int = 50
) -> list[DeadLetterQueue]:
    result = await db.execute(
        select(DeadLetterQueue)
        .order_by(DeadLetterQueue.failed_at.desc())
        .limit(limit)
    )
    return result.scalars().all()