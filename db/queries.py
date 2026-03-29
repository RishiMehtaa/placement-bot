# db/queries.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update
from db.models import Message, compute_content_hash
from utils.logger import get_logger
from datetime import datetime, timezone

logger = get_logger(__name__)


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
    - saved=True means it was inserted
    - saved=False means it was skipped (duplicate)
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

    logger.info(
        {"message_id": message_id, "content_hash": content_hash},
        "Message saved"
    )
    return True, "saved"


async def get_unprocessed_messages(db: AsyncSession, limit: int = 100) -> list[Message]:
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
        select(Message.process_attempts).where(Message.message_id == message_id)
    )
    current = result.scalar_one_or_none() or 0
    await db.execute(
        update(Message)
        .where(Message.message_id == message_id)
        .values(process_attempts=current + 1)
    )
    await db.commit()