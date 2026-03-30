# db/queue.py
# PostgreSQL-backed queue for Phase 4 (local development).
# In Phase 17 this is swapped for Amazon SQS via QUEUE_BACKEND env var.

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update

from db.models import QueueItem
from utils.logger import get_logger

from db.models import (
    Message, Family, MessageFamilyMap,
    SheetsSync, DeadLetterQueue, QueueItem,
    compute_content_hash
)

logger = get_logger(__name__)

# Maximum number of processing attempts before a message is marked failed
MAX_ATTEMPTS = 3


async def enqueue(db: AsyncSession, message_id: str) -> QueueItem:
    """
    Add a message to the queue with status=pending.
    If the message is already queued, return the existing item.
    """
    # Check if already queued
    result = await db.execute(
        select(QueueItem).where(QueueItem.message_id == message_id)
    )
    existing = result.scalar_one_or_none()
    if existing:
        logger.info(
            {"message_id": message_id, "status": existing.status},
            "Message already in queue — skipping enqueue"
        )
        return existing

    item = QueueItem(
        message_id=message_id,
        status="pending",
        enqueued_at=datetime.now(timezone.utc),
        attempts=0,
    )
    db.add(item)
    await db.commit()
    await db.refresh(item)

    logger.info(
        {"message_id": message_id, "queue_id": item.id},
        "Message enqueued"
    )
    return item


async def dequeue_pending(
    db: AsyncSession, limit: int = 10
) -> list[QueueItem]:
    """
    Fetch up to `limit` pending queue items and mark them as processing.
    Used by the scheduler retry loop (Phase 14).
    """
    result = await db.execute(
        select(QueueItem)
        .where(
            QueueItem.status == "pending",
            QueueItem.attempts < MAX_ATTEMPTS,
        )
        .order_by(QueueItem.enqueued_at.asc())
        .limit(limit)
    )
    items = result.scalars().all()

    for item in items:
        item.status = "processing"
        item.started_at = datetime.now(timezone.utc)
        item.attempts += 1

    await db.commit()

    logger.info({"count": len(items)}, "Dequeued items for processing")
    return items


async def mark_done(db: AsyncSession, message_id: str):
    """Mark a queue item as successfully completed."""
    await db.execute(
        update(QueueItem)
        .where(QueueItem.message_id == message_id)
        .values(
            status="done",
            completed_at=datetime.now(timezone.utc),
        )
    )
    await db.commit()
    logger.info({"message_id": message_id}, "Queue item marked done")


async def mark_failed(
    db: AsyncSession, message_id: str, error: str
):
    """
    Mark a queue item as failed with the error reason.
    If attempts < MAX_ATTEMPTS, reset to pending for retry.
    """
    result = await db.execute(
        select(QueueItem).where(QueueItem.message_id == message_id)
    )
    item = result.scalar_one_or_none()
    if not item:
        return

    if item.attempts >= MAX_ATTEMPTS:
        item.status = "failed"
        item.last_error = error
        logger.warning(
            {"message_id": message_id, "attempts": item.attempts},
            "Queue item permanently failed — max attempts reached"
        )
    else:
        # Reset to pending for retry
        item.status = "pending"
        item.last_error = error
        logger.warning(
            {"message_id": message_id, "attempts": item.attempts},
            "Queue item failed — will retry"
        )

    await db.commit()


async def get_queue_stats(db: AsyncSession) -> dict:
    """Return counts of items in each status."""
    from sqlalchemy import func
    result = await db.execute(
        select(QueueItem.status, func.count(QueueItem.id))
        .group_by(QueueItem.status)
    )
    rows = result.all()
    stats = {row[0]: row[1] for row in rows}
    return {
        "pending": stats.get("pending", 0),
        "processing": stats.get("processing", 0),
        "done": stats.get("done", 0),
        "failed": stats.get("failed", 0),
        "total": sum(stats.values()),
    }