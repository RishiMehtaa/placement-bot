# # db/queue.py
# # PostgreSQL-backed queue for Phase 4 (local development).
# # In Phase 17 this is swapped for Amazon SQS via QUEUE_BACKEND env var.

# from datetime import datetime, timezone
# from typing import Optional

# from sqlalchemy.ext.asyncio import AsyncSession
# from sqlalchemy import select, update

# from db.models import QueueItem
# from utils.logger import get_logger

# from db.models import (
#     Message, Family, MessageFamilyMap,
#     SheetsSync, DeadLetterQueue, QueueItem,
#     compute_content_hash
# )

# logger = get_logger(__name__)

# # Maximum number of processing attempts before a message is marked failed
# MAX_ATTEMPTS = 3


# async def enqueue(db: AsyncSession, message_id: str) -> QueueItem:
#     """
#     Add a message to the queue with status=pending.
#     If the message is already queued, return the existing item.
#     """
#     # Check if already queued
#     result = await db.execute(
#         select(QueueItem).where(QueueItem.message_id == message_id)
#     )
#     existing = result.scalar_one_or_none()
#     if existing:
#         logger.info(f"Message already in queue — skipping enqueue: {message_id} status={existing.status}")
#         return existing

#     item = QueueItem(
#         message_id=message_id,
#         status="pending",
#         enqueued_at=datetime.now(timezone.utc),
#         attempts=0,
#     )
#     db.add(item)
#     await db.commit()
#     await db.refresh(item)

#     logger.info(f"Message enqueued: {message_id} queue_id={item.id}")
#     return item


# async def dequeue_pending(
#     db: AsyncSession, limit: int = 10
# ) -> list[QueueItem]:
#     """
#     Fetch up to `limit` pending queue items and mark them as processing.
#     Used by the scheduler retry loop (Phase 14).
#     """
#     result = await db.execute(
#         select(QueueItem)
#         .where(
#             QueueItem.status == "pending",
#             QueueItem.attempts < MAX_ATTEMPTS,
#         )
#         .order_by(QueueItem.enqueued_at.asc())
#         .limit(limit)
#     )
#     items = result.scalars().all()

#     for item in items:
#         item.status = "processing"
#         item.started_at = datetime.now(timezone.utc)
#         item.attempts += 1

#     await db.commit()

#     logger.info(f"Dequeued {len(items)} items for processing")
#     return items


# async def mark_done(db: AsyncSession, message_id: str):
#     """Mark a queue item as successfully completed."""
#     await db.execute(
#         update(QueueItem)
#         .where(QueueItem.message_id == message_id)
#         .values(
#             status="done",
#             completed_at=datetime.now(timezone.utc),
#         )
#     )
#     await db.commit()
#     logger.info(f"Queue item marked done: {message_id}")


# async def mark_failed(
#     db: AsyncSession, message_id: str, error: str
# ):
#     """
#     Mark a queue item as failed with the error reason.
#     If attempts < MAX_ATTEMPTS, reset to pending for retry.
#     """
#     result = await db.execute(
#         select(QueueItem).where(QueueItem.message_id == message_id)
#     )
#     item = result.scalar_one_or_none()
#     if not item:
#         return

#     if item.attempts >= MAX_ATTEMPTS:
#         item.status = "failed"
#         item.last_error = error
#         logger.warning(f"Queue item permanently failed — max attempts reached: {message_id} attempts={item.attempts}")
#     else:
#         # Reset to pending for retry
#         item.status = "pending"
#         item.last_error = error
#         logger.warning(f"Queue item failed — will retry: {message_id} attempts={item.attempts}")

#     await db.commit()


# async def get_queue_stats(db: AsyncSession) -> dict:
#     """Return counts of items in each status."""
#     from sqlalchemy import func
#     result = await db.execute(
#         select(QueueItem.status, func.count(QueueItem.id))
#         .group_by(QueueItem.status)
#     )
#     rows = result.all()
#     stats = {row[0]: row[1] for row in rows}
#     return {
#         "pending": stats.get("pending", 0),
#         "processing": stats.get("processing", 0),
#         "done": stats.get("done", 0),
#         "failed": stats.get("failed", 0),
#         "total": sum(stats.values()),
#     }

# async def reset_stale_processing(db: AsyncSession, older_than_minutes: int = 10):
#     """
#     Reset any queue items stuck in 'processing' status back to 'pending'.
#     This handles cases where the worker crashed mid-processing.
#     Called on FastAPI startup.
#     """
#     from sqlalchemy import func
#     from datetime import timedelta

#     cutoff = datetime.now(timezone.utc) - timedelta(minutes=older_than_minutes)

#     result = await db.execute(
#         select(QueueItem).where(
#             QueueItem.status == "processing",
#             QueueItem.started_at < cutoff,
#         )
#     )
#     stale = result.scalars().all()

#     for item in stale:
#         item.status = "pending"
#         item.last_error = "Reset from stale processing state on startup"

#     if stale:
#         await db.commit()
#         logger.warning(f"Reset {len(stale)} stale processing items to pending")
#     else:
#         logger.info(f"No stale processing items found")

"""
Queue abstraction — supports postgres (local) and SQS (production).
QUEUE_BACKEND env var controls which backend is used.
"""

import json
import uuid
from datetime import datetime, timezone

from config.settings import settings
from utils.logger import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# SQS client — only imported when QUEUE_BACKEND=sqs
# ---------------------------------------------------------------------------

_sqs_client = None


def _get_sqs_client():
    global _sqs_client
    if _sqs_client is None:
        import boto3
        _sqs_client = boto3.client("sqs", region_name=settings.AWS_REGION)
    return _sqs_client


# ---------------------------------------------------------------------------
# Public interface — same signatures regardless of backend
# ---------------------------------------------------------------------------

async def enqueue_message(db, message_id: str) -> None:
    """Add a message_id to the processing queue."""
    if settings.QUEUE_BACKEND == "sqs":
        await _sqs_enqueue(message_id)
    else:
        await _pg_enqueue(db, message_id)


async def dequeue_messages(db, batch_size: int = 10) -> list[str]:
    """Pull up to batch_size message_ids from the queue."""
    if settings.QUEUE_BACKEND == "sqs":
        return await _sqs_dequeue(batch_size)
    else:
        return await _pg_dequeue(db, batch_size)


async def ack_message(db, message_id: str, receipt_handle: str = None) -> None:
    """Acknowledge successful processing — removes from queue."""
    if settings.QUEUE_BACKEND == "sqs":
        await _sqs_ack(receipt_handle)
    else:
        await _pg_ack(db, message_id)


async def get_queue_stats(db) -> dict:
    """Return queue depth and status summary."""
    if settings.QUEUE_BACKEND == "sqs":
        return await _sqs_stats()
    else:
        return await _pg_stats(db)


# ---------------------------------------------------------------------------
# PostgreSQL backend
# ---------------------------------------------------------------------------

async def _pg_enqueue(db, message_id: str) -> None:
    from sqlalchemy import text
    try:
        await db.execute(
            text("""
                INSERT INTO queue_items (id, message_id, status, created_at)
                VALUES (:id, :message_id, 'pending', NOW())
                ON CONFLICT (message_id) DO NOTHING
            """),
            {"id": str(uuid.uuid4()), "message_id": message_id}
        )
        await db.commit()
        logger.debug(f"[PG Queue] Enqueued {message_id}")
    except Exception as e:
        logger.error(f"[PG Queue] Enqueue failed for {message_id}: {e}")
        raise


async def _pg_dequeue(db, batch_size: int) -> list[str]:
    from sqlalchemy import text
    try:
        result = await db.execute(
            text("""
                UPDATE queue_items
                SET status = 'processing', updated_at = NOW()
                WHERE id IN (
                    SELECT id FROM queue_items
                    WHERE status = 'pending'
                    ORDER BY created_at ASC
                    LIMIT :batch_size
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING message_id
            """),
            {"batch_size": batch_size}
        )
        await db.commit()
        rows = result.fetchall()
        return [row[0] for row in rows]
    except Exception as e:
        logger.error(f"[PG Queue] Dequeue failed: {e}")
        return []


async def _pg_ack(db, message_id: str) -> None:
    from sqlalchemy import text
    try:
        await db.execute(
            text("UPDATE queue_items SET status = 'done', updated_at = NOW() WHERE message_id = :mid"),
            {"message_id": message_id}
        )
        await db.commit()
    except Exception as e:
        logger.error(f"[PG Queue] Ack failed for {message_id}: {e}")


async def _pg_stats(db) -> dict:
    from sqlalchemy import text
    try:
        result = await db.execute(
            text("SELECT status, COUNT(*) FROM queue_items GROUP BY status")
        )
        rows = result.fetchall()
        return {row[0]: row[1] for row in rows}
    except Exception as e:
        logger.error(f"[PG Queue] Stats failed: {e}")
        return {}


# ---------------------------------------------------------------------------
# SQS backend
# ---------------------------------------------------------------------------

async def _sqs_enqueue(message_id: str) -> None:
    try:
        client = _get_sqs_client()
        client.send_message(
            QueueUrl=settings.AWS_SQS_QUEUE_URL,
            MessageBody=json.dumps({"message_id": message_id}),
            MessageGroupId=None,  # Standard queue — no group needed
        )
        logger.debug(f"[SQS Queue] Enqueued {message_id}")
    except Exception as e:
        logger.error(f"[SQS Queue] Enqueue failed for {message_id}: {e}")
        raise


async def _sqs_dequeue(batch_size: int) -> list[str]:
    """
    Returns list of (message_id, receipt_handle) tuples for SQS.
    Caller must pass receipt_handle to ack_message.
    """
    try:
        client = _get_sqs_client()
        response = client.receive_message(
            QueueUrl=settings.AWS_SQS_QUEUE_URL,
            MaxNumberOfMessages=min(batch_size, 10),  # SQS max is 10
            WaitTimeSeconds=1,  # short poll
            VisibilityTimeout=60,
        )
        messages = response.get("Messages", [])
        result = []
        for msg in messages:
            body = json.loads(msg["Body"])
            result.append((body["message_id"], msg["ReceiptHandle"]))
        return result
    except Exception as e:
        logger.error(f"[SQS Queue] Dequeue failed: {e}")
        return []


async def _sqs_ack(receipt_handle: str) -> None:
    if not receipt_handle:
        logger.warning("[SQS Queue] Ack called with no receipt_handle — skipping")
        return
    try:
        client = _get_sqs_client()
        client.delete_message(
            QueueUrl=settings.AWS_SQS_QUEUE_URL,
            ReceiptHandle=receipt_handle,
        )
        logger.debug("[SQS Queue] Message deleted from SQS")
    except Exception as e:
        logger.error(f"[SQS Queue] Ack failed: {e}")


async def _sqs_stats() -> dict:
    try:
        client = _get_sqs_client()
        response = client.get_queue_attributes(
            QueueUrl=settings.AWS_SQS_QUEUE_URL,
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible",
            ]
        )
        attrs = response.get("Attributes", {})
        return {
            "pending": int(attrs.get("ApproximateNumberOfMessages", 0)),
            "in_flight": int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0)),
        }
    except Exception as e:
        logger.error(f"[SQS Queue] Stats failed: {e}")
        return {}