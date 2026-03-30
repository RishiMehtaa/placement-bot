# worker/processor.py
# Queue worker — picks up pending items and processes them.
# Phase 5: stub pipeline (logs only).
# Phase 14: full pipeline wired in.

from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import AsyncSession

from db.database import AsyncSessionLocal
from db.queue import dequeue_pending, mark_done, mark_failed
from db.queries import mark_message_processed, increment_process_attempts
from utils.logger import get_logger

logger = get_logger(__name__)


async def process_message_stub(message_id: str) -> dict:
    """
    Phase 5 stub — simulates processing.
    Returns a result dict with status and extracted fields.
    Full pipeline (preprocessor → regex → context → LLM → normalize)
    is wired in Phase 14.
    """
    logger.info(
        {"message_id": message_id},
        "Processing message (stub) — full pipeline in Phase 14"
    )
    # Stub always succeeds
    return {
        "status": "success",
        "message_id": message_id,
        "extracted": {},
    }


async def process_single(message_id: str):
    """
    Process one message end-to-end:
    1. Increment attempt counter
    2. Run pipeline (stub in Phase 5, real in Phase 14)
    3. Mark done or failed in queue and messages table
    """
    async with AsyncSessionLocal() as db:
        try:
            await increment_process_attempts(db, message_id)

            result = await process_message_stub(message_id)

            if result["status"] == "success":
                await mark_done(db, message_id)
                await mark_message_processed(db, message_id)
                logger.info(
                    {"message_id": message_id},
                    "Message processed successfully"
                )
            else:
                await mark_failed(db, message_id, "Pipeline returned non-success")
                logger.warning(
                    {"message_id": message_id},
                    "Message processing failed"
                )

        except Exception as e:
            logger.error(
                {"message_id": message_id, "error": str(e)},
                "Exception during message processing"
            )
            try:
                await mark_failed(db, message_id, str(e))
            except Exception as inner:
                logger.error(
                    {"message_id": message_id, "error": str(inner)},
                    "Failed to mark message as failed in queue"
                )


async def process_pending_messages():
    """
    Pick up all pending queue items and process them.
    Called by:
    - The scheduler loop every SCHEDULER_INTERVAL_SECONDS (retry safety net)
    - Directly after ingest as an immediate background task (Phase 14)
    """
    async with AsyncSessionLocal() as db:
        items = await dequeue_pending(db, limit=50)

    if not items:
        logger.info("No pending messages in queue")
        return

    logger.info({"count": len(items)}, "Processing pending messages")

    for item in items:
        await process_single(item.message_id)

    logger.info({"count": len(items)}, "Finished processing batch")