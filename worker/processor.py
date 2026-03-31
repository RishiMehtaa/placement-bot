"""
Queue worker — processes messages from the queue.

process_single(message_id)     — processes one message end-to-end
process_pending_messages()     — dequeues and processes all pending items
run_pipeline(message_id, text) — full extraction pipeline (Stages 1-2 live, Stages 3–8 stubs)
"""

import asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from db.database import get_db_context
from db.queue import dequeue_pending, mark_done, mark_failed
from db.queries import (
    get_unprocessed_messages,
    mark_message_processed,
    increment_process_attempts,
    add_to_dead_letter,
)
from extraction.preprocessor import preprocess
from extraction.regex_extractor import extract_with_regex
from utils.logger import get_logger

logger = get_logger(__name__)

MAX_ATTEMPTS = 3


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

async def run_pipeline(message_id: str, text: str) -> bool:
    """
    Full extraction pipeline.

    Stage 1 — Preprocessor         (Phase 6 — LIVE)
    Stage 2 — Regex Extractor       (Phase 7 — LIVE)
    Stage 3 — Context Resolver      (Phase 8 — stub)
    Stage 4 — LLM Extractor         (Phase 9 — stub)
    Stage 5 — Normalizer            (Phase 10 — stub)
    Stage 6 — Family Resolution     (Phase 11 — stub)
    Stage 7 — Merge Engine          (Phase 12 — stub)
    Stage 8 — Deduplication         (Phase 13 — stub)

    Returns True if pipeline completed successfully, False otherwise.
    Non-processable messages return True (not an error — just skipped gracefully).
    """

    # ------------------------------------------------------------------
    # Stage 1 — Preprocessing
    # ------------------------------------------------------------------
    preprocessed = preprocess(message_id, text)

    if not preprocessed.is_processable:
        logger.info(
            f"[{message_id}] Message is not processable — skipping remaining stages"
        )
        return True

    logger.debug(f"[{message_id}] Stage 1 complete — proceeding to Stage 2")

    # ------------------------------------------------------------------
    # Stage 2 — Regex Extraction (Phase 7 — LIVE)
    # ------------------------------------------------------------------
    regex_fields = extract_with_regex(preprocessed)
    logger.info(
        f"[{message_id}] Stage 2 complete | "
        f"deadline={regex_fields.deadline_raw!r} | "
        f"package={regex_fields.package_raw!r} | "
        f"jd_link={regex_fields.jd_link!r} | "
        f"confidence={regex_fields.confidence}"
    )

    # ------------------------------------------------------------------
    # Stage 3 — Context Resolution (Phase 8 stub)
    # ------------------------------------------------------------------
    logger.debug(f"[{message_id}] Stage 3 (context resolver) — stub, skipping")

    # ------------------------------------------------------------------
    # Stage 4 — LLM Extraction (Phase 9 stub)
    # ------------------------------------------------------------------
    logger.debug(f"[{message_id}] Stage 4 (llm extractor) — stub, skipping")

    # ------------------------------------------------------------------
    # Stage 5 — Normalization (Phase 10 stub)
    # ------------------------------------------------------------------
    logger.debug(f"[{message_id}] Stage 5 (normalizer) — stub, skipping")

    # ------------------------------------------------------------------
    # Stage 6 — Family Resolution (Phase 11 stub)
    # ------------------------------------------------------------------
    logger.debug(f"[{message_id}] Stage 6 (family resolution) — stub, skipping")

    # ------------------------------------------------------------------
    # Stage 7 — Merge Engine (Phase 12 stub)
    # ------------------------------------------------------------------
    logger.debug(f"[{message_id}] Stage 7 (merge engine) — stub, skipping")

    # ------------------------------------------------------------------
    # Stage 8 — Deduplication (Phase 13 stub)
    # ------------------------------------------------------------------
    logger.debug(f"[{message_id}] Stage 8 (deduplication) — stub, skipping")

    return True


# ---------------------------------------------------------------------------
# Single message processor
# ---------------------------------------------------------------------------

async def process_single(message_id: str) -> None:
    """
    Process one message end-to-end:
    1. Increment attempt counter
    2. Fetch raw text from DB
    3. Run pipeline
    4. Mark done or failed
    5. On unhandled exception: add to dead letter queue
    """
    logger.info(f"[{message_id}] Processing started")

    async with get_db_context() as db:
        try:
            await increment_process_attempts(db, message_id)

            text = await fetch_message_text(db, message_id)
            if text is None:
                logger.warning(f"[{message_id}] Message not found in DB — skipping")
                await mark_failed(db, message_id, "message not found in DB")
                return

            success = await run_pipeline(message_id, text)

            if success:
                await mark_message_processed(db, message_id)
                await mark_done(db, message_id)
                logger.info(f"[{message_id}] Processing complete — marked done")
            else:
                await mark_failed(db, message_id, "pipeline returned False")
                logger.warning(f"[{message_id}] Pipeline returned False — marked failed")

        except Exception as exc:
            error_msg = str(exc)
            logger.error(f"[{message_id}] Unhandled exception: {error_msg}", exc_info=True)
            await mark_failed(db, message_id, error_msg)
            await add_to_dead_letter(db, message_id, error_msg, None)


# ---------------------------------------------------------------------------
# Batch processor (called by scheduler and on startup)
# ---------------------------------------------------------------------------

async def process_pending_messages() -> None:
    """
    Dequeue and process all currently pending messages.
    Called by the scheduler retry loop and once at startup.
    Each message is processed independently — one failure does not block others.
    """
    logger.info("process_pending_messages: scanning queue for pending items")

    async with get_db_context() as db:
        items = await dequeue_pending(db, limit=50)

    if not items:
        logger.info("process_pending_messages: no pending items found")
        return

    logger.info(f"process_pending_messages: found {len(items)} pending items")

    tasks = [process_single(item.message_id) for item in items]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for item, result in zip(items, results):
        if isinstance(result, Exception):
            logger.error(
                f"[{item.message_id}] gather-level exception: {result}",
                exc_info=result,
            )


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

async def fetch_message_text(db: AsyncSession, message_id: str):
    """Fetch raw text for a message_id. Returns None if not found."""
    from sqlalchemy import select
    from db.models import Message

    result = await db.execute(
        select(Message.text).where(Message.message_id == message_id)
    )
    row = result.scalar_one_or_none()
    return row