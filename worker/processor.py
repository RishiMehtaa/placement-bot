"""
Queue worker — processes messages from the queue.

process_single(message_id)     — processes one message end-to-end
process_pending_messages()     — dequeues and processes all pending items
run_pipeline(message)          — full extraction pipeline (Stages 1-3 live, Stages 4–8 stubs)
"""

import asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from db.database import get_db_context
from db.queue import dequeue_pending, mark_done, mark_failed
from db.queries import (
    get_message,                  # CHANGED: replaces fetch_message_text
    get_window_messages,          # NEW: for Stage 3 sliding window
    get_unprocessed_messages,
    mark_message_processed,
    increment_process_attempts,
    add_to_dead_letter,
)
from extraction.preprocessor import preprocess
from extraction.regex_extractor import extract_with_regex
from extraction.context_resolver import resolve_context  # NEW: Stage 3
from utils.logger import get_logger

logger = get_logger(__name__)

MAX_ATTEMPTS = 3


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

async def run_pipeline(message) -> bool:  # CHANGED: accepts full Message object, not (message_id, text)
    """
    Full extraction pipeline.

    Stage 1 — Preprocessor         (Phase 6 — LIVE)
    Stage 2 — Regex Extractor       (Phase 7 — LIVE)
    Stage 3 — Context Resolver      (Phase 8 — LIVE)
    Stage 4 — LLM Extractor         (Phase 9 — stub)
    Stage 5 — Normalizer            (Phase 10 — stub)
    Stage 6 — Family Resolution     (Phase 11 — stub)
    Stage 7 — Merge Engine          (Phase 12 — stub)
    Stage 8 — Deduplication         (Phase 13 — stub)

    Returns True if pipeline completed successfully, False otherwise.
    Non-processable messages return True (not an error — just skipped gracefully).
    """
    message_id = message.message_id  # CHANGED: extracted from object

    # ------------------------------------------------------------------
    # Stage 1 — Preprocessing
    # ------------------------------------------------------------------
    preprocessed = preprocess(message_id, message.text)  # CHANGED: message.text

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
    # Stage 3 — Context Resolution (Phase 8 — LIVE)
    # ------------------------------------------------------------------
    async with get_db_context() as db:
        window_messages = await get_window_messages(
            db,
            before_timestamp=message.timestamp,
            limit=5,
        )

    context_fields = resolve_context(message, window_messages)
    logger.info(
        f"[{message_id}] Stage 3 complete | "
        f"company={context_fields.company!r} | "
        f"role={context_fields.role!r} | "
        f"source={context_fields.context_source} | "
        f"confidence={context_fields.confidence}"
    )

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
    2. Fetch full Message object from DB       ← CHANGED: was fetch_message_text
    3. Run pipeline
    4. Mark done or failed
    5. On unhandled exception: add to dead letter queue
    """
    logger.info(f"[{message_id}] Processing started")

    async with get_db_context() as db:
        try:
            await increment_process_attempts(db, message_id)

            message = await get_message(db, message_id)  # CHANGED: full object, not just text
            if message is None:
                logger.warning(f"[{message_id}] Message not found in DB — skipping")
                await mark_failed(db, message_id, "message not found in DB")
                return

            success = await run_pipeline(message)  # CHANGED: pass full object

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

# NOTE: fetch_message_text helper removed — replaced by get_message in db/queries.py