# """
# Queue worker — orchestrates the full extraction pipeline.
# Stages 1–5 are live. Stages 6–8 are stubs (Phases 11–13).
# """

# import asyncio
# from db.database import get_db_context
# from db.queries import (
#     get_message,
#     get_window_messages,
#     mark_message_processed,
#     increment_process_attempts,
#     add_to_dead_letter,
# )
# from db.queue import dequeue_pending, mark_done, mark_failed
# from extraction.preprocessor import preprocess
# from extraction.regex_extractor import extract_with_regex
# from extraction.context_resolver import resolve_context
# from extraction.llm_extractor import extract_with_llm
# from extraction.normalizer import normalize, NormalizedRecord
# from utils.logger import get_logger

# logger = get_logger(__name__)

# MAX_ATTEMPTS = 3


# # ---------------------------------------------------------------------------
# # Stage stubs — Phases 11–13
# # ---------------------------------------------------------------------------

# async def _stage6_family_resolution(record: NormalizedRecord) -> dict:
#     """Stage 6 stub — Family Resolution (Phase 11)"""
#     logger.debug(f"[Stage 6 stub] family_resolution message_id={record.message_id}")
#     return {}


# async def _stage7_merge_engine(record: NormalizedRecord, family: dict) -> dict:
#     """Stage 7 stub — Merge Engine (Phase 12)"""
#     logger.debug(f"[Stage 7 stub] merge_engine message_id={record.message_id}")
#     return {}


# async def _stage8_deduplication(record: NormalizedRecord, merged: dict) -> None:
#     """Stage 8 stub — Deduplication (Phase 13)"""
#     logger.debug(f"[Stage 8 stub] deduplication message_id={record.message_id}")


# # ---------------------------------------------------------------------------
# # Full pipeline — run all stages for one message
# # ---------------------------------------------------------------------------

# async def run_pipeline(message_id: str) -> None:
#     """
#     Runs all pipeline stages for a single message.
#     Stages 1–5 are live. Stages 6–8 are stubs.
#     """
#     async with get_db_context() as db:
#         message = await get_message(db, message_id)
#         if not message:
#             logger.warning(f"run_pipeline: message_id={message_id} not found in DB")
#             return

#         # Stage 1 — Preprocessor
#         preprocessed = preprocess(message_id, message.text)
#         logger.info(
#             f"[Stage 1] message_id={message_id} "
#             f"is_processable={preprocessed.is_processable} "
#             f"keywords={preprocessed.matched_keywords} "
#             f"urls={len(preprocessed.urls)}"
#         )

#         if not preprocessed.is_processable:
#             logger.info(f"[Stage 1] Skipping non-processable message_id={message_id}")
#             await mark_message_processed(db, message_id)
#             return

#         # Stage 2 — Regex Extractor
#         regex_fields = extract_with_regex(preprocessed)
#         logger.info(
#             f"[Stage 2] message_id={message_id} "
#             f"deadline={regex_fields.deadline_normalized} "
#             f"package={regex_fields.package_normalized} "
#             f"jd_link={regex_fields.jd_link} "
#             f"confidence={regex_fields.confidence}"
#         )

#         # Stage 3 — Context Resolver
#         window_messages = await get_window_messages(
#             db, before_timestamp=message.timestamp, limit=5
#         )
#         context_fields = resolve_context(message, window_messages)
#         logger.info(
#             f"[Stage 3] message_id={message_id} "
#             f"company={context_fields.company} "
#             f"role={context_fields.role} "
#             f"source={context_fields.context_source} "
#             f"confidence={context_fields.confidence}"
#         )

#         # Stage 4 — LLM Extractor
#         llm_fields = extract_with_llm(preprocessed, context_fields)
#         logger.info(
#             f"[Stage 4] message_id={message_id} "
#             f"company={llm_fields.company} "
#             f"role={llm_fields.role} "
#             f"source={llm_fields.source} "
#             f"confidence={llm_fields.confidence}"
#         )

#         # Stage 5 — Normalizer
#         record = normalize(preprocessed, regex_fields, context_fields, llm_fields)
#         logger.info(
#             f"[Stage 5] message_id={message_id} "
#             f"company={record.company} (source={record.company_source}) "
#             f"role={record.role} (source={record.role_source}) "
#             f"deadline={record.deadline} "
#             f"package={record.package} "
#             f"jd_link={record.jd_link} "
#             f"confidence={record.confidence}"
#         )

#         # Stage 6 stub — Family Resolution
#         family = await _stage6_family_resolution(record)

#         # Stage 7 stub — Merge Engine
#         merged = await _stage7_merge_engine(record, family)

#         # Stage 8 stub — Deduplication
#         await _stage8_deduplication(record, merged)

#         # Mark message processed
#         await mark_message_processed(db, message_id)
#         logger.info(f"run_pipeline: complete for message_id={message_id}")


# # ---------------------------------------------------------------------------
# # Single message processor (called by /ingest background task)
# # ---------------------------------------------------------------------------

# async def process_single(message_id: str) -> None:
#     """
#     Entry point for immediate background task processing.
#     Called directly by /ingest for every new message.
#     """
#     async with get_db_context() as db:
#         await increment_process_attempts(db, message_id)

#     try:
#         await run_pipeline(message_id)
#         async with get_db_context() as db:
#             await mark_done(db, message_id)
#         logger.info(f"process_single: done for message_id={message_id}")
#     except Exception as e:
#         logger.error(f"process_single: failed for message_id={message_id}: {e}")
#         async with get_db_context() as db:
#             await mark_failed(db, message_id, str(e))
#             await add_to_dead_letter(db, message_id, str(e), None)


# # ---------------------------------------------------------------------------
# # Batch processor (called by scheduler safety net)
# # ---------------------------------------------------------------------------

# async def process_pending_messages() -> None:
#     """
#     Dequeues up to 50 pending items and processes them concurrently.
#     Called only by the scheduler safety net — not by /ingest.
#     """
#     async with get_db_context() as db:
#         pending = await dequeue_pending(db, limit=50)

#     if not pending:
#         logger.info("process_pending_messages: no pending items")
#         return

#     logger.info(f"process_pending_messages: processing {len(pending)} items")
#     tasks = [process_single(item.message_id) for item in pending]
#     await asyncio.gather(*tasks, return_exceptions=True)


"""
worker/processor.py

Processing pipeline orchestrator.
Runs all 8 stages for a single message.
Called immediately on ingest (BackgroundTask) and by the scheduler (safety net).
"""

from __future__ import annotations

import asyncio
from typing import Optional

from db.database import get_db_context
from db import queries
from db.queries import (
    get_message,
    get_unprocessed_messages,
    get_window_messages,
    increment_process_attempts,
    mark_message_processed,
    map_message_to_family,
    add_to_dead_letter,
)
from extraction.preprocessor import preprocess
from extraction.regex_extractor import extract_with_regex
from extraction.context_resolver import resolve_context
from extraction.llm_extractor import extract_with_llm, LLMExtractedFields
from extraction.normalizer import normalize
from extraction.family_resolver import resolve_family
from extraction.merge_engine import merge_into_family
from extraction.deduplicator import run_deduplication
from integrations.calendar import sync_to_calendar
from utils.logger import get_logger

logger = get_logger(__name__)

MAX_PROCESS_ATTEMPTS = 3

async def run_pipeline(message_id: str) -> None:
    """
    Run the full 8-stage pipeline for a single message.

    Returns True if processing succeeded, False if it failed or was skipped.
    """
    async with get_db_context() as db:
        # ------------------------------------------------------------------ #
        # Load raw message
        # ------------------------------------------------------------------ #
        # message = await queries.get_message(db, message_id)
        # if not message:
        #     logger.warning("Pipeline | message=%s not found in DB", message_id)
        #     return

        # await queries.increment_process_attempts(db, message_id)

        # logger.info("Pipeline | message=%s | starting", message_id)

        message = await get_message(db, message_id)
        if message is None:
            logger.warning("run_pipeline: message_id=%s not found in DB", message_id)
            return False

        if message.process_attempts >= MAX_PROCESS_ATTEMPTS:
            logger.warning(
                "run_pipeline: message_id=%s exceeded max attempts (%d), sending to dead letter",
                message_id,
                MAX_PROCESS_ATTEMPTS,
            )
            await add_to_dead_letter(
                db,
                message_id=message_id,
                failure_reason=f"exceeded max attempts ({MAX_PROCESS_ATTEMPTS})",
                raw_payload={"message_id": message_id},
            )
            await mark_message_processed(db, message_id)
            return False

        await increment_process_attempts(db, message_id)

        logger.info("run_pipeline: START message_id=%s", message_id)

        # ------------------------------------------------------------------ #
        # Stage 1 — Preprocessor
        # ------------------------------------------------------------------ #
        preprocessed = preprocess(
            message_id=message.message_id,
            raw_text=message.text,
        )

        if not preprocessed.is_processable:
            logger.info(
                "Pipeline | message=%s | Stage 1 not processable — skipping",
                message_id,
            )
            await queries.mark_message_processed(db, message_id)
            return

        logger.info(
            "Pipeline | message=%s | Stage 1 complete | urls=%d keywords=%s",
            message_id,
            len(preprocessed.urls),
            preprocessed.matched_keywords,
        )

        # ------------------------------------------------------------------ #
        # Stage 8 — Deduplication (run early, before expensive stages)       #
        # ------------------------------------------------------------------ #
        dedup_result = await run_deduplication(
            message_id=message_id,
            raw_text=message.text,
            urls=preprocessed.urls,
            db=db,
        )
        logger.info(
            "[Stage 8] message_id=%s layers=%s should_skip=%s skip_reason=%s layer3_family=%s",
            message_id,
            dedup_result.layers_fired,
            dedup_result.should_skip,
            dedup_result.skip_reason,
            dedup_result.layer3_duplicate_family_id,
        )

        if dedup_result.should_skip:
            logger.info(
                "run_pipeline: message_id=%s skipped by deduplicator — %s",
                message_id,
                dedup_result.skip_reason,
            )
            await mark_message_processed(db, message_id)
            return True

        # ------------------------------------------------------------------ #
        # Stage 2 — Regex Extractor
        # ------------------------------------------------------------------ #
        regex_fields = extract_with_regex(preprocessed)

        logger.info(
            "Pipeline | message=%s | Stage 2 complete | deadline=%s package=%s jd_link=%s",
            message_id,
            regex_fields.deadline_raw,
            regex_fields.package_raw,
            regex_fields.jd_link,
        )

        # ------------------------------------------------------------------ #
        # Stage 3 — Context Resolver
        # ------------------------------------------------------------------ #
        window_messages = await queries.get_window_messages(db, message.timestamp, limit=5)
        # reply_message = None
        # if message.reply_to_id:
        #     reply_message = await queries.get_message(db, message.reply_to_id)

        context_fields = resolve_context(
            current_message=message,
            window_messages=window_messages,
        )

        logger.info(
            "Pipeline | message=%s | Stage 3 complete | company=%s role=%s confidence=%.2f",
            message_id,
            context_fields.company,
            context_fields.role,
            context_fields.confidence,
        )

# ------------------------------------------------------------------ #
        # Stage 4 — LLM Extractor (only when company or role still unknown)
        # ------------------------------------------------------------------ #
        # should_use_llm = (
        #     context_fields.company is None
        #     or context_fields.role is None
        #     or context_fields.context_source != "self"
        # )

        # if should_use_llm:
        # # if context_fields.company is None or context_fields.role is None:
        #     llm_fields = extract_with_llm(preprocessed, context_fields)
        #     logger.info(
        #         "Pipeline | message=%s | Stage 4 complete | company=%s role=%s confidence=%.2f",
        #         message_id,
        #         llm_fields.company,
        #         llm_fields.role,
        #         llm_fields.confidence,
        #     )
        # else:
        #     llm_fields = LLMExtractedFields(
        #         company=None,
        #         role=None,
        #         confidence=0.0,
        #         reasoning=None,
        #         source="skipped",
        #     )
        #     logger.info(
        #         "Pipeline | message=%s | Stage 4 skipped (company+role resolved by Stage 3)",
        #         message_id,
        #     )        
        # # ------------------------------------------------------------------ #
        # # Stage 5 — Normalizer
        # # ------------------------------------------------------------------ #
        # record = normalize(
        #     preprocessed=preprocessed,
        #     regex_fields=regex_fields,
        #     context_fields=context_fields,
        #     llm_fields=llm_fields,
        # )

        # logger.info(
        #     "Pipeline | message=%s | Stage 5 complete | company=%s role=%s deadline=%s package=%s confidence=%.2f",
        #     message_id,
        #     record.company,
        #     record.role,
        #     record.deadline,
        #     record.package,
        #     record.confidence,
        # )

        llm_fields = extract_with_llm(preprocessed, context_fields)

        logger.info(
            "Pipeline | message=%s | Stage 4 complete | company=%s role=%s confidence=%.2f",
            message_id,
            llm_fields.company,
            llm_fields.roles,
            llm_fields.confidence,
            # llm_fields.source,
        )

        # ------------------------------------------------------------------ #
        # Stage 5 — Normalizer
        # ------------------------------------------------------------------ #
        record = normalize(
            preprocessed=preprocessed,
            regex_fields=regex_fields,
            context_fields=context_fields,
            llm_fields=llm_fields,
        )

        logger.info(
            "Pipeline | message=%s | Stage 5 complete | company=%s role=%s deadline=%s package=%s location=%s confidence=%.2f",
            message_id,
            record.company,
            record.role,
            record.deadline,
            record.package,
            record.location,
            record.confidence,
        )
        # ------------------------------------------------------------------ #
        # Stage 6 — Family Resolver
        # ------------------------------------------------------------------ #
        resolution = await resolve_family(record=record, db=db, current_message=message)

        logger.info(
            "Pipeline | message=%s | Stage 6 complete | family=%s is_new=%s contribution=%s matched_on=%s",
            message_id,
            resolution.family_id,
            resolution.is_new_family,
            resolution.contribution_role,
            resolution.matched_on,
        )

        # ------------------------------------------------------------------ #
        # Stage 7 — Merge Engine (Phase 12 stub)
        # ------------------------------------------------------------------ #
        merge_result = await merge_into_family(record, resolution, db)
        logger.info(
            f"[Stage 7] message_id={message_id} "
            f"family_id={merge_result.family_id} "
            f"was_merged={merge_result.was_merged} "
            f"updated={merge_result.updated_fields} "
            f"skipped={merge_result.skipped_fields}"
        )

        # --- map message → family ---
        # --- map message → family ---
        if resolution.family_id is not None:
            await map_message_to_family(
                db,
                message_id=message_id,
                family_id=str(resolution.family_id),
                contribution_role=resolution.contribution_role,
            )
        else:
            logger.warning(
                "Pipeline | message=%s | Stage 7 skipping map — family_id is None (unmapped message)",
                message_id,
            )
        logger.info(
            "Pipeline | message=%s | Stage 7 stub — merge engine not yet built",
            message_id,
        )

        # # ------------------------------------------------------------------ #
        # # Stage 8 — Deduplication (Phase 13 stub)
        # # ------------------------------------------------------------------ #
        # logger.info(
        #     "Pipeline | message=%s | Stage 8 stub — deduplication not yet built",
        #     message_id,
        # )

        # --- Phase 15: Google Sheets sync will be called here ---
        from integrations.sheets import sync_to_sheets
        # Phase 15 — Sheets sync
        sheets_ok = await sync_to_sheets(str(merge_result.family_id))
        if sheets_ok:
            logger.info(f"Pipeline | Stage 15 | message={message_id} | Sheets sync complete | family_id={merge_result.family_id}")
        else:
            logger.warning(f"Pipeline | Stage 15 |message={message_id} | Sheets sync failed — pipeline continues | family_id={merge_result.family_id}")
        # --- Phase 16: Google Calendar sync will be called here ---
        calendar_ok = await sync_to_calendar(resolution.family_id)
        if calendar_ok:
            logger.info(
                f"[processor] Stage 16 complete — calendar synced for family {resolution.family_id}"
            )
        else:
            logger.warning(
                f"[processor] Stage 16 skipped or failed for family {resolution.family_id} "
                "(deadline may be null or API error — check logs above)"
            )

        logger.info(f"[processor] Pipeline complete for message {message_id}")

        # ------------------------------------------------------------------ #
        # Mark processed
        # ------------------------------------------------------------------ #
        await queries.mark_message_processed(db, message_id)
        logger.info("Pipeline | message=%s | complete", message_id)

async def process_single_message(message_id: str) -> None:
    """Entry point called by /ingest background task and queue worker."""
    logger.info("[BackgroundTask] Triggered for message_id=%s", message_id)
    try:
        await run_pipeline(message_id)
        logger.info("[BackgroundTask] Completed for message_id=%s", message_id)
    except Exception as e:
        logger.error("[BackgroundTask] Unhandled error message_id=%s error=%s", message_id, e, exc_info=True)


async def process_pending_messages() -> None:
    """
    Called by the scheduler every SCHEDULER_INTERVAL_SECONDS.
    Picks up all messages still marked processed=False and retries them.
    """
    async with get_db_context() as db:
        pending = await queries.get_unprocessed_messages(db)
        if not pending:
            logger.info("Scheduler | no pending messages")
            return

        logger.info("Scheduler | processing %d pending messages", len(pending))
        for msg in pending:
            await process_single_message(msg.message_id)