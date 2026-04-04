# # main.py
# # Phase 5 — queue worker wired into lifespan

# import asyncio
# from contextlib import asynccontextmanager
# from datetime import datetime, timezone

# from fastapi import FastAPI, BackgroundTasks, Depends, HTTPException
# from sqlalchemy.ext.asyncio import AsyncSession

# from db.database import get_db, init_db, AsyncSessionLocal
# from db.queries import save_message
# from db.queue import enqueue, get_queue_stats, reset_stale_processing
# from worker.processor import process_single, process_pending_messages
# from scraper.receiver import MessagePayload, TEST_MESSAGES
# from utils.logger import get_logger
# from config.settings import settings

# logger = get_logger(__name__)


# # ── Scheduler loop ────────────────────────────────────────────────────────────

# async def scheduler_loop():
#     """
#     Safety net — retries any unprocessed messages every SCHEDULER_INTERVAL_SECONDS.
#     This is NOT the primary processing trigger.
#     Primary processing fires immediately on /ingest as a background task.
#     """
#     while True:
#         await asyncio.sleep(settings.SCHEDULER_INTERVAL_SECONDS)
#         logger.info("Scheduler: running retry pass for unprocessed messages")
#         try:
#             await process_pending_messages()
#         except Exception as e:
#             logger.error(
#                 {"error": str(e)},
#                 "Scheduler encountered an error"
#             )


# # ── Lifespan ──────────────────────────────────────────────────────────────────

# @asynccontextmanager
# async def lifespan(app: FastAPI):
#     logger.info("FastAPI startup — Phase 5")

#     # Initialize DB tables
#     await init_db()
#     logger.info("Database tables initialized")

#     # Reset any items stuck in processing from a previous crashed run
#     async with AsyncSessionLocal() as db:
#         await reset_stale_processing(db)

#     # Start scheduler loop
#     scheduler_task = None
#     if settings.SCHEDULER_ENABLED:
#         scheduler_task = asyncio.create_task(scheduler_loop())
#         logger.info(
#             {"interval_seconds": settings.SCHEDULER_INTERVAL_SECONDS},
#             "Scheduler started"
#         )

#     yield

#     # Shutdown
#     if scheduler_task:
#         scheduler_task.cancel()
#         try:
#             await scheduler_task
#         except asyncio.CancelledError:
#             pass
#     logger.info("FastAPI shutdown")


# # ── App ───────────────────────────────────────────────────────────────────────

# app = FastAPI(
#     title="WhatsApp Placement Intelligence System",
#     version="0.5.0",
#     lifespan=lifespan,
# )


# # ── Routes ────────────────────────────────────────────────────────────────────

# @app.get("/health")
# async def health():
#     return {
#         "status": "ok",
#         "phase": 5,
#         "message": "Placement bot is running",
#         "scheduler_enabled": settings.SCHEDULER_ENABLED,
#         "scheduler_interval_seconds": settings.SCHEDULER_INTERVAL_SECONDS,
#     }


# @app.post("/ingest")
# async def ingest(
#     payload: MessagePayload,
#     background_tasks: BackgroundTasks,
#     db: AsyncSession = Depends(get_db),
# ):
#     """
#     Receive a message from Baileys:
#     1. Save to messages table (with deduplication)
#     2. Enqueue in queue_items
#     3. Immediately fire background processing
#     """
#     saved, reason = await save_message(db, payload.model_dump())

#     if not saved:
#         return {
#             "status": "skipped",
#             "reason": reason,
#             "message_id": payload.message_id,
#         }

#     await enqueue(db, payload.message_id)
#     background_tasks.add_task(process_single, payload.message_id)

#     logger.info(
#         {"message_id": payload.message_id},
#         "Message accepted, enqueued, background task fired"
#     )

#     return {
#         "status": "accepted",
#         "message_id": payload.message_id,
#     }


# @app.post("/ingest/test")
# async def ingest_test(
#     background_tasks: BackgroundTasks,
#     db: AsyncSession = Depends(get_db),
# ):
#     """Inject test messages without WhatsApp. Development only."""
#     if settings.ENV != "development":
#         raise HTTPException(
#             status_code=403,
#             detail="Test endpoint only available in development mode"
#         )

#     results = []
#     for msg in TEST_MESSAGES:
#         saved, reason = await save_message(db, msg)
#         if saved:
#             await enqueue(db, msg["message_id"])
#             background_tasks.add_task(process_single, msg["message_id"])

#         results.append({
#             "message_id": msg["message_id"],
#             "status": "accepted" if saved else "skipped",
#             "reason": reason if not saved else None,
#         })

#     return {
#         "status": "complete",
#         "total": len(TEST_MESSAGES),
#         "accepted": sum(1 for r in results if r["status"] == "accepted"),
#         "skipped": sum(1 for r in results if r["status"] == "skipped"),
#         "results": results,
#     }


# @app.get("/messages")
# async def list_messages(
#     limit: int = 20,
#     db: AsyncSession = Depends(get_db),
# ):
#     """List recently ingested messages."""
#     from sqlalchemy import select
#     from db.models import Message

#     result = await db.execute(
#         select(Message)
#         .order_by(Message.created_at.desc())
#         .limit(limit)
#     )
#     messages = result.scalars().all()

#     return {
#         "count": len(messages),
#         "messages": [
#             {
#                 "message_id": m.message_id,
#                 "text": m.text[:80],
#                 "sender": m.sender,
#                 "processed": m.processed,
#                 "created_at": m.created_at.isoformat(),
#             }
#             for m in messages
#         ],
#     }


# @app.get("/queue/stats")
# async def queue_stats(db: AsyncSession = Depends(get_db)):
#     """Return current queue status counts."""
#     stats = await get_queue_stats(db)
#     return stats


# @app.post("/queue/process")
# async def queue_process(background_tasks: BackgroundTasks):
#     """
#     Manually trigger processing of all pending queue items.
#     Used for testing and debugging only.
#     """
#     if settings.ENV != "development":
#         raise HTTPException(
#             status_code=403,
#             detail="Manual process trigger only available in development mode"
#         )
#     background_tasks.add_task(process_pending_messages)
#     return {"status": "triggered", "message": "Processing pending messages in background"}


import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, BackgroundTasks, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from db.database import get_db, init_db, AsyncSessionLocal
from db.queries import save_message
from db.queue import enqueue, get_queue_stats, reset_stale_processing
from worker.processor import process_single_message, process_pending_messages
from scraper.receiver import MessagePayload, TEST_MESSAGES
from utils.logger import get_logger
from config.settings import settings

logger = get_logger(__name__)


async def scheduler_loop():
    """
    Safety net - retries any unprocessed messages every SCHEDULER_INTERVAL_SECONDS.
    This is NOT the primary processing trigger.
    Primary processing fires immediately on /ingest as a background task.
    """
    while True:
        await asyncio.sleep(settings.SCHEDULER_INTERVAL_SECONDS)
        logger.info("Scheduler: running retry pass for unprocessed messages")
        try:
            await process_pending_messages()
        except Exception as e:
            logger.error("Scheduler encountered an error: error=%s", str(e))


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("FastAPI startup - Phase 14")

    await init_db()
    logger.info("Database tables initialized")

    async with AsyncSessionLocal() as db:
        await reset_stale_processing(db)

    scheduler_task = None
    if settings.SCHEDULER_ENABLED:
        scheduler_task = asyncio.create_task(scheduler_loop())
        logger.info(
            "Scheduler started: interval_seconds=%s",
            settings.SCHEDULER_INTERVAL_SECONDS,
        )

    yield

    if scheduler_task:
        scheduler_task.cancel()
        try:
            await scheduler_task
        except asyncio.CancelledError:
            pass

    logger.info("FastAPI shutdown")


app = FastAPI(
    title="WhatsApp Placement Intelligence System",
    version="0.14.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "phase": 14,
        "message": "Placement bot is running",
        "scheduler_enabled": settings.SCHEDULER_ENABLED,
        "scheduler_interval_seconds": settings.SCHEDULER_INTERVAL_SECONDS,
    }


@app.post("/ingest")
async def ingest(
    payload: MessagePayload,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    """
    Receive a message from Baileys:
    1. Save to messages table (with deduplication)
    2. Enqueue in queue_items
    3. Immediately fire background processing
    """
    saved, reason = await save_message(db, payload.model_dump())

    if not saved:
        return {
            "status": "skipped",
            "reason": reason,
            "message_id": payload.message_id,
        }

    await enqueue(db, payload.message_id)
    background_tasks.add_task(process_single_message, payload.message_id)

    logger.info(
        "Message accepted, enqueued, background task fired: message_id=%s",
        payload.message_id,
    )

    return {
        "status": "accepted",
        "message_id": payload.message_id,
    }


@app.post("/ingest/test")
async def ingest_test(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    """Inject test messages without WhatsApp. Development only."""
    if settings.ENV != "development":
        raise HTTPException(
            status_code=403,
            detail="Test endpoint only available in development mode",
        )

    results = []
    for msg in TEST_MESSAGES:
        saved, reason = await save_message(db, msg)
        if saved:
            await enqueue(db, msg["message_id"])
            background_tasks.add_task(process_single_message, msg["message_id"])
        results.append(
            {
                "message_id": msg["message_id"],
                "status": "accepted" if saved else "skipped",
                "reason": reason if not saved else None,
            }
        )

    return {
        "status": "complete",
        "total": len(TEST_MESSAGES),
        "accepted": sum(1 for r in results if r["status"] == "accepted"),
        "skipped": sum(1 for r in results if r["status"] == "skipped"),
        "results": results,
    }


@app.get("/messages")
async def list_messages(
    limit: int = 20,
    db: AsyncSession = Depends(get_db),
):
    """List recently ingested messages."""
    from sqlalchemy import select
    from db.models import Message

    result = await db.execute(
        select(Message)
        .order_by(Message.created_at.desc())
        .limit(limit)
    )
    messages = result.scalars().all()

    return {
        "count": len(messages),
        "messages": [
            {
                "message_id": m.message_id,
                "text": m.text[:80],
                "sender": m.sender,
                "processed": m.processed,
                "created_at": m.created_at.isoformat(),
            }
            for m in messages
        ],
    }


@app.get("/queue/stats")
async def queue_stats(db: AsyncSession = Depends(get_db)):
    """Return current queue status counts."""
    stats = await get_queue_stats(db)
    return stats


@app.post("/queue/process")
async def queue_process(background_tasks: BackgroundTasks):
    """
    Manually trigger processing of all pending queue items.
    Used for testing and debugging only.
    """
    if settings.ENV != "development":
        raise HTTPException(
            status_code=403,
            detail="Manual process trigger only available in development mode",
        )
    background_tasks.add_task(process_pending_messages)
    return {
        "status": "triggered",
        "message": "Processing pending messages in background",
    }