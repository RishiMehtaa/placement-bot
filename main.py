# main.py
# Phase 2 — FastAPI receiver with /ingest and /ingest/test endpoints

import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, BackgroundTasks, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from db.database import get_db, init_db
from db.queries import save_message
from scraper.receiver import MessagePayload, TEST_MESSAGES
from utils.logger import get_logger
from config.settings import settings

logger = get_logger(__name__)


# ── Lifespan ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("FastAPI startup — Phase 2")
    await init_db()
    logger.info("Database tables initialized")
    yield
    logger.info("FastAPI shutdown")


# ── App ───────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="WhatsApp Placement Intelligence System",
    version="0.2.0",
    lifespan=lifespan,
)


# ── Background processing placeholder ────────────────────────────────────────
# Full pipeline wired in Phase 14.
# For now this is a no-op that logs intent.

async def process_single_message(message_id: str):
    logger.info(
        {"message_id": message_id},
        "Background task triggered — full pipeline wired in Phase 14"
    )


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "phase": 2,
        "message": "Placement bot is running",
    }


@app.post("/ingest")
async def ingest(
    payload: MessagePayload,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    """
    Receive a message from Baileys, save it to the database,
    and immediately fire background processing.
    """
    saved, reason = await save_message(db, payload.model_dump())

    if not saved:
        return {
            "status": "skipped",
            "reason": reason,
            "message_id": payload.message_id,
        }

    # Immediately trigger processing pipeline as background task
    background_tasks.add_task(process_single_message, payload.message_id)

    return {
        "status": "accepted",
        "message_id": payload.message_id,
    }


@app.post("/ingest/test")
async def ingest_test(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    """
    Inject all test messages into the pipeline without WhatsApp.
    Used during development when Baileys is not connected.
    Only available in development mode.
    """
    if settings.ENV != "development":
        raise HTTPException(
            status_code=403,
            detail="Test endpoint only available in development mode"
        )

    results = []
    for msg in TEST_MESSAGES:
        saved, reason = await save_message(db, msg)
        if saved:
            background_tasks.add_task(process_single_message, msg["message_id"])
        results.append({
            "message_id": msg["message_id"],
            "status": "accepted" if saved else "skipped",
            "reason": reason if not saved else None,
        })
        logger.info(
            {"message_id": msg["message_id"], "status": "accepted" if saved else "skipped"},
            "Test message ingested"
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
    """
    List recently ingested messages. Used for validation only.
    """
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