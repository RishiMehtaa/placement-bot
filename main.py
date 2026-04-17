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
from fastapi import Query
from sqlalchemy import text
from db.database import get_db_context


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

from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS.split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "phase": 16,
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




@app.get("/opportunities")
async def get_opportunities(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    search: str = Query(None),
    eligible: str = Query(None),
):
    async with get_db_context() as db:
        base_query = """
                SELECT
                    f.id,
                    f.company,
                    f.role,
                    f.roles,
                    f.deadline,
                    f.package,
                    f.jd_link,
                    f.confidence,
                    f.created_at,
                    f.updated_at,
                    f.notes,
                    ss.sync_status
                FROM families f
                LEFT JOIN sheets_sync ss ON ss.family_id = f.id
            """
        conditions = []
        params = {}

        if search:
            conditions.append("(LOWER(f.company) LIKE :search OR LOWER(f.role) LIKE :search)")
            params["search"] = f"%{search.lower()}%"

        if eligible:
            conditions.append("f.notes::text LIKE :eligible_filter")
            params["eligible_filter"] = f"%\"eligible\": \"{eligible}\"%"

        if conditions:
            base_query += " WHERE " + " AND ".join(conditions)

        base_query += " ORDER BY f.created_at DESC"

        count_query = f"SELECT COUNT(*) FROM ({base_query}) AS sub"
        count_result = await db.execute(text(count_query), params)
        total = count_result.scalar()

        base_query += " LIMIT :limit OFFSET :offset"
        params["limit"] = page_size
        params["offset"] = (page - 1) * page_size

        result = await db.execute(text(base_query), params)
        rows = result.fetchall()

        opportunities = []
        for row in rows:
            # opportunities.append({
            #     "id": str(row.id),
            #     "company": row.company,
            #     "role": row.role,
            #     "deadline": row.deadline.isoformat() if row.deadline else None,
            #     "package": row.package,
            #     "jd_link": row.jd_link,
            #     "confidence": row.confidence,
            #     "created_at": row.created_at.isoformat() if row.created_at else None,
            #     "updated_at": row.updated_at.isoformat() if row.updated_at else None,
            #     "sync_status": row.sync_status,
            # })
            roles = row.roles if row.roles else ([row.role] if row.role else [])
            for role in roles:
                opportunities.append({
                    "id": str(row.id),
                    "company": row.company,
                    "role": role,
                    "deadline": row.deadline.isoformat() if row.deadline else None,
                    "package": row.package,
                    "jd_link": row.jd_link,
                    "confidence": row.confidence,
                    "created_at": row.created_at.isoformat() if row.created_at else None,
                    "updated_at": row.updated_at.isoformat() if row.updated_at else None,
                    "sync_status": row.sync_status,
                })

        # return {
        #     "opportunities": opportunities,
        #     "total": total,
        #     "page": page,
        #     "page_size": page_size,
        #     "total_pages": (total + page_size - 1) // page_size,
        # }
        expanded_total = len(opportunities)
        return {
            "opportunities": opportunities,
            "total": expanded_total,
            "page": page,
            "page_size": page_size,
            "total_pages": (expanded_total + page_size - 1) // page_size,
        }

@app.get("/opportunities/{family_id}")
async def get_opportunity(family_id: str):
    async with get_db_context() as db:
        result = await db.execute(
            text("""
                SELECT f.*, ss.sync_status, ss.last_synced_at
                FROM families f
                LEFT JOIN sheets_sync ss ON ss.family_id = f.id
                WHERE f.id = :id
            """),
            {"id": family_id},
        )
        row = result.fetchone()
        if not row:
            from fastapi import HTTPException
            raise HTTPException(status_code=404, detail="Opportunity not found")

        messages_result = await db.execute(
            text("""
                SELECT m.message_id, m.text, m.timestamp, m.sender, mfm.contribution_role
                FROM message_family_map mfm
                JOIN messages m ON m.message_id = mfm.message_id
                WHERE mfm.family_id = :id
                ORDER BY m.timestamp ASC
            """),
            {"id": family_id},
        )
        msgs = messages_result.fetchall()

        return {
            "id": str(row.id),
            "company": row.company,
            "role": row.role,
            "deadline": row.deadline.isoformat() if row.deadline else None,
            "package": row.package,
            "jd_link": row.jd_link,
            "notes": row.notes,
            "confidence": row.confidence,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
            "sync_status": row.sync_status,
            "last_synced_at": row.last_synced_at.isoformat() if row.last_synced_at else None,
            "messages": [
                {
                    "message_id": m.message_id,
                    "text": m.text,
                    "timestamp": m.timestamp.isoformat() if m.timestamp else None,
                    "sender": m.sender,
                    "contribution_role": m.contribution_role,
                }
                for m in msgs
            ],
        }

@app.get("/analytics/summary")
async def analytics_summary():
    async with get_db_context() as db:
        total = (await db.execute(text("SELECT COUNT(*) + COALESCE(SUM(CARDINALITY(roles)), 0) FROM families"))).scalar()
        today = (await db.execute(text("SELECT COUNT(*) + COALESCE(SUM(CARDINALITY(roles)), 0) FROM families WHERE DATE(created_at) = CURRENT_DATE"))).scalar()
        this_week = (await db.execute(text("SELECT COUNT(*) + COALESCE(SUM(CARDINALITY(roles)), 0) FROM families WHERE deadline BETWEEN NOW() AND NOW() + INTERVAL '7 days'"))).scalar()
        top_companies = (await db.execute(text("""
            SELECT
                company,
                COUNT(*) + COALESCE(SUM(CARDINALITY(roles)), 0) AS count
            FROM families
            WHERE company IS NOT NULL
            GROUP BY company
            ORDER BY count DESC
            LIMIT 5
        """))).fetchall()
        # final_companes = []
        # for r in top_companies:
        #     for role in r.roles if r.roles else ([r.role] if r.role else []):
        #         final_companes.append({"company": r.company, "count": r.count})

        return {
            "total_opportunities": total,
            "new_today": today,
            "deadlines_this_week": this_week,
    
            "top_companies": [{"company": r.company, "count": r.count } for r in top_companies],
        }

@app.get("/analytics/timeline")
async def analytics_timeline():
    async with get_db_context() as db:
        result = await db.execute(text("""
            SELECT DATE(created_at) as date, COUNT(*) as count
            FROM families
            GROUP BY DATE(created_at)
            ORDER BY date ASC
            LIMIT 30
        """))
        rows = result.fetchall()
        return {
            "timeline": [
                {"date": str(r.date), "count": r.count - 1}
                for r in rows
            ]
        }

@app.get("/demo/qr")
async def demo_qr():
    return {
        "connected": True,
        "qr_image_url": "/static/demo_qr.png",
    }

@app.get("/demo/groups")
async def demo_groups():
    return {
        "groups": [
            {"id": "120363406687081890@g.us", "name": "DJ Sanghvi Placements 2027"},
            {"id": "demo-group-2", "name": "CE Internships"},
            {"id": "demo-group-3", "name": "Placement Updates"},
        ]
    }