# main.py
# Phase 0 — minimal FastAPI app with /health endpoint
# Full processing pipeline, scheduler, and routes added in later phases

from contextlib import asynccontextmanager
from fastapi import FastAPI
from utils.logger import get_logger

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("FastAPI startup — Phase 0 baseline")
    yield
    logger.info("FastAPI shutdown")


app = FastAPI(
    title="WhatsApp Placement Intelligence System",
    version="0.1.0",
    lifespan=lifespan
)


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "phase": 0,
        "message": "Placement bot is running"
    }