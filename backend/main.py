"""
FastAPI application entry point.

Start with:
    uvicorn backend.main:app --host 0.0.0.0 --port 8000 --no-access-log
"""
import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from backend.kafka_consumer import consumer
from backend.routers import events, health, internal, media, streams, pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-5s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("ds.api")

# Paths that are polled constantly — only log errors for these
_SILENT_PREFIXES = (
    "/api/snapshot",
    "/api/health",
    "/api/events",
    "/internal/snapshot",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    consumer.start()
    yield
    consumer.stop()


app = FastAPI(title="DeepStream API", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def _log(request: Request, call_next):
    t0       = time.monotonic()
    response = await call_next(request)
    ms       = (time.monotonic() - t0) * 1000
    path     = request.url.path
    status   = response.status_code
    silent   = any(path.startswith(p) for p in _SILENT_PREFIXES)

    # Always log errors; only log non-silent paths at INFO
    if status >= 500:
        logger.error("%s %s  %d  %.0fms", request.method, path, status, ms)
    elif status >= 400 and not silent:
        logger.warning("%s %s  %d  %.0fms", request.method, path, status, ms)
    elif not silent:
        logger.info("%s %s  %d  %.0fms", request.method, path, status, ms)

    return response


app.include_router(events.router)
app.include_router(streams.router)
app.include_router(health.router)
app.include_router(media.router)
app.include_router(internal.router)
app.include_router(pipeline.router)
