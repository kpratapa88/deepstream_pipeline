"""
API client for the FastAPI backend.
Each function caches responses with a TTL matching its poll interval.
Returns None on connection error so the frontend can handle gracefully.
"""
import time
from typing import Optional

import requests

from dashboard.config import BACKEND_URL

# Re-export for components that need the base URL
__all__ = ["BACKEND_URL", "get_events", "get_streams", "get_health", "get_benchmark",
           "get_active_streams", "get_snapshot", "get_models", "get_pipeline_status",
           "start_pipeline", "stop_pipeline", "invalidate_snapshot_cache", 
           "clear_events", "clear_clips"]

# ---------------------------------------------------------------------------
# Simple TTL cache
# ---------------------------------------------------------------------------

_cache: dict = {}


def _get_cached(key: str, ttl: float):
    entry = _cache.get(key)
    if entry and (time.monotonic() - entry["ts"]) < ttl:
        return entry["value"]
    return _MISS


_MISS = object()


def _set_cache(key: str, value):
    _cache[key] = {"value": value, "ts": time.monotonic()}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get(path: str, params: dict | None = None, cache_key: str | None = None, ttl: float = 2.0):
    if cache_key:
        cached = _get_cached(cache_key, ttl)
        if cached is not _MISS:
            return cached
    try:
        resp = requests.get(f"{BACKEND_URL}{path}", params=params, timeout=5)
        resp.raise_for_status()
        result = resp.json()
    except Exception:
        return None
    if cache_key:
        _set_cache(cache_key, result)
    return result


def _get_bytes(path: str, params: dict | None = None, cache_key: str | None = None, ttl: float = 1.0) -> Optional[bytes]:
    if cache_key:
        cached = _get_cached(cache_key, ttl)
        if cached is not _MISS:
            return cached
    try:
        resp = requests.get(f"{BACKEND_URL}{path}", params=params, timeout=5)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        result = resp.content
    except Exception:
        return None
    if cache_key and result:
        _set_cache(cache_key, result)
    return result


def _post(path: str, json: dict | None = None) -> Optional[dict]:
    try:
        resp = requests.post(f"{BACKEND_URL}{path}", json=json, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Public API functions
# ---------------------------------------------------------------------------

def get_events(
    cam_id: str | None = None,
    event_type: str | None = None,
    since_ms: int | None = None,
    limit: int | None = None,
) -> Optional[list]:
    """Poll interval: 1 second. Requirements: 4.1"""
    params = {}
    if cam_id is not None:
        params["cam_id"] = cam_id
    if event_type is not None:
        params["event_type"] = event_type
    if since_ms is not None:
        params["since_ms"] = since_ms
    if limit is not None:
        params["limit"] = limit
    cache_key = f"events:{cam_id}:{event_type}:{since_ms}:{limit}"
    return _get("/api/events", params=params, cache_key=cache_key, ttl=1.0)


def get_streams() -> Optional[list]:
    """Poll interval: 2 seconds. Requirements: 3.1"""
    return _get("/api/streams", cache_key="streams", ttl=2.0)


def get_health() -> Optional[dict]:
    """No cache — always fresh so CPU/GPU values update every rerun."""
    return _get("/api/health", cache_key=None)


def get_benchmark() -> Optional[dict]:
    """Poll interval: 5 seconds. Requirements: 7.1"""
    return _get("/api/benchmark", cache_key="benchmark", ttl=5.0)


def get_active_streams() -> list:
    """Return stream IDs with recent snapshots (last 5s). No cache — always fresh."""
    result = _get("/api/active_streams", cache_key=None)
    if result is None:
        return []
    return result.get("stream_ids", [])


def get_snapshot(stream_id: int, mode: str = "osd") -> Optional[bytes]:
    """Poll interval: 2 seconds. mode='osd' or 'raw'."""
    return _get_bytes(
        f"/api/snapshot/{stream_id}",
        params={"mode": mode},
        cache_key=f"snapshot:{mode}:{stream_id}",
        ttl=2.0,
    )


def invalidate_snapshot_cache() -> None:
    """Clear all cached snapshot entries — call when pipeline restarts."""
    keys = [k for k in _cache if k.startswith("snapshot:")]
    for k in keys:
        _cache.pop(k, None)


def get_models() -> Optional[dict]:
    """Poll interval: 10 seconds (models rarely change)."""
    return _get("/api/models", cache_key="models", ttl=10.0)


def get_pipeline_status() -> Optional[dict]:
    """Poll interval: 2 seconds."""
    return _get("/api/pipeline/status", cache_key="pipeline_status", ttl=2.0)


def start_pipeline(payload: dict) -> Optional[dict]:
    """POST /api/pipeline/start — no caching, always fresh."""
    # Invalidate status cache so next poll reflects new state immediately
    _cache.pop("pipeline_status", None)
    return _post("/api/pipeline/start", json=payload)


def stop_pipeline() -> Optional[dict]:
    """POST /api/pipeline/stop — no caching."""
    _cache.pop("pipeline_status", None)
    return _post("/api/pipeline/stop")


def clear_events() -> Optional[dict]:
    """DELETE /internal/events — clear all events."""
    try:
        resp = requests.delete(f"{BACKEND_URL}/internal/events", timeout=5)
        resp.raise_for_status()
        return {"status": "success"}
    except Exception:
        return None


def clear_clips() -> Optional[dict]:
    """DELETE /internal/clips — delete all clip files."""
    try:
        resp = requests.delete(f"{BACKEND_URL}/internal/clips", timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None
