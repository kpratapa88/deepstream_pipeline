"""
GET /api/health    — pipeline online status + GPU metrics
GET /api/benchmark — per-stream FPS/latency stats
Requirements: 2.3, 2.6, 7.1, 7.2, 7.3
"""
import time
from typing import Any, Dict

from fastapi import APIRouter

from backend.state import app_state

router = APIRouter()

# pipeline_online = True if last heartbeat arrived within this window
_HEARTBEAT_TIMEOUT_MS = 15_000


@router.get("/api/health")
def get_health() -> Dict[str, Any]:
    hb = app_state.latest_heartbeat
    now_ms = int(time.time() * 1000)

    last_heartbeat_ms: int = int(hb.get("timestamp_ms", 0)) if hb else 0
    pipeline_online: bool = (
        last_heartbeat_ms > 0
        and (now_ms - last_heartbeat_ms) < _HEARTBEAT_TIMEOUT_MS
    )

    payload = hb.get("payload", {}) if hb else {}

    # Debug: log what's in the payload so we can trace missing fields
    import logging as _log
    _log.getLogger("ds.api").debug("heartbeat payload keys: %s", list(payload.keys()))

    highlights = {
        str(sid): {"alert_type": atype, "timestamp_ms": ts_ms}
        for sid, (atype, ts_ms) in app_state.alert_highlights.items()
        if now_ms - ts_ms < 1000
    }

    return {
        "pipeline_online": pipeline_online,
        "broker_connected": bool(hb),
        "streams_active": payload.get("streams_active", 0) or 0,
        "fps_total":      float(payload.get("fps_total",    0.0) or 0.0),
        "gpu_util_pct":   float(payload.get("gpu_util_pct", 0.0) or 0.0),
        "vram_used_mb":   float(payload.get("vram_used_mb", 0.0) or 0.0),
        "cpu_util_pct":   float(payload.get("cpu_util_pct", 0.0) or 0.0),
        "ram_used_mb":    float(payload.get("ram_used_mb",  0.0) or 0.0),
        "last_heartbeat_ms": last_heartbeat_ms,
        "alert_highlights": highlights,
        "_payload_keys": list(payload.keys()),  # temporary debug field
    }


@router.get("/api/benchmark")
def get_benchmark() -> Dict[str, Any]:
    """
    Return the latest per-stream benchmark stats.
    Requirements: 2.6, 7.2
    """
    bm = app_state.latest_benchmark
    if not bm:
        return {"streams": []}

    payload = bm.get("payload", {})
    raw_streams = payload.get("streams", [])

    # Pipeline publishes streams as a dict {stream_id: {...}} — normalise to list
    if isinstance(raw_streams, dict):
        streams_list = [
            {"stream_id": sid, **data}
            for sid, data in raw_streams.items()
            if isinstance(data, dict)
        ]
    else:
        streams_list = [s for s in raw_streams if isinstance(s, dict)]

    return {
        "timestamp_ms": bm.get("timestamp_ms", 0),
        "streams": streams_list,
    }
