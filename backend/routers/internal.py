"""
POST /internal/snapshot/{stream_id} — pipeline pushes JPEG bytes
POST /internal/clips                — Clip Writer registers a new clip
DELETE /internal/snapshots          — clear all snapshots (pipeline stopped)
Requirements: 2.7, 2.8
"""
import logging
import uuid

from fastapi import APIRouter, Request
from pydantic import BaseModel

from backend.state import EventEntry, app_state

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/internal/snapshot/{mode}/{stream_id}", status_code=204)
async def post_snapshot(mode: str, stream_id: int, request: Request):
    """
    Accept JPEG bytes from the pipeline.
    mode: 'raw' (pre-OSD) or 'osd' (post-OSD with bounding boxes).
    """
    jpeg_bytes = await request.body()
    if jpeg_bytes:
        import asyncio
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _store_snapshot, mode, stream_id, jpeg_bytes)


def _store_snapshot(mode: str, stream_id: int, jpeg_bytes: bytes) -> None:
    import time as _time
    if mode == "raw":
        app_state.snapshots_raw[stream_id] = jpeg_bytes
    else:
        app_state.snapshots_osd[stream_id] = jpeg_bytes
        app_state.push_frame(stream_id, jpeg_bytes)
    # Track last snapshot time for staleness detection
    app_state.snapshot_ts[stream_id] = _time.time()  # frame buffer for clips uses OSD frames


@router.delete("/internal/snapshots", status_code=204)
def clear_snapshots():
    app_state.snapshots_osd.clear()
    app_state.snapshots_raw.clear()
    app_state.alert_highlights.clear()
    with app_state.frame_buffers_lock:
        app_state.frame_buffers.clear()


@router.delete("/internal/events", status_code=204)
def clear_events():
    """Clear all events from the event log."""
    app_state.event_log.clear()
    logger.info("Cleared all events from event log")


@router.delete("/internal/clips", status_code=200)
def clear_clips():
    """Delete all clip files from the clips directory and clear clip registry."""
    import os
    from backend.config import settings
    
    deleted_count = 0
    errors = []
    
    # Clear clip registry
    app_state.clip_registry.clear()
    
    # Delete all files in clips directory
    clips_dir = settings.CLIPS_DIR
    if os.path.exists(clips_dir):
        try:
            for filename in os.listdir(clips_dir):
                file_path = os.path.join(clips_dir, filename)
                if os.path.isfile(file_path) and filename.endswith('.mp4'):
                    try:
                        os.remove(file_path)
                        deleted_count += 1
                    except Exception as e:
                        errors.append(f"{filename}: {str(e)}")
        except Exception as e:
            errors.append(f"Directory error: {str(e)}")
    
    logger.info(f"Cleared {deleted_count} clip files from {clips_dir}")
    
    return {
        "deleted_count": deleted_count,
        "errors": errors if errors else None
    }


@router.get("/internal/active_streams")
def get_active_streams():
    """Return stream IDs that have received at least one snapshot."""
    return {"stream_ids": app_state.active_stream_ids()}


@router.post("/internal/osd", status_code=204)
def set_osd(enabled: bool = True):
    """Toggle OSD overlay — pipeline polls this to show/hide bounding boxes."""
    app_state.osd_enabled = enabled


@router.get("/internal/osd")
def get_osd():
    return {"enabled": app_state.osd_enabled}


@router.get("/internal/frames/{stream_id}")
def get_frames(stream_id: int):
    """
    Return the rolling JPEG frame buffer for a stream as base64-encoded entries.
    Used by the ClipWriter to build clips without needing in-process frame sharing.
    """
    import base64
    entries = app_state.get_frames(stream_id)
    return [
        {"jpeg_b64": base64.b64encode(jpeg).decode(), "ts": ts}
        for jpeg, ts in entries
    ]


class ClipMetadata(BaseModel):
    stream_id: int = 0
    cam_id: str = ""
    location: str = ""
    alert_type: str = ""
    clip_path: str = ""
    clip_url: str = ""
    triggered_at_ms: int = 0


@router.post("/internal/clips", status_code=201)
def post_clip(meta: ClipMetadata):
    """
    Register a new clip from the Clip Writer.
    Appends to clip_registry, updates matching alert event's clip_url,
    and adds a 'clip' entry to the event log.
    Requirements: 2.8
    """
    clip_dict = meta.model_dump()
    app_state.clip_registry.append(clip_dict)

    # Back-fill clip_url on the matching alert event so the table shows it inline
    for entry in reversed(list(app_state.event_log)):
        if (
            entry.event_type == "alert"
            and entry.alert_type == meta.alert_type
            and entry.cam_id == meta.cam_id
            and entry.clip_url == ""
        ):
            entry.clip_url = meta.clip_url
            break

    # Also add a dedicated clip event row
    entry = EventEntry(
        message_id=str(uuid.uuid4()),
        event_type="clip",
        alert_type=meta.alert_type,
        cam_id=meta.cam_id,
        location=meta.location,
        confidence=0.0,
        details=clip_dict,
        clip_url=meta.clip_url,
        timestamp_utc="",
        timestamp_ms=meta.triggered_at_ms,
    )
    app_state.event_log.append(entry)

    return {"status": "registered", "clip_url": meta.clip_url}
