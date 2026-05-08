"""
GET /api/snapshot/{stream_id} — latest JPEG thumbnail
GET /api/stream/{stream_id}   — MJPEG live stream (browser-native, real-time)
GET /api/clips/{filename}     — serve MP4 clip
"""
import asyncio
import os

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, Response, StreamingResponse

from backend.config import settings
from backend.state import app_state

router = APIRouter()


@router.get("/api/snapshot/{stream_id}")
def get_snapshot(stream_id: int, mode: str = "osd"):
    store = app_state.snapshots_raw if mode == "raw" else app_state.snapshots_osd
    jpeg_bytes = store.get(stream_id)
    if jpeg_bytes is None:
        raise HTTPException(status_code=404, detail=f"No snapshot for stream {stream_id}")
    return Response(content=jpeg_bytes, media_type="image/jpeg")


@router.get("/api/stream/{stream_id}")
async def mjpeg_stream(stream_id: int):
    """
    MJPEG stream that displays frames as they arrive from the pipeline.
    The pipeline already processes at 30 FPS and reuses inference results between
    detection frames, so we just need to forward frames without additional pacing.
    """
    async def _generate():
        last_frame: bytes = b""
        last_frame_id: int = 0
        import time as _time
        
        while True:
            current_time = _time.time()
            
            # Exit if stream has gone stale (pipeline stopped)
            last_ts = app_state.snapshot_ts.get(stream_id, 0)
            if last_ts > 0 and current_time - last_ts > 8.0:
                break

            # Get the latest frame
            frame = app_state.snapshots_osd.get(stream_id)
            
            # Only send if we have a new frame (different from last sent)
            if frame and frame != last_frame:
                last_frame = frame
                yield (
                    b"--frame\r\n"
                    b"Content-Type: image/jpeg\r\n\r\n"
                    + frame +
                    b"\r\n"
                )
            
            # Small sleep to prevent busy-waiting while checking for new frames
            # Pipeline sends at ~30 FPS (33ms per frame), so check every 10ms
            await asyncio.sleep(0.01)

    return StreamingResponse(
        _generate(),
        media_type="multipart/x-mixed-replace; boundary=frame",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/api/active_streams")
def get_active_streams():
    """Return stream IDs that have received a snapshot in the last 5 seconds."""
    return {"stream_ids": app_state.active_stream_ids(max_age_s=5.0)}


@router.get("/api/clips/{filename}")
@router.head("/api/clips/{filename}")
def get_clip(filename: str):
    safe_name = os.path.basename(filename)
    clip_path = os.path.abspath(os.path.join(settings.CLIPS_DIR, safe_name))
    if not os.path.isfile(clip_path):
        raise HTTPException(status_code=404, detail=f"Clip not found: {safe_name}")
    return FileResponse(clip_path, media_type="video/mp4", filename=safe_name)
