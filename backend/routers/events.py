"""
GET /api/events — filtered event log endpoint.
"""
from typing import List, Optional

from fastapi import APIRouter
from pydantic import BaseModel

from backend.state import EventEntry, app_state

router = APIRouter()


class EventResponse(BaseModel):
    message_id: str
    event_type: str
    alert_type: str
    cam_id: str
    location: str
    confidence: float
    details: dict
    clip_url: str
    timestamp_utc: str
    timestamp_ms: int


@router.get("/api/events", response_model=List[EventResponse])
def get_events(
    cam_id: Optional[str] = None,
    event_type: Optional[str] = None,
    since_ms: Optional[int] = None,
    limit: Optional[int] = 500,
):
    """
    Return the event log as JSON with optional filters.
    Params: cam_id, event_type, since_ms, limit
    Requirements: 2.1
    """
    events = list(app_state.event_log)

    if cam_id is not None:
        events = [e for e in events if e.cam_id == cam_id]
    if event_type is not None:
        events = [e for e in events if e.event_type == event_type]
    if since_ms is not None:
        events = [e for e in events if e.timestamp_ms >= since_ms]

    if limit is not None and limit > 0:
        events = events[-limit:]

    return [
        EventResponse(
            message_id=e.message_id,
            event_type=e.event_type,
            alert_type=e.alert_type,
            cam_id=e.cam_id,
            location=e.location,
            confidence=e.confidence,
            details=e.details,
            clip_url=e.clip_url,
            timestamp_utc=e.timestamp_utc,
            timestamp_ms=e.timestamp_ms,
        )
        for e in events
    ]
