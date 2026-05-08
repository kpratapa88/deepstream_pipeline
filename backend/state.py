"""
Shared in-memory application state for the FastAPI backend.
"""
import threading
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Tuple


@dataclass
class EventEntry:
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


@dataclass
class AppState:
    event_log: deque = field(default_factory=lambda: deque(maxlen=500))

    # Two snapshot stores: osd = with bounding boxes, raw = clean frame
    snapshots_osd: Dict[int, bytes] = field(default_factory=dict)
    snapshots_raw: Dict[int, bytes] = field(default_factory=dict)

    # Last snapshot arrival time per stream — used to detect stale/stopped streams
    snapshot_ts: Dict[int, float] = field(default_factory=dict)

    # Keep snapshots as alias for osd for backward compat
    @property
    def snapshots(self) -> Dict[int, bytes]:
        return self.snapshots_osd

    # Rolling JPEG frame buffer per stream for ClipWriter
    frame_buffers: Dict[int, deque] = field(default_factory=dict)
    frame_buffers_lock: threading.Lock = field(default_factory=threading.Lock)

    clip_registry: List[dict] = field(default_factory=list)
    latest_heartbeat: dict = field(default_factory=dict)
    latest_benchmark: dict = field(default_factory=dict)
    alert_highlights: Dict[int, Tuple[str, int]] = field(default_factory=dict)

    # OSD mode flag — pipeline polls this to enable/disable bounding box overlay
    osd_enabled: bool = True

    def push_frame(self, stream_id: int, jpeg_bytes: bytes) -> None:
        import time
        with self.frame_buffers_lock:
            if stream_id not in self.frame_buffers:
                # 90 frames = 3 seconds at 30 FPS — enough for 2s pre-event buffer
                self.frame_buffers[stream_id] = deque(maxlen=90)
            self.frame_buffers[stream_id].append((jpeg_bytes, time.time()))

    def get_frames(self, stream_id: int) -> list:
        with self.frame_buffers_lock:
            buf = self.frame_buffers.get(stream_id)
            return list(buf) if buf else []

    def active_stream_ids(self, max_age_s: float = 5.0) -> list:
        """Return stream IDs that received a snapshot within max_age_s seconds."""
        import time
        now = time.time()
        return sorted(
            sid for sid, ts in self.snapshot_ts.items()
            if now - ts < max_age_s
        )


app_state = AppState()
