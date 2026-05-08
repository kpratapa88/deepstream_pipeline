"""
Frame publisher: pushes JPEG snapshots to the FastAPI backend.

Two modes per stream:
  raw → POST /internal/snapshot/raw/{stream_id}   (pre-OSD, clean frame)
  osd → POST /internal/snapshot/osd/{stream_id}   (post-OSD, bounding boxes)

Only active when BACKEND_URL env var is set.
Non-blocking: queue(maxsize=2) per (stream, mode) — drops frames when backend is slow.
"""
import logging
import os
import queue
import threading
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)


def _encode_jpeg(frame: np.ndarray) -> Optional[bytes]:
    try:
        import cv2
        ok, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 75])
        return bytes(buf) if ok else None
    except Exception:
        return None


class FramePublisher:
    def __init__(self, backend_url: Optional[str] = None):
        self._backend_url: Optional[str] = (
            backend_url or os.environ.get("BACKEND_URL", "").rstrip("/") or None
        )
        # Keyed by (stream_id, mode)
        self._queues: dict[tuple, queue.Queue] = {}
        self._lock = threading.Lock()
        self._workers: list[threading.Thread] = []
        self._stop_event = threading.Event()

        if self._backend_url:
            print(f"[FramePublisher] active → {self._backend_url}", flush=True)
        else:
            print("[FramePublisher] inactive (BACKEND_URL not set)", flush=True)

    @property
    def active(self) -> bool:
        return self._backend_url is not None and not self._stop_event.is_set()

    def push_frame(self, stream_id: int, frame: np.ndarray) -> None:
        """Non-blocking push — drops silently if queue full."""
        if not self.active:
            return
        q = self._get_or_create_queue(stream_id)
        try:
            q.put_nowait(frame)
        except queue.Full:
            pass

    def start(self) -> None:
        pass  # threads created lazily

    def stop(self) -> None:
        self._stop_event.set()
        with self._lock:
            for q in self._queues.values():
                try:
                    q.put_nowait(None)
                except queue.Full:
                    pass
        for t in self._workers:
            t.join(timeout=2)

    def _get_or_create_queue(self, stream_id: int) -> queue.Queue:
        with self._lock:
            if stream_id not in self._queues:
                q = queue.Queue(maxsize=2)
                self._queues[stream_id] = q
                t = threading.Thread(
                    target=self._drain_worker,
                    args=(stream_id, q),
                    daemon=True,
                    name=f"frame-pub-{stream_id}",
                )
                self._workers.append(t)
                t.start()
            return self._queues[stream_id]

    def _drain_worker(self, stream_id: int, q: queue.Queue) -> None:
        import requests
        url     = f"{self._backend_url}/internal/snapshot/osd/{stream_id}"
        session = requests.Session()
        sent    = 0

        while not self._stop_event.is_set():
            try:
                frame = q.get(timeout=1.0)
            except queue.Empty:
                continue
            if frame is None:
                break

            jpeg = _encode_jpeg(frame)
            if jpeg is None:
                continue

            try:
                resp = session.post(
                    url, data=jpeg,
                    headers={"Content-Type": "image/jpeg"},
                    timeout=3.0,
                )
                sent += 1
                # Only log first frame and every 300 frames (every 10 seconds at 30 FPS)
                if sent == 1 or sent % 300 == 0:
                    print(f"[FramePublisher] stream {stream_id} → {resp.status_code} (sent={sent})", flush=True)
            except Exception as exc:
                # Only log errors occasionally to avoid spam
                if sent % 30 == 0:
                    print(f"[FramePublisher] POST failed stream {stream_id}: {exc}", flush=True)
