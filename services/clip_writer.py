"""
Clip Writer service.

Subscribes to ds.alerts via Kafka. On alert, pulls JPEG frames from the
FastAPI backend's rolling frame buffer, decodes them, and writes an MP4 clip.

Clip structure: 2s pre-event + ~1s at event + 2s post-event = ~5s total
Registers the clip with the backend via POST /internal/clips.

Usage:
    python services/clip_writer.py

Environment variables:
    KAFKA_BROKER    — Kafka bootstrap server  (default: localhost:9092)
    CLIPS_DIR       — Output directory for clips (default: output/clips)
    BACKEND_URL     — FastAPI backend URL (default: http://localhost:8000)
    PRE_EVENT_S     — Seconds of pre-event footage (default: 2)
    POST_EVENT_S    — Seconds of post-event footage (default: 2)
    CLIP_FPS        — Clip playback FPS (default: 30)
"""
import base64
import json
import logging
import os
import subprocess
import shutil
import threading
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional, Tuple

import cv2
import numpy as np
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

KAFKA_BROKER:  str   = os.getenv("KAFKA_BROKER",  "localhost:9092")
CLIPS_DIR:     str   = os.getenv("CLIPS_DIR",     "output/clips")
BACKEND_URL:   str   = os.getenv("BACKEND_URL",   "http://localhost:8000")
PRE_EVENT_S:   float = float(os.getenv("PRE_EVENT_S",  "2"))
POST_EVENT_S:  float = float(os.getenv("POST_EVENT_S", "2"))
CLIP_FPS:      float = float(os.getenv("CLIP_FPS",     "30"))

# Buffer must hold at least PRE_EVENT_S worth of frames
BUFFER_FRAMES: int = int(os.getenv("BUFFER_FRAMES", str(int((PRE_EVENT_S + 1) * CLIP_FPS))))

DEDUP_WINDOW_S:   float = 5.0
RETRY_INTERVAL_S: int   = 10


# ---------------------------------------------------------------------------
# StreamFrameBuffer — kept for test compatibility
# ---------------------------------------------------------------------------

class StreamFrameBuffer:
    def __init__(self, maxsize: int = BUFFER_FRAMES):
        self._buf: deque = deque(maxlen=maxsize)
        self._lock = threading.Lock()
        self.fps: float = 30.0

    def push(self, frame: np.ndarray, fps: float = 30.0) -> None:
        with self._lock:
            self._buf.append(frame)
            self.fps = fps

    def snapshot(self) -> list:
        with self._lock:
            return list(self._buf)

    def __len__(self) -> int:
        with self._lock:
            return len(self._buf)


# ---------------------------------------------------------------------------
# ClipWriter
# ---------------------------------------------------------------------------

class ClipWriter:
    def __init__(
        self,
        kafka_broker: str = KAFKA_BROKER,
        clips_dir: str    = CLIPS_DIR,
        buffer_frames: int = BUFFER_FRAMES,
        backend_url: str  = BACKEND_URL,
    ):
        self._kafka_broker  = kafka_broker
        self._clips_dir     = Path(clips_dir)
        self._buffer_frames = buffer_frames
        self._backend_url   = backend_url

        self._last_clip_ts: Dict[Tuple[int, str], float] = {}
        self._dedup_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._clips_dir.mkdir(parents=True, exist_ok=True)

    # ── Public ────────────────────────────────────────────────────────────────

    def run(self) -> None:
        while not self._stop_event.is_set():
            consumer = self._create_consumer()
            if consumer is None:
                self._stop_event.wait(timeout=RETRY_INTERVAL_S)
                continue
            try:
                consumer.subscribe(["ds.alerts"])
                logger.info("ClipWriter subscribed to ds.alerts (broker=%s)", self._kafka_broker)
                self._consume_loop(consumer)
            except Exception as exc:
                logger.error("ClipWriter Kafka error: %s — retrying in %ds", exc, RETRY_INTERVAL_S)
            finally:
                try:
                    consumer.close()
                except Exception:
                    pass
            if not self._stop_event.is_set():
                self._stop_event.wait(timeout=RETRY_INTERVAL_S)

    def stop(self) -> None:
        self._stop_event.set()

    def is_duplicate(self, stream_id: int, alert_type: str, now_s: Optional[float] = None) -> bool:
        if now_s is None:
            now_s = time.time()
        with self._dedup_lock:
            return (now_s - self._last_clip_ts.get((stream_id, alert_type), 0.0)) < DEDUP_WINDOW_S

    def record_clip_time(self, stream_id: int, alert_type: str, ts_s: float) -> None:
        with self._dedup_lock:
            self._last_clip_ts[(stream_id, alert_type)] = ts_s

    # ── Internal ──────────────────────────────────────────────────────────────

    def _create_consumer(self):
        try:
            from confluent_kafka import Consumer
        except ImportError:
            logger.warning("confluent_kafka not installed; ClipWriter disabled")
            self._stop_event.set()
            return None
        conf = {
            "bootstrap.servers": self._kafka_broker,
            "group.id": "clip-writer",
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        }
        try:
            c = Consumer(conf)
            c.list_topics(timeout=5)
            return c
        except Exception as exc:
            logger.error("ClipWriter: broker unreachable: %s", exc)
            return None

    def _consume_loop(self, consumer) -> None:
        from confluent_kafka import KafkaError
        while not self._stop_event.is_set():
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                err = msg.error()
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                raise Exception(str(err))
            try:
                data = json.loads(msg.value().decode("utf-8"))
            except Exception as exc:
                logger.warning("ClipWriter: bad message: %s", exc)
                continue
            self._handle_alert(data)

    def _handle_alert(self, msg: dict) -> None:
        payload      = msg.get("payload", {})
        stream_id    = int(payload.get("stream_id", 0))
        cam_id       = payload.get("cam_id", f"CAM_{stream_id:02d}")
        location     = payload.get("location", "")
        alert_type   = payload.get("alert_type", "unknown")
        triggered_ms = int(msg.get("timestamp_ms", int(time.time() * 1000)))

        now_s = time.time()
        with self._dedup_lock:
            last = self._last_clip_ts.get((stream_id, alert_type), 0.0)
            if now_s - last < DEDUP_WINDOW_S:
                return
            self._last_clip_ts[(stream_id, alert_type)] = now_s

        threading.Thread(
            target=self._capture_clip,
            args=(stream_id, cam_id, location, alert_type, triggered_ms),
            daemon=True,
        ).start()

    def _capture_clip(
        self,
        stream_id: int,
        cam_id: str,
        location: str,
        alert_type: str,
        triggered_at_ms: int,
    ) -> None:
        """
        Capture a clip with PRE_EVENT_S seconds before and POST_EVENT_S seconds after
        the alert event. Total clip duration = PRE_EVENT_S + POST_EVENT_S seconds.

        Timeline:
          [--- PRE_EVENT_S ---][event][--- POST_EVENT_S ---]
               (from buffer)          (wait & collect live)
        """
        def _fetch_frames(since_ts: float = 0.0) -> list:
            """Fetch frames from backend buffer, optionally only frames after since_ts."""
            try:
                resp = requests.get(
                    f"{self._backend_url}/internal/frames/{stream_id}",
                    timeout=5,
                )
                resp.raise_for_status()
                entries = resp.json()
            except Exception as exc:
                logger.error("ClipWriter: failed to fetch frames stream %d: %s", stream_id, exc)
                return []
            result = []
            for entry in entries:
                if entry.get("ts", 0) >= since_ts:
                    try:
                        raw   = base64.b64decode(entry["jpeg_b64"])
                        arr   = np.frombuffer(raw, dtype=np.uint8)
                        frame = cv2.imdecode(arr, cv2.IMREAD_COLOR)
                        if frame is not None:
                            result.append((frame, entry.get("ts", 0)))
                    except Exception:
                        continue
            return result

        event_ts = triggered_at_ms / 1000.0  # seconds

        # ── Step 1: Grab pre-event frames from rolling buffer ─────────────────
        pre_cutoff = event_ts - PRE_EVENT_S
        pre_frames = _fetch_frames(since_ts=pre_cutoff)

        if not pre_frames:
            logger.warning("ClipWriter: no pre-event frames for stream %d", stream_id)
            # Still continue — we'll at least capture post-event

        logger.info(
            "ClipWriter: stream %d — %d pre-event frames, waiting %.1fs for post-event",
            stream_id, len(pre_frames), POST_EVENT_S,
        )

        # ── Step 2: Wait POST_EVENT_S seconds to collect post-event frames ────
        # Poll every 0.2s so we get frames as they arrive
        post_frames: list = []
        poll_interval = 0.2
        deadline = time.time() + POST_EVENT_S
        last_seen_ts = event_ts  # only collect frames after the event

        while time.time() < deadline:
            time.sleep(poll_interval)
            new = _fetch_frames(since_ts=last_seen_ts)
            if new:
                post_frames.extend(new)
                last_seen_ts = new[-1][1]  # advance cursor to last frame ts

        logger.info(
            "ClipWriter: stream %d — %d post-event frames collected",
            stream_id, len(post_frames),
        )

        # ── Step 3: Combine pre + post frames ────────────────────────────────
        all_frame_tuples = pre_frames + post_frames
        if not all_frame_tuples:
            logger.warning("ClipWriter: no frames at all for stream %d — skipping", stream_id)
            return

        # Deduplicate by timestamp (pre and post may overlap slightly)
        seen_ts: set = set()
        frames = []
        for frame, ts in all_frame_tuples:
            if ts not in seen_ts:
                seen_ts.add(ts)
                frames.append(frame)

        logger.info(
            "ClipWriter: stream %d — writing %d total frames (%.1fs clip at %.0f FPS)",
            stream_id, len(frames), len(frames) / CLIP_FPS, CLIP_FPS,
        )

        # ── Step 4: Write video ───────────────────────────────────────────────
        ts_str    = datetime.fromtimestamp(triggered_at_ms / 1000, tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename  = f"{cam_id}_{alert_type}_{ts_str}.mp4"
        clip_path = self._clips_dir / filename
        tmp_path  = self._clips_dir / f"_tmp_{filename}"

        h, w = frames[0].shape[:2]
        fourcc = cv2.VideoWriter_fourcc(*"mp4v")
        writer = cv2.VideoWriter(str(tmp_path), fourcc, CLIP_FPS, (w, h))
        if not writer.isOpened():
            logger.error("ClipWriter: failed to open VideoWriter for stream %d", stream_id)
            return
        try:
            for frame in frames:
                writer.write(frame)
        finally:
            writer.release()

        # ── Step 5: Re-encode to H.264 via ffmpeg for browser compatibility ───
        ffmpeg_bin = shutil.which("ffmpeg") or "/usr/bin/ffmpeg"
        if os.path.isfile(ffmpeg_bin):
            try:
                result = subprocess.run(
                    [
                        ffmpeg_bin, "-y",
                        "-i", str(tmp_path),
                        "-c:v", "libx264",
                        "-preset", "fast",
                        "-crf", "23",
                        "-movflags", "+faststart",
                        "-loglevel", "error",
                        str(clip_path),
                    ],
                    capture_output=True, timeout=60,
                )
                if result.returncode == 0:
                    tmp_path.unlink(missing_ok=True)
                    logger.info("ClipWriter: re-encoded to H.264 → %s", clip_path)
                else:
                    shutil.move(str(tmp_path), str(clip_path))
                    logger.warning("ClipWriter: ffmpeg failed, using mp4v: %s",
                                   result.stderr.decode(errors="replace"))
            except Exception as exc:
                shutil.move(str(tmp_path), str(clip_path))
                logger.warning("ClipWriter: ffmpeg error (%s), using mp4v", exc)
        else:
            shutil.move(str(tmp_path), str(clip_path))
            logger.info("ClipWriter: ffmpeg not found, saved as mp4v → %s", clip_path)

        logger.info("ClipWriter: wrote %s (%d frames, %.1fs)",
                    clip_path, len(frames), len(frames) / CLIP_FPS)

        # ── Step 6: Register clip with backend ────────────────────────────────
        clip_url = f"{self._backend_url}/api/clips/{filename}"
        try:
            requests.post(
                f"{self._backend_url}/internal/clips",
                json={
                    "stream_id":       stream_id,
                    "cam_id":          cam_id,
                    "location":        location,
                    "alert_type":      alert_type,
                    "clip_path":       str(clip_path),
                    "clip_url":        clip_url,
                    "triggered_at_ms": triggered_at_ms,
                },
                timeout=5,
            ).raise_for_status()
            logger.info("ClipWriter: registered clip %s", filename)
        except Exception as exc:
            logger.error("ClipWriter: failed to register clip: %s", exc)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    writer = ClipWriter()
    logger.info(
        "ClipWriter starting (broker=%s, clips_dir=%s, backend=%s)",
        KAFKA_BROKER, CLIPS_DIR, BACKEND_URL,
    )
    writer.run()
