"""
Event deduplication and emission.
Converts raw detections into deduplicated alerts sent to the dashboard.
"""
import time
from collections import defaultdict
from .constants import MODEL_W, MODEL_H, FIRE_SMOKE_LABELS, C


class EventEmitter:
    """
    Deduplicates events and routes them to the ConsoleDashboard.
    Thread-safe for use from worker threads.
    """

    def __init__(self, dashboard, rule_engine, cooldown_sec: float = 3.0):
        self.dashboard    = dashboard
        self.rule_engine  = rule_engine
        self.cooldown     = cooldown_sec
        self._last_events = defaultdict(dict)  # {stream_id: {event_key: last_ts}}

    def _scale_det(self, det: dict, mux_w: int, mux_h: int) -> dict:
        """Convert detection from model space to mux frame space."""
        scale = min(MODEL_W / mux_w, MODEL_H / mux_h)
        pad_x = (MODEL_W - mux_w * scale) / 2.0
        pad_y = (MODEL_H - mux_h * scale) / 2.0
        x1 = max(0.0, min((det["x1"] - pad_x) / scale, mux_w))
        y1 = max(0.0, min((det["y1"] - pad_y) / scale, mux_h))
        x2 = max(0.0, min((det["x2"] - pad_x) / scale, mux_w))
        y2 = max(0.0, min((det["y2"] - pad_y) / scale, mux_h))
        w, h = x2 - x1, y2 - y1
        return {**det, "x1": x1, "y1": y1, "x2": x2, "y2": y2, "w": w, "h": h}

    def _can_emit(self, stream_id: int, key: str) -> bool:
        now = time.time()
        if now - self._last_events[stream_id].get(key, 0) >= self.cooldown:
            self._last_events[stream_id][key] = now
            return True
        return False

    def emit(self, dets: list, stream_id: int, model_key: str,
             mux_w: int, mux_h: int, crowd_monitor=None) -> None:
        """
        Process detections and emit deduplicated events.
        Safe to call from worker threads (no pyds/GLib calls).
        """
        stream_info = self.rule_engine.get_stream(stream_id)
        if stream_info is None:
            return
        cam_id   = stream_info.id
        location = stream_info.location

        scaled = [self._scale_det(d, mux_w, mux_h) for d in dets]
        scaled = [s for s in scaled if s["w"] > 0 and s["h"] > 0]
        if not scaled:
            return

        # ── Crowd density ─────────────────────────────────────────────────────
        if model_key == "coco" and crowd_monitor:
            thresholds = stream_info.crowd  # StreamConfig attribute, not dict.get()
            if "crowd_density" in stream_info.features(model_key) and thresholds:
                person_count = sum(1 for d in scaled if d["class_id"] == 0)
                if person_count > 0:
                    crowd_monitor.update(
                        stream_id, person_count, thresholds,
                        self.dashboard, cam_id, location
                    )

        # ── Fire / smoke events ───────────────────────────────────────────────
        if model_key == "fire_smoke":
            for det in scaled:
                key  = f"fire_{det['class_id']}"
                name = FIRE_SMOKE_LABELS.get(det["class_id"], "unknown")
                if self._can_emit(stream_id, key):
                    self.dashboard.push_event(
                        "fire" if det["class_id"] == 0 else "smoke",
                        stream_id, cam_id, location,
                        f"{name} conf={det['confidence']:.2f}"
                    )

        # ── Fall confirmed events ─────────────────────────────────────────────
        if model_key == "coco":
            for det in scaled:
                if det.get("fall_confirmed"):
                    key = f"fall_{stream_id}"
                    if self._can_emit(stream_id, key):
                        self.dashboard.push_event(
                            "fall", stream_id, cam_id, location,
                            f"FALL conf={det.get('fall_prob', 0):.2f}"
                        )

        # ── Dashboard stats ───────────────────────────────────────────────────
        zone = crowd_monitor.current_zone(stream_id) if crowd_monitor else "CLEAR"
        self.dashboard.update_stream(
            stream_id, len(scaled), 0.0,
            scaled[0].get("class_name", "") if scaled else "",
            zone if stream_id == 3 else None
        )
