"""
Event deduplication and emission.
Converts raw detections into deduplicated alerts sent to the dashboard
and published to Kafka (when configured).
"""
import time
from collections import defaultdict
from .constants import MODEL_W, MODEL_H, FIRE_SMOKE_LABELS, C


class EventEmitter:
    """
    Deduplicates events and routes them to:
      - ConsoleDashboard (always)
      - KafkaEventProducer (when kafka_producer is provided)

    Thread-safe for use from worker threads.
    """

    def __init__(self, dashboard, rule_engine,
                 cooldown_sec: float = 3.0,
                 kafka_producer=None):
        self.dashboard      = dashboard
        self.rule_engine    = rule_engine
        self.cooldown       = cooldown_sec
        self.kafka          = kafka_producer   # KafkaEventProducer | None
        self._last_events   = defaultdict(dict)  # {stream_id: {event_key: last_ts}}

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
            thresholds = stream_info.crowd
            if "crowd_density" in stream_info.features(model_key) and thresholds:
                person_count = sum(1 for d in scaled if d["class_id"] == 0)
                if person_count > 0:
                    crowd_monitor.update(
                        stream_id, person_count, thresholds,
                        self.dashboard, cam_id, location
                    )

        # ── Publish all detections to Kafka ds.detections ─────────────────────
        if self.kafka:
            for det in scaled:
                if model_key == "fire_smoke":
                    class_name = FIRE_SMOKE_LABELS.get(det["class_id"], f"class_{det['class_id']}")
                else:
                    class_name = det.get("class_name", f"class_{det['class_id']}")
                self.kafka.publish_detection(
                    stream_id  = stream_id,
                    cam_id     = cam_id,
                    location   = location,
                    model      = model_key,
                    class_name = class_name,
                    confidence = det["confidence"],
                    bbox       = {
                        "x1": round(det["x1"], 2), "y1": round(det["y1"], 2),
                        "x2": round(det["x2"], 2), "y2": round(det["y2"], 2),
                        "w":  round(det["w"],  2),  "h":  round(det["h"],  2),
                    },
                )

        # ── Fire / smoke alerts ───────────────────────────────────────────────
        if model_key == "fire_smoke":
            for det in scaled:
                key  = f"fire_{det['class_id']}"
                name = FIRE_SMOKE_LABELS.get(det["class_id"], "unknown")
                alert_type = "fire" if det["class_id"] == 0 else "smoke"
                if self._can_emit(stream_id, key):
                    self.dashboard.push_event(
                        alert_type, stream_id, cam_id, location,
                        f"{name} conf={det['confidence']:.2f}"
                    )
                    if self.kafka:
                        self.kafka.publish_alert(
                            stream_id  = stream_id,
                            cam_id     = cam_id,
                            location   = location,
                            alert_type = alert_type,
                            confidence = det["confidence"],
                            details    = {"class_name": name},
                        )

        # ── Fall confirmed alerts ─────────────────────────────────────────────
        if model_key == "coco":
            for det in scaled:
                if det.get("fall_confirmed"):
                    key = f"fall_{stream_id}"
                    if self._can_emit(stream_id, key):
                        fall_prob = det.get("fall_prob", 0.0)
                        self.dashboard.push_event(
                            "fall", stream_id, cam_id, location,
                            f"FALL conf={fall_prob:.2f}"
                        )
                        if self.kafka:
                            self.kafka.publish_alert(
                                stream_id  = stream_id,
                                cam_id     = cam_id,
                                location   = location,
                                alert_type = "fall",
                                confidence = fall_prob,
                                details    = {
                                    "bbox": {
                                        "x1": round(det["x1"], 2),
                                        "y1": round(det["y1"], 2),
                                        "x2": round(det["x2"], 2),
                                        "y2": round(det["y2"], 2),
                                    }
                                },
                            )

        # ── Dashboard stats ───────────────────────────────────────────────────
        zone = crowd_monitor.current_zone(stream_id) if crowd_monitor else "CLEAR"
        self.dashboard.update_stream(
            stream_id, len(scaled), 0.0,
            scaled[0].get("class_name", "") if scaled else "",
            zone if stream_id == 3 else None
        )
