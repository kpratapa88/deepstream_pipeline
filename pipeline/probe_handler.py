"""
GStreamer probe callbacks.
Each probe handles one model's tensor output and injects OSD for all streams
that subscribed to that model.
"""
import time
import numpy as np
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor

from .constants import GIE_COCO, GIE_FIRE, GIE_POSE, FALL_WINDOW_SIZE, FALL_MIN_HITS
from .detection import extract_tensor, extract_pose_tensor, parse_yolo_tensor, add_obj_meta
from .fall_detector import FallDetector
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .frame_publisher import FramePublisher


class ProbeHandler:
    """
    Manages GStreamer pad probe logic for all models.
    Each model probe filters detections per stream based on rules.json subscriptions.
    """

    def __init__(self, rule_engine, label_fn, fall_detector: FallDetector,
                 crowd_monitor, event_emitter, dashboard,
                 mux_w: int, mux_h: int, bm: dict,
                 frame_publisher: "FramePublisher | None" = None):
        self.rule_engine      = rule_engine
        self.label_fn         = label_fn
        self.fall_detector    = fall_detector
        self.crowd_monitor    = crowd_monitor
        self.event_emitter    = event_emitter
        self.dashboard        = dashboard
        self.mux_w            = mux_w
        self.mux_h            = mux_h
        self._bm              = bm
        self._frame_publisher = frame_publisher

        # Per-stream detection cache: {(stream_id, model_name): [dets]}
        self._last_dets    = defaultdict(list)
        # Per-stream fall windows: {stream_id: {bbox_key: deque}}
        self._fall_windows = defaultdict(dict)
        # Frame counter for Kafka detection throttle
        self._frame_count  = defaultdict(int)
        # Per-stream raw frame counter for snapshot publishing (every 30th frame)
        self._snapshot_count: dict[int, int] = defaultdict(int)

        # Async executor for event emission (console + Kafka alerts)
        # 4 workers: enough headroom without competing with GStreamer threads
        self._executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="events")

    def make_probe(self, model_name: str):
        """
        Return a GStreamer pad probe for the given model.
        Processes all streams that subscribed to this model.
        """
        model_def = self.rule_engine.model_registry.get(model_name, {})
        gie_id    = model_def.get("gie_id", GIE_COCO)

        # Always use the full tensor row count for the model, not just the
        # subset of classes configured in rules.json. The tensor always contains
        # all classes — we filter AFTER parsing via allowed_classes.
        _FULL_CLASS_COUNTS = {"coco": 80, "fire_smoke": 2, "pose": 51, "combined": 9}
        num_model_classes = _FULL_CLASS_COUNTS.get(model_name, 80)

        def probe(pad, info, u_data):
            import pyds
            from gi.repository import Gst

            gst_buffer = info.get_buffer()
            if not gst_buffer:
                return Gst.PadProbeReturn.OK
            batch_meta = pyds.gst_buffer_get_nvds_batch_meta(hash(gst_buffer))
            if not batch_meta:
                return Gst.PadProbeReturn.OK

            l_frame = batch_meta.frame_meta_list
            while l_frame is not None:
                try:
                    frame_meta = pyds.NvDsFrameMeta.cast(l_frame.data)
                except StopIteration:
                    break

                stream_id  = frame_meta.pad_index
                stream_cfg = self.rule_engine.get_stream(stream_id)

                # Skip if this stream doesn't subscribe to this model
                if stream_cfg is None or model_name not in stream_cfg.active_models:
                    l_frame = l_frame.next
                    continue

                # Count frames once per stream (not once per model per stream)
                # Use a per-model counter key so each probe counts independently
                self._bm[stream_id]["frames"] += 1

                sub         = stream_cfg.get_subscription(model_name)
                allowed     = sub["class_ids"]
                conf_thresh = min(sub["thresholds"].values()) if sub["thresholds"] else 0.25
                num_rows    = 4 + num_model_classes

                # ── Extract & parse tensor ────────────────────────────────────
                t0     = time.perf_counter()
                tensor = extract_tensor(frame_meta, gie_id, num_rows)
                if tensor is not None:
                    dets     = parse_yolo_tensor(tensor, conf_thresh, allowed, model_name)
                    infer_ms = (time.perf_counter() - t0) * 1000
                    self._bm[stream_id]["infer_ms_total"] += infer_ms
                    self._bm[stream_id]["infer_calls"]    += 1
                    self._bm[stream_id]["detections"]     += len(dets)

                    # Inject class names from rule engine so OSD shows correct
                    # labels for any model (combined, fire_smoke, coco, etc.)
                    model_reg   = self.rule_engine.model_registry.get(model_name, {})
                    id_to_name  = {
                        v["class_id"]: k
                        for k, v in model_reg.get("classes", {}).items()
                    }
                    for d in dets:
                        if d["class_id"] in id_to_name:
                            d["class_name"] = id_to_name[d["class_id"]]

                    # ── Fall detection ────────────────────────────────────────
                    if model_name == "coco" and stream_cfg.has_feature("fall_detection"):
                        pose_data = extract_pose_tensor(frame_meta)
                        dets = self._apply_fall(dets, pose_data, stream_id)

                    cache_key = (stream_id, model_name)
                    self._last_dets[cache_key] = dets

                    if dets:
                        # Throttle executor submissions — emit every 3rd frame.
                        # Alert deduplication inside emit() ensures alerts still
                        # fire on time regardless of this throttle.
                        self._frame_count[(stream_id, model_name)] += 1
                        if self._frame_count[(stream_id, model_name)] % 3 == 0:
                            self._executor.submit(
                                self.event_emitter.emit,
                                list(dets), stream_id, model_name,
                                self.mux_w, self.mux_h, self.crowd_monitor
                            )

                # ── Inject OSD from cache every frame ─────────────────────────
                cache_key   = (stream_id, model_name)
                fall_active = stream_cfg.has_feature("fall_detection") and model_name == "coco"
                for det in self._last_dets.get(cache_key, []):
                    add_obj_meta(
                        batch_meta, frame_meta, det, stream_id, model_name,
                        self.label_fn, self.mux_w, self.mux_h, fall_active
                    )

                # ── Crowd OSD ─────────────────────────────────────────────────
                if model_name == "coco" and stream_cfg.has_feature("crowd_density"):
                    self.crowd_monitor.draw_osd(frame_meta, batch_meta, stream_id)

                l_frame = l_frame.next
            return Gst.PadProbeReturn.OK
        return probe

    def _apply_fall(self, dets: list, pose_data, stream_id: int) -> list:
        updated = []
        for det in dets:
            if det["class_id"] != 0:
                updated.append(det)
                continue

            det      = dict(det)
            bw       = det["x2"] - det["x1"]
            bh       = det["y2"] - det["y1"]
            best_kps = None

            if pose_data is not None:
                pcx   = (det["x1"] + det["x2"]) / 2
                pcy   = (det["y1"] + det["y2"]) / 2
                dists = np.sqrt((pose_data[:, 0] - pcx)**2 + (pose_data[:, 1] - pcy)**2)
                if dists.min() < 80:
                    best_kps = pose_data[dists.argmin()][5:].reshape(17, 3)
                    fall_detected, fall_prob = self.fall_detector.predict_from_keypoints(best_kps)
                else:
                    fall_detected, fall_prob = self.fall_detector.predict_from_bbox(bw, bh)
            else:
                fall_detected, fall_prob = self.fall_detector.predict_from_bbox(bw, bh)

            bbox_key = (stream_id, round(det["x1"], -1), round(det["y1"], -1))
            windows  = self._fall_windows[stream_id]
            if bbox_key not in windows:
                windows[bbox_key] = deque(maxlen=FALL_WINDOW_SIZE)
            windows[bbox_key].append(1 if fall_detected else 0)
            window = windows[bbox_key]
            fall_confirmed = len(window) == FALL_WINDOW_SIZE and sum(window) >= FALL_MIN_HITS

            det.update({
                "fall_detected":  fall_detected,
                "fall_prob":      fall_prob,
                "fall_confirmed": fall_confirmed,
                "pose_kps":       best_kps,
            })
            updated.append(det)
        return updated

    def shutdown(self) -> None:
        self._executor.shutdown(wait=False)
