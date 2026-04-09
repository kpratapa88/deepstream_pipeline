"""
Fall detection using YOLOv11-pose keypoints.
Computes torso angle from shoulder→hip vector.
Falls back to bbox aspect ratio when pose model unavailable.
"""
import math
import os
import numpy as np
from collections import deque, defaultdict

from .constants import FALL_WINDOW_SIZE, FALL_MIN_HITS

try:
    import onnxruntime as ort
    _ORT_AVAILABLE = True
except ImportError:
    _ORT_AVAILABLE = False


class FallDetector:
    """
    Two-stage fall detection:
    1. Primary: YOLOv11-pose keypoints → torso angle analysis
    2. Fallback: bounding box aspect ratio heuristic
    """

    # COCO keypoint indices
    KP_LEFT_SHOULDER  = 5
    KP_RIGHT_SHOULDER = 6
    KP_LEFT_HIP       = 11
    KP_RIGHT_HIP      = 12

    FALL_ANGLE_THRESHOLD = 40  # degrees from horizontal — below = fallen

    def __init__(self, onnx_path: str = "models/yolo11n-pose.onnx"):
        self.onnx_path  = onnx_path
        self.session    = None
        self._init_done = False

    def _lazy_init(self) -> None:
        if self._init_done:
            return
        self._init_done = True
        if not _ORT_AVAILABLE or not os.path.exists(self.onnx_path):
            print(f"[FallDetector] pose model not found: {self.onnx_path} — using aspect ratio fallback")
            return
        try:
            self.session    = ort.InferenceSession(self.onnx_path, providers=["CPUExecutionProvider"])
            self.input_name = self.session.get_inputs()[0].name
            print(f"[FallDetector] loaded {self.onnx_path} (CPUExecutionProvider)")
        except Exception as e:
            print(f"[FallDetector] failed to load: {e}")

    @property
    def available(self) -> bool:
        return _ORT_AVAILABLE and os.path.exists(self.onnx_path)

    def _torso_angle(self, kps: np.ndarray):
        """
        Compute torso angle from [17,3] keypoints.
        Returns angle in degrees (0=horizontal, 90=vertical), or None if insufficient confidence.
        """
        def get_kp(idx):
            x, y, c = kps[idx]
            return (x, y) if c > 0.3 else None

        ls, rs = get_kp(self.KP_LEFT_SHOULDER), get_kp(self.KP_RIGHT_SHOULDER)
        lh, rh = get_kp(self.KP_LEFT_HIP),      get_kp(self.KP_RIGHT_HIP)

        shoulder = ((ls[0]+rs[0])/2, (ls[1]+rs[1])/2) if ls and rs else (ls or rs)
        hip      = ((lh[0]+rh[0])/2, (lh[1]+rh[1])/2) if lh and rh else (lh or rh)

        if shoulder is None or hip is None:
            return None

        dx = shoulder[0] - hip[0]
        dy = shoulder[1] - hip[1]
        return abs(math.degrees(math.atan2(abs(dy), abs(dx))))

    def predict_from_keypoints(self, kps: np.ndarray):
        """
        Returns (fall_detected: bool, confidence: float) from [17,3] keypoints.
        """
        angle = self._torso_angle(kps)
        if angle is None:
            return False, 0.0
        fall = angle < self.FALL_ANGLE_THRESHOLD
        if fall:
            conf = max(0.0, min(1.0, (self.FALL_ANGLE_THRESHOLD - angle) / self.FALL_ANGLE_THRESHOLD))
        else:
            conf = max(0.0, min(1.0, (angle - self.FALL_ANGLE_THRESHOLD) / (90 - self.FALL_ANGLE_THRESHOLD)))
        return fall, round(conf, 3)

    def predict_from_bbox(self, bbox_w: float, bbox_h: float):
        """Fallback: aspect ratio heuristic (wide bbox = fallen)."""
        aspect = bbox_w / bbox_h if bbox_h > 0 else 0
        return aspect > 1.1, min(aspect / 2.0, 1.0)


class FallTemporalWindow:
    """
    Per-stream temporal window for fall confirmation.
    Tracks fall state per person (matched by bbox proximity).
    """

    def __init__(self):
        # {stream_id: {bbox_key: deque([0/1, ...])}}
        self._windows = defaultdict(dict)
        self._alerted = defaultdict(set)

    def update(self, stream_id: int, bbox_key: tuple, fall_detected: bool) -> bool:
        """
        Push a new observation. Returns True if fall is confirmed by window.
        """
        windows = self._windows[stream_id]
        if bbox_key not in windows:
            windows[bbox_key] = deque(maxlen=FALL_WINDOW_SIZE)
        windows[bbox_key].append(1 if fall_detected else 0)
        window = windows[bbox_key]
        return len(window) == FALL_WINDOW_SIZE and sum(window) >= FALL_MIN_HITS

    def should_alert(self, stream_id: int) -> bool:
        """True if stream has not been alerted recently (managed externally)."""
        return stream_id not in self._alerted

    def mark_alerted(self, stream_id: int) -> None:
        self._alerted[stream_id].add(stream_id)

    def clear_alert(self, stream_id: int) -> None:
        self._alerted[stream_id].discard(stream_id)

    def prune_stale(self, stream_id: int, active_keys: set) -> None:
        """Remove windows for persons no longer in frame."""
        stale = set(self._windows[stream_id].keys()) - active_keys
        for k in stale:
            del self._windows[stream_id][k]
