"""
Detection utilities: NMS, YOLO tensor parsing, OSD meta injection, pose keypoint drawing.
"""
import ctypes
import math
import numpy as np

from .constants import (
    MODEL_W, MODEL_H, FIRE_SMOKE_LABELS, FIRE_SMOKE_CLASSES,
    GIE_COCO, GIE_FIRE, GIE_POSE, C
)


# ── NMS ───────────────────────────────────────────────────────────────────────

def nms(boxes: np.ndarray, scores: np.ndarray, iou_threshold: float = 0.45):
    if len(boxes) == 0:
        return []
    x1, y1, x2, y2 = boxes[:, 0], boxes[:, 1], boxes[:, 2], boxes[:, 3]
    areas = (x2 - x1) * (y2 - y1)
    order = scores.argsort()[::-1]
    keep  = []
    while order.size > 0:
        i = order[0]
        keep.append(i)
        xx1   = np.maximum(x1[i], x1[order[1:]])
        yy1   = np.maximum(y1[i], y1[order[1:]])
        xx2   = np.minimum(x2[i], x2[order[1:]])
        yy2   = np.minimum(y2[i], y2[order[1:]])
        inter = np.maximum(0, xx2 - xx1) * np.maximum(0, yy2 - yy1)
        ovr   = inter / (areas[i] + areas[order[1:]] - inter + 1e-6)
        order = order[np.where(ovr <= iou_threshold)[0] + 1]
    return keep


# ── YOLO tensor parser ────────────────────────────────────────────────────────

def parse_yolo_tensor(tensor: np.ndarray, conf_thresh: float = 0.25,
                      allowed_classes=None, model_key: str = "coco") -> list:
    """
    Parse raw YOLO output tensor [rows, 8400].
    Returns list of detection dicts with model-space coordinates.
    """
    data = tensor.T                          # [8400, rows]
    cx, cy, w, h = data[:, 0], data[:, 1], data[:, 2], data[:, 3]
    cls_scores = data[:, 4:]

    if cls_scores.max() > 1.0 or cls_scores.min() < 0.0:
        cls_scores = 1.0 / (1.0 + np.exp(-np.clip(cls_scores, -20, 20)))

    max_scores = cls_scores.max(axis=1)
    class_ids  = cls_scores.argmax(axis=1)

    mask = max_scores > conf_thresh
    if allowed_classes:
        mask &= np.isin(class_ids, list(allowed_classes))
    if not mask.any():
        return []

    cx, cy, w, h = cx[mask], cy[mask], w[mask], h[mask]
    scores       = max_scores[mask]
    cids         = class_ids[mask]
    x1, y1       = cx - w / 2, cy - h / 2
    x2, y2       = cx + w / 2, cy + h / 2

    keep = nms(np.stack([x1, y1, x2, y2], axis=1), scores)
    return [
        {
            "class_id":   int(cids[i]),
            "confidence": float(scores[i]),
            "x1": float(x1[i]), "y1": float(y1[i]),
            "x2": float(x2[i]), "y2": float(y2[i]),
        }
        for i in keep
    ]


# ── Tensor extraction from DeepStream metadata ────────────────────────────────

def extract_tensor(frame_meta, gie_id: int, num_rows: int):
    """
    Extract and copy raw tensor from NvDsInferTensorMeta for a given GIE.
    Returns numpy array [num_rows, 8400] or None.
    """
    import pyds
    size = num_rows * 8400
    try:
        l_user = frame_meta.frame_user_meta_list
        while l_user is not None:
            try:
                user_meta = pyds.NvDsUserMeta.cast(l_user.data)
            except StopIteration:
                break
            if user_meta.base_meta.meta_type == pyds.NvDsMetaType.NVDSINFER_TENSOR_OUTPUT_META:
                tm = pyds.NvDsInferTensorMeta.cast(user_meta.user_meta_data)
                if tm.unique_id != gie_id:
                    l_user = l_user.next
                    continue
                layer = pyds.get_nvds_LayerInfo(tm, 0)
                ptr   = ctypes.cast(
                    pyds.get_ptr(layer.buffer), ctypes.POINTER(ctypes.c_float)
                )
                arr = np.ctypeslib.as_array(ptr, shape=(size,)).copy()
                return arr.reshape(num_rows, 8400)
            l_user = l_user.next
    except Exception as e:
        print(f"[extract_tensor] gie_id={gie_id} num_rows={num_rows} error: {e}")
    return None


def extract_pose_tensor(frame_meta):
    """Extract pose tensor from GIE_4. Returns [N, 56] filtered by confidence, or None."""
    raw = extract_tensor(frame_meta, GIE_POSE, 56)
    if raw is None:
        return None
    data = raw.T          # [8400, 56]
    mask = data[:, 4] > 0.3
    return data[mask] if mask.any() else None


# ── OSD meta injection ────────────────────────────────────────────────────────

def add_obj_meta(batch_meta, frame_meta, det: dict, stream_id: int,
                 model_key: str, label_fn, mux_w: int, mux_h: int,
                 fall_active: bool = False) -> None:
    """
    Convert a detection dict to NvDsObjectMeta and add it to the frame.
    Handles coordinate de-letterboxing from model space to mux frame space.
    """
    import pyds

    scale = min(MODEL_W / mux_w, MODEL_H / mux_h)
    pad_x = (MODEL_W - mux_w * scale) / 2.0
    pad_y = (MODEL_H - mux_h * scale) / 2.0

    x1 = max(0.0, min((det["x1"] - pad_x) / scale, mux_w))
    y1 = max(0.0, min((det["y1"] - pad_y) / scale, mux_h))
    x2 = max(0.0, min((det["x2"] - pad_x) / scale, mux_w))
    y2 = max(0.0, min((det["y2"] - pad_y) / scale, mux_h))
    w, h = x2 - x1, y2 - y1
    if w <= 0 or h <= 0:
        return

    class_id   = det["class_id"]
    confidence = det["confidence"]

    # Resolve class name: prefer rule-engine label map injected into det,
    # then fall back to model-specific logic, then label_fn (COCO labels.txt)
    if "class_name" in det and det["class_name"]:
        class_name = det["class_name"]
    elif model_key == "fire_smoke":
        class_name = FIRE_SMOKE_LABELS.get(class_id, f"class_{class_id}")
    else:
        class_name = label_fn(class_id)

    fall_detected  = det.get("fall_detected", False)
    fall_confirmed = det.get("fall_confirmed", False)
    fall_prob      = det.get("fall_prob", 0.0)

    obj_meta = pyds.nvds_acquire_obj_meta_from_pool(batch_meta)
    obj_meta.class_id            = class_id
    obj_meta.confidence          = confidence
    obj_meta.unique_component_id = GIE_COCO
    obj_meta.object_id           = 0xFFFFFFFFFFFFFFFF

    obj_meta.rect_params.left         = x1
    obj_meta.rect_params.top          = y1
    obj_meta.rect_params.width        = w
    obj_meta.rect_params.height       = h
    obj_meta.rect_params.border_width = 3

    # ── Border color ──────────────────────────────────────────────────────────
    if model_key == "fire_smoke":
        if class_id == 0:
            obj_meta.rect_params.border_color.set(1.0, 0.4, 0.0, 1.0)  # orange — fire
        else:
            obj_meta.rect_params.border_color.set(0.7, 0.7, 0.7, 1.0)  # gray — smoke
        label = f"{class_name} {confidence:.2f}"
    elif model_key == "combined":
        # fire=4, smoke=8 get warning colors; animals/people get cyan
        if class_id == 4:   # fire
            obj_meta.rect_params.border_color.set(1.0, 0.4, 0.0, 1.0)  # orange
        elif class_id == 8: # smoke
            obj_meta.rect_params.border_color.set(0.7, 0.7, 0.7, 1.0)  # gray
        elif class_id == 6: # people
            obj_meta.rect_params.border_color.set(0.0, 1.0, 0.0, 1.0)  # green
        else:               # animals
            obj_meta.rect_params.border_color.set(0.0, 0.8, 1.0, 1.0)  # cyan
        label = f"{class_name} {confidence:.2f}"
    elif class_id == 0 and fall_active:
        if fall_confirmed:
            obj_meta.rect_params.border_color.set(1.0, 0.0, 0.0, 1.0)  # red
            obj_meta.rect_params.border_width = 5
            label = f"person {confidence:.2f} | FALL {fall_prob:.2f}"
        elif fall_detected:
            obj_meta.rect_params.border_color.set(1.0, 1.0, 0.0, 1.0)  # yellow
            label = f"person {confidence:.2f} | fall? {fall_prob:.2f}"
        else:
            obj_meta.rect_params.border_color.set(0.0, 1.0, 0.0, 1.0)  # green
            label = f"person {confidence:.2f} | no_fall {1 - fall_prob:.2f}"
    elif class_id == 0:
        obj_meta.rect_params.border_color.set(0.0, 1.0, 0.0, 1.0)
        label = f"{class_name} {confidence:.2f}"
    else:
        obj_meta.rect_params.border_color.set(1.0, 1.0, 0.0, 1.0)
        label = f"{class_name} {confidence:.2f}"

    obj_meta.text_params.display_text = label
    obj_meta.text_params.x_offset     = int(x1)
    obj_meta.text_params.y_offset     = max(0, int(y1) - 16)
    obj_meta.text_params.font_params.font_name = "Serif"
    obj_meta.text_params.font_params.font_size = 8
    obj_meta.text_params.font_params.font_color.set(1.0, 1.0, 1.0, 1.0)
    obj_meta.text_params.set_bg_clr = 1
    obj_meta.text_params.text_bg_clr.set(0.0, 0.0, 0.0, 0.8)

    pyds.nvds_add_obj_meta_to_frame(frame_meta, obj_meta, None)

    # ── Pose keypoints ────────────────────────────────────────────────────────
    if class_id == 0 and det.get("pose_kps") is not None:
        draw_pose_keypoints(
            batch_meta, frame_meta, det["pose_kps"],
            mux_w, mux_h,
            fall_detected=fall_detected,
            fall_confirmed=fall_confirmed,
        )


# ── Pose keypoint OSD ─────────────────────────────────────────────────────────

_SKELETON = [
    (5, 6), (5, 7), (7, 9), (6, 8), (8, 10),
    (5, 11), (6, 12), (11, 12),
    (11, 13), (13, 15), (12, 14), (14, 16),
]


def draw_pose_keypoints(batch_meta, frame_meta, kps: np.ndarray,
                        mux_w: int, mux_h: int,
                        fall_detected: bool = False,
                        fall_confirmed: bool = False) -> None:
    """
    Draw pose skeleton on OSD.
    cyan=standing, yellow=fall detected, red=fall confirmed.
    """
    import pyds

    scale = min(MODEL_W / mux_w, MODEL_H / mux_h)
    pad_x = (MODEL_W - mux_w * scale) / 2.0
    pad_y = (MODEL_H - mux_h * scale) / 2.0

    def to_frame(kx, ky):
        return (
            int(max(0, min((kx - pad_x) / scale, mux_w))),
            int(max(0, min((ky - pad_y) / scale, mux_h))),
        )

    if fall_confirmed:
        r, g, b = 1.0, 0.0, 0.0   # red
    elif fall_detected:
        r, g, b = 1.0, 1.0, 0.0   # yellow
    else:
        r, g, b = 0.0, 1.0, 1.0   # cyan

    try:
        dm = pyds.nvds_acquire_display_meta_from_pool(batch_meta)
        dm.num_circles = 0
        dm.num_lines   = 0

        for kx, ky, kc in kps:
            if kc < 0.3 or dm.num_circles >= 18:
                continue
            fx, fy = to_frame(kx, ky)
            c = dm.circle_params[dm.num_circles]
            c.xc = fx; c.yc = fy; c.radius = 3
            c.circle_color.set(r, g, b, 1.0)
            c.has_bg_color = 0
            dm.num_circles += 1

        for i, j in _SKELETON:
            kx1, ky1, kc1 = kps[i]
            kx2, ky2, kc2 = kps[j]
            if kc1 < 0.3 or kc2 < 0.3 or dm.num_lines >= 20:
                continue
            fx1, fy1 = to_frame(kx1, ky1)
            fx2, fy2 = to_frame(kx2, ky2)
            ln = dm.line_params[dm.num_lines]
            ln.x1 = fx1; ln.y1 = fy1
            ln.x2 = fx2; ln.y2 = fy2
            ln.line_width = 2
            ln.line_color.set(r, g, b, 0.8)
            dm.num_lines += 1

        if dm.num_circles > 0 or dm.num_lines > 0:
            pyds.nvds_add_display_meta_to_frame(frame_meta, dm)
    except Exception:
        pass
