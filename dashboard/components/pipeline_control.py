"""
Screen 2 — Pipeline Control
Configure streams, models, detection classes, events, crowd thresholds.
Launch/stop pipeline via docker exec.
"""
import json
import os

import requests
import streamlit as st

from dashboard.config import BACKEND_URL

_SECTION = (
    "<div style='font-size:0.6rem;color:#aaa;letter-spacing:0.14em;"
    "text-transform:uppercase;margin-bottom:8px;margin-top:16px;'>{}</div>"
)

# Available models and their classes (mirrors rules.json models section)
_MODELS = {
    "coco": {
        "classes": ["person", "bird", "cat", "dog", "horse", "sheep", "cow",
                    "elephant", "bear", "zebra", "giraffe"],
        "features": ["fall_detection", "crowd_density"],
    },
    "fire_smoke": {
        "classes": ["fire", "smoke"],
        "features": [],
    },
    "combined": {
        "classes": ["cat", "chicken", "cow", "dog", "fire", "horse",
                    "people", "sheep", "smoke"],
        "features": [],
    },
}

_STATUS_COLOUR = {
    "running":  "#2e7d32",
    "starting": "#e65100",
    "stopped":  "#aaa",
    "error":    "#d32f2f",
}
_STATUS_ICON = {
    "running": "🟢", "starting": "🟡", "stopped": "⚫", "error": "🔴",
}


def _get_status() -> dict:
    try:
        r = requests.get(f"{BACKEND_URL}/api/pipeline/status", timeout=3)
        return r.json()
    except Exception:
        return {"status": "unknown", "last_error": ""}


def _post(path: str, payload: dict) -> dict | None:
    try:
        r = requests.post(f"{BACKEND_URL}{path}", json=payload, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"error": str(e)}


def render_pipeline_control() -> None:
    # ── Status bar ────────────────────────────────────────────────────────────
    status_data = _get_status()
    status      = status_data.get("status", "unknown")
    last_err    = status_data.get("last_error", "")
    s_col       = _STATUS_COLOUR.get(status, "#aaa")
    s_icon      = _STATUS_ICON.get(status, "⚪")

    sb1, sb2, sb3 = st.columns([3, 2, 2])
    with sb1:
        err_html = (f"<br><span style=\"font-size:0.7rem;color:#d32f2f;\">{last_err}</span>"
                    if last_err else "")
        st.markdown(
            f"<div style='background:#f8f9fb;border:1px solid #e8eaed;"
            f"border-radius:8px;padding:10px 16px;'>"
            f"<span style='font-size:0.7rem;color:#aaa;'>Pipeline Status</span><br>"
            f"<span style='font-size:1rem;font-weight:700;color:{s_col};'>"
            f"{s_icon} {status.upper()}</span>"
            f"{err_html}"
            f"</div>",
            unsafe_allow_html=True,
        )
    with sb2:
        if status in ("running", "starting"):
            if st.button("⏹ Stop Pipeline", type="secondary", use_container_width=True):
                res = _post("/api/pipeline/stop", {})
                if res and "error" not in res:
                    st.success("Stop signal sent.")
                else:
                    st.error(f"Error: {res}")
                st.rerun()
    with sb3:
        # Show pipeline log tail if error
        if status == "error" and st.button("📋 View Log", use_container_width=True):
            st.session_state["pc_show_log"] = True

    if st.session_state.get("pc_show_log"):
        try:
            r = requests.get(f"{BACKEND_URL}/api/pipeline/log", timeout=3)
            st.code(r.text[-3000:] if len(r.text) > 3000 else r.text)
        except Exception:
            st.caption("Log not available.")

    st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)
    st.markdown("<hr style='border-color:#eee;margin:8px 0 16px 0;'>",
                unsafe_allow_html=True)

    # ── Stream configuration ──────────────────────────────────────────────────
    st.markdown(_SECTION.format("Stream Configuration"), unsafe_allow_html=True)

    if "pc_streams" not in st.session_state:
        st.session_state["pc_streams"] = [_default_stream(0)]

    streams_cfg = st.session_state["pc_streams"]

    for i, _ in enumerate(streams_cfg):
        _render_stream_row(i)

    ca, cb = st.columns([1, 5])
    with ca:
        if st.button("➕ Add Stream", key="pc_add"):
            st.session_state["pc_streams"].append(_default_stream(len(streams_cfg)))
            st.rerun()

    # ── Global settings ───────────────────────────────────────────────────────
    st.markdown(_SECTION.format("Global Settings"), unsafe_allow_html=True)

    g1, g2, g3 = st.columns(3)
    with g1:
        interval = st.number_input(
            "Inference Interval",
            min_value=0, max_value=10, value=2, step=1,
            key="pc_interval",
            help="0=every frame, 2=every 3rd frame (default), 4=every 5th",
        )
    with g2:
        kafka = st.text_input("Kafka Broker", value="localhost:9092",
                              key="pc_kafka")
    with g3:
        container = st.text_input("Docker Container", value="ds-pipeline",
                                  key="pc_container",
                                  help="Container name for docker exec")

    # ── Launch ────────────────────────────────────────────────────────────────
    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    if st.button("▶ Start Pipeline", type="primary", use_container_width=False):
        _launch(interval, kafka, container)


def _default_stream(idx: int) -> dict:
    return {
        "video_path": "",
        "model": "coco",
        "detect": ["person"],
        "features": [],
        "crowd": {"low": 3, "medium": 8, "high": 15, "critical": 25},
        "location": f"Stream_{idx}",
        "cam_id": f"CAM_{idx+1:02d}",
    }


def _render_stream_row(i: int) -> None:
    streams = st.session_state["pc_streams"]
    cfg     = streams[i]

    with st.expander(f"Stream {i+1}  —  {cfg.get('cam_id','')}  ·  {cfg.get('location','')}", expanded=True):
        r1c1, r1c2, r1c3 = st.columns([2, 2, 2])

        with r1c1:
            cfg["cam_id"] = st.text_input("Camera ID", value=cfg["cam_id"],
                                          key=f"pc_camid_{i}")
        with r1c2:
            cfg["location"] = st.text_input("Location", value=cfg["location"],
                                            key=f"pc_loc_{i}")
        with r1c3:
            uploaded = st.file_uploader("Video file (MP4/AVI)",
                                        type=["mp4", "avi"],
                                        key=f"pc_video_{i}")
            if uploaded:
                save_dir = os.environ.get("UPLOAD_DIR", "videos/uploads")
                os.makedirs(save_dir, exist_ok=True)
                dest = os.path.join(save_dir, uploaded.name)
                with open(dest, "wb") as f:
                    f.write(uploaded.getbuffer())
                cfg["video_path"] = dest
                st.caption(f"✅ {uploaded.name}")
            elif cfg["video_path"]:
                st.caption(f"📁 {os.path.basename(cfg['video_path'])}")

        # RTSP alternative
        rtsp = st.text_input("Or RTSP URL", value="" if cfg["video_path"].startswith("videos") else cfg["video_path"],
                             placeholder="rtsp://...", key=f"pc_rtsp_{i}")
        if rtsp.strip().startswith("rtsp://"):
            cfg["video_path"] = rtsp.strip()

        # Model + classes
        r2c1, r2c2 = st.columns([1, 3])
        with r2c1:
            model = st.selectbox("Model", list(_MODELS.keys()),
                                 index=list(_MODELS.keys()).index(cfg["model"]),
                                 key=f"pc_model_{i}")
            cfg["model"] = model

        with r2c2:
            available_classes = _MODELS[model]["classes"]
            st.markdown("<span style='font-size:0.72rem;color:#888;'>Detect classes</span>",
                        unsafe_allow_html=True)
            cls_cols = st.columns(min(6, len(available_classes)))
            selected = []
            for ci, cls in enumerate(available_classes):
                with cls_cols[ci % len(cls_cols)]:
                    checked = st.checkbox(cls, value=cls in cfg["detect"],
                                          key=f"pc_cls_{i}_{cls}")
                    if checked:
                        selected.append(cls)
            cfg["detect"] = selected

        # Features
        available_features = _MODELS[model]["features"]
        if available_features:
            st.markdown("<span style='font-size:0.72rem;color:#888;'>Features</span>",
                        unsafe_allow_html=True)
            feat_cols = st.columns(len(available_features))
            sel_feats = []
            for fi, feat in enumerate(available_features):
                with feat_cols[fi]:
                    if st.checkbox(feat.replace("_", " ").title(),
                                   value=feat in cfg["features"],
                                   key=f"pc_feat_{i}_{feat}"):
                        sel_feats.append(feat)
            cfg["features"] = sel_feats

            # Crowd thresholds — per stream
            if "crowd_density" in cfg["features"]:
                st.markdown("<span style='font-size:0.72rem;color:#888;'>Crowd Thresholds (persons)</span>",
                            unsafe_allow_html=True)
                tc1, tc2, tc3, tc4 = st.columns(4)
                crowd = cfg.get("crowd", {"low": 3, "medium": 8, "high": 15, "critical": 25})
                with tc1:
                    crowd["low"]      = st.number_input("Low",      1, 100, crowd["low"],      key=f"pc_cl_{i}")
                with tc2:
                    crowd["medium"]   = st.number_input("Medium",   1, 100, crowd["medium"],   key=f"pc_cm_{i}")
                with tc3:
                    crowd["high"]     = st.number_input("High",     1, 200, crowd["high"],     key=f"pc_ch_{i}")
                with tc4:
                    crowd["critical"] = st.number_input("Critical", 1, 500, crowd["critical"], key=f"pc_cc_{i}")
                cfg["crowd"] = crowd

        # Remove stream button
        if len(st.session_state["pc_streams"]) > 1:
            if st.button(f"🗑 Remove Stream {i+1}", key=f"pc_remove_{i}"):
                st.session_state["pc_streams"].pop(i)
                st.rerun()

    st.session_state["pc_streams"][i] = cfg


def _launch(interval: int, kafka: str, container: str) -> None:
    streams = st.session_state.get("pc_streams", [])

    # Validate
    errors = []
    for i, s in enumerate(streams):
        if not s.get("video_path"):
            errors.append(f"Stream {i+1}: no video file or RTSP URL")
        if not s.get("detect"):
            errors.append(f"Stream {i+1}: no detection classes selected")

    if errors:
        for e in errors:
            st.error(e)
        return

    # Build rules.json
    stream_configs = {}
    for i, s in enumerate(streams):
        model_entry = {
            "name":    s["model"],
            "detect":  s["detect"],
        }
        if s.get("features"):
            model_entry["features"] = s["features"]

        entry = {
            "id":          s["cam_id"],
            "location":    s["location"],
            "models":      [model_entry],
            "description": f"{s['location']} — {s['model']}",
        }
        if "crowd_density" in s.get("features", []):
            entry["crowd"] = s.get("crowd", {"low": 3, "medium": 8, "high": 15, "critical": 25})

        stream_configs[str(i)] = entry

    # Build stream list for API
    api_streams = [
        {
            "video_path": s["video_path"],
            "model":      s["model"],
            "detect":     s["detect"],
            "features":   s.get("features", []),
        }
        for s in streams
    ]

    payload = {
        "streams":            api_streams,
        "inference_interval": int(interval),
        "kafka_broker":       kafka,
    }

    # Update container name in env so the launcher picks it up
    os.environ["PIPELINE_CONTAINER"] = container

    with st.spinner("Starting pipeline…"):
        res = _post("/api/pipeline/start", payload)

    if res and "error" not in res:
        st.success(f"Pipeline started — status: {res.get('status', 'unknown')}")
        st.rerun()
    else:
        st.error(f"Failed to start: {res}")
