"""
Pipeline control form component for the Streamlit dashboard.

Renders a form for configuring and launching the DeepStream pipeline:
- Dynamic model and class dropdowns populated from GET /api/models
- Per-stream rows: video upload, model select, class checkboxes, feature toggles
- "Add Stream" button to append new stream rows
- "Start Pipeline" button to submit config via POST /api/pipeline/start
- Current pipeline status display with "Stop Pipeline" button

Requirements: 9.1, 9.2, 9.8, 9.9, 9.10
"""
from typing import Optional

import streamlit as st

from dashboard.api_client import (
    get_models,
    get_pipeline_status,
    start_pipeline,
    stop_pipeline,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_AVAILABLE_FEATURES = ["fall_detection", "crowd_density"]

_STATUS_COLOURS = {
    "running":  "🟢",
    "starting": "🟡",
    "stopped":  "🔴",
    "error":    "🔴",
}

_DEFAULT_INFERENCE_INTERVAL = 4


# ---------------------------------------------------------------------------
# Session state helpers
# ---------------------------------------------------------------------------

def _init_state() -> None:
    """Initialise session state keys used by this component."""
    if "pf_stream_count" not in st.session_state:
        st.session_state["pf_stream_count"] = 1
    if "pf_uploaded_files" not in st.session_state:
        st.session_state["pf_uploaded_files"] = {}


def _add_stream() -> None:
    st.session_state["pf_stream_count"] += 1


def _remove_stream(idx: int) -> None:
    count = st.session_state["pf_stream_count"]
    if count > 1:
        st.session_state["pf_stream_count"] = count - 1
        # Shift uploaded file state down
        uploads = st.session_state.get("pf_uploaded_files", {})
        for i in range(idx, count - 1):
            uploads[i] = uploads.get(i + 1)
        uploads.pop(count - 1, None)
        st.session_state["pf_uploaded_files"] = uploads


# ---------------------------------------------------------------------------
# Status display
# ---------------------------------------------------------------------------

def _render_status_bar(status_data: Optional[dict]) -> None:
    """Show pipeline status and Stop button (Requirement 9.8)."""
    if status_data is None:
        st.warning("Could not reach backend — pipeline status unavailable.")
        return

    status = status_data.get("status", "stopped")
    last_error = status_data.get("last_error", "")
    icon = _STATUS_COLOURS.get(status, "⚪")

    col_status, col_stop = st.columns([3, 1])
    with col_status:
        st.markdown(f"**Pipeline status:** {icon} `{status}`")
        if status == "error" and last_error:
            st.error(f"Last error: {last_error}")

    with col_stop:
        if status in ("running", "starting"):
            if st.button("⏹ Stop Pipeline", type="secondary", width="stretch"):
                result = stop_pipeline()
                if result is not None:
                    st.success("Stop signal sent.")
                else:
                    st.error("Failed to reach backend.")
                st.rerun()


# ---------------------------------------------------------------------------
# Stream row
# ---------------------------------------------------------------------------

def _render_stream_row(
    idx: int,
    models_data: dict,
    model_names: list[str],
    stream_count: int,
) -> Optional[dict]:
    """Render one stream configuration row and return its config dict, or None."""
    with st.expander(f"Stream {idx + 1}", expanded=True):
        # Video upload (Requirement 9.1, 9.9)
        uploaded = st.file_uploader(
            "Upload video (MP4 / AVI)",
            type=["mp4", "avi"],
            key=f"pf_video_{idx}",
            label_visibility="visible",
        )

        if not model_names:
            st.warning("No models available — check backend connection.")
            return None

        # Model select (Requirement 9.10)
        model = st.selectbox(
            "Detection model",
            options=model_names,
            key=f"pf_model_{idx}",
        )

        # Class checkboxes — populated from selected model's classes (Req 9.10)
        available_classes: list[str] = []
        if model and model in models_data:
            available_classes = models_data[model].get("classes", [])

        selected_classes: list[str] = []
        if available_classes:
            st.markdown("**Detect classes:**")
            class_cols = st.columns(min(4, len(available_classes)))
            for ci, cls in enumerate(available_classes):
                with class_cols[ci % len(class_cols)]:
                    checked = st.checkbox(cls, value=True, key=f"pf_cls_{idx}_{cls}")
                    if checked:
                        selected_classes.append(cls)
        else:
            st.caption("No detectable classes for this model.")

        # Feature toggles
        st.markdown("**Optional features:**")
        feat_cols = st.columns(len(_AVAILABLE_FEATURES))
        selected_features: list[str] = []
        for fi, feat in enumerate(_AVAILABLE_FEATURES):
            with feat_cols[fi]:
                if st.checkbox(feat.replace("_", " ").title(), key=f"pf_feat_{idx}_{feat}"):
                    selected_features.append(feat)

        # Remove stream button (only when more than one stream)
        if stream_count > 1:
            if st.button("Remove stream", key=f"pf_remove_{idx}", type="secondary"):
                _remove_stream(idx)
                st.rerun()

    if uploaded is None:
        return None  # caller will flag missing video

    return {
        "uploaded_file": uploaded,
        "model": model,
        "detect": selected_classes,
        "features": selected_features,
    }


# ---------------------------------------------------------------------------
# Public component
# ---------------------------------------------------------------------------

def render_pipeline_form() -> None:
    """Render the full pipeline control form.

    - Fetches available models from GET /api/models (Requirement 9.10)
    - Renders per-stream config rows (Requirement 9.1)
    - "Add Stream" button (Requirement 9.1)
    - Global inference interval selector
    - "Start Pipeline" button (Requirement 9.2)
    - Pipeline status + "Stop Pipeline" button (Requirement 9.8)
    """
    _init_state()

    st.subheader("Pipeline Control")

    # -----------------------------------------------------------------------
    # Current status (Requirement 9.8)
    # -----------------------------------------------------------------------
    status_data = get_pipeline_status()
    _render_status_bar(status_data)

    st.markdown("---")
    st.subheader("New Pipeline Run")

    # -----------------------------------------------------------------------
    # Fetch models for dropdowns (Requirement 9.10)
    # -----------------------------------------------------------------------
    models_data: dict = get_models() or {}
    model_names: list[str] = list(models_data.keys())

    if not model_names:
        st.warning("No models found — ensure the backend is running and rules.json is present.")

    # -----------------------------------------------------------------------
    # Global settings
    # -----------------------------------------------------------------------
    inference_interval = st.number_input(
        "Inference interval (process every Nth frame)",
        min_value=1,
        max_value=30,
        value=_DEFAULT_INFERENCE_INTERVAL,
        step=1,
        key="pf_inference_interval",
        help="At interval=4, every 5th frame is inferred. Lower = more accurate, higher = faster.",
    )

    st.markdown("---")

    # -----------------------------------------------------------------------
    # Per-stream rows (Requirement 9.1)
    # -----------------------------------------------------------------------
    stream_count: int = st.session_state["pf_stream_count"]
    stream_configs = []
    missing_videos: list[int] = []

    for idx in range(stream_count):
        row = _render_stream_row(idx, models_data, model_names, stream_count)
        if row is None:
            missing_videos.append(idx + 1)
        else:
            stream_configs.append(row)

    # "Add Stream" button (Requirement 9.1)
    if st.button("➕ Add Stream", key="pf_add_stream"):
        _add_stream()
        st.rerun()

    st.markdown("---")

    # -----------------------------------------------------------------------
    # Start Pipeline button (Requirement 9.2)
    # -----------------------------------------------------------------------
    if st.button("▶ Start Pipeline", type="primary", key="pf_start"):
        if missing_videos:
            st.error(
                f"Please upload a video for stream(s): {', '.join(str(n) for n in missing_videos)}"
            )
        elif not stream_configs:
            st.error("Add at least one stream before starting.")
        else:
            _submit_pipeline(stream_configs, int(inference_interval))


def _submit_pipeline(stream_configs: list[dict], inference_interval: int) -> None:
    """Save uploaded files and call POST /api/pipeline/start (Requirements 9.2, 9.9)."""
    import os
    import tempfile

    upload_dir = os.environ.get("UPLOAD_DIR", "videos/uploads")
    os.makedirs(upload_dir, exist_ok=True)

    payload_streams = []
    saved_paths: list[str] = []

    for row in stream_configs:
        uploaded_file = row["uploaded_file"]
        # Save uploaded file to upload directory (Requirement 9.9)
        dest_path = os.path.join(upload_dir, uploaded_file.name)
        with open(dest_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        saved_paths.append(dest_path)

        payload_streams.append({
            "video_path": dest_path,
            "model": row["model"],
            "detect": row["detect"],
            "features": row["features"],
        })

    payload = {
        "streams": payload_streams,
        "inference_interval": inference_interval,
    }

    with st.spinner("Starting pipeline…"):
        result = start_pipeline(payload)

    if result is not None:
        st.success(f"Pipeline started. Status: `{result.get('status', 'unknown')}`")
        # Reset stream count for next run
        st.session_state["pf_stream_count"] = 1
        st.rerun()
    else:
        st.error("Failed to start pipeline — backend unreachable or returned an error.")
