"""
Health panel component for the Streamlit dashboard.

Renders pipeline health metrics (streams, FPS, GPU%, VRAM), a pipeline
offline warning, time-since-last-heartbeat counter, and a per-stream
benchmark table.

Requirements: 7.1, 7.2, 7.3, 7.4
"""
import time
from typing import Optional

import pandas as pd
import streamlit as st

from dashboard.api_client import get_benchmark, get_health

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _seconds_since(timestamp_ms: Optional[int]) -> Optional[float]:
    """Return seconds elapsed since *timestamp_ms* (epoch ms), or None."""
    if timestamp_ms is None:
        return None
    return (time.time() * 1000 - timestamp_ms) / 1000.0


def _fmt_elapsed(seconds: Optional[float]) -> str:
    """Format elapsed seconds as a human-readable string."""
    if seconds is None:
        return "—"
    if seconds < 60:
        return f"{seconds:.1f}s ago"
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    return f"{minutes}m {secs:02d}s ago"


# ---------------------------------------------------------------------------
# Public component
# ---------------------------------------------------------------------------

def render_health_panel() -> None:
    """Render the system health panel.

    Displays:
    - 4 metric cards: active streams, total FPS, GPU utilisation, VRAM usage
    - "Pipeline offline" warning banner when pipeline_online is False
    - Time-since-last-heartbeat counter
    - Per-stream benchmark table from GET /api/benchmark

    Requirements: 7.1, 7.2, 7.3, 7.4
    """
    health: Optional[dict] = get_health()

    # -----------------------------------------------------------------------
    # Pipeline offline warning (Requirement 7.3)
    # -----------------------------------------------------------------------
    if health is None:
        st.error("Backend unreachable — health data unavailable.")
        return

    pipeline_online: bool = health.get("pipeline_online", False)
    if not pipeline_online:
        st.warning("⚠️ Pipeline offline")

    # -----------------------------------------------------------------------
    # 4 metric cards (Requirement 7.1)
    # -----------------------------------------------------------------------
    streams_active = health.get("streams_active", 0)
    fps_total = health.get("fps_total", 0.0)
    gpu_util_pct = health.get("gpu_util_pct", 0.0)
    vram_used_mb = health.get("vram_used_mb", 0.0)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Streams", streams_active)
    c2.metric("FPS (total)", f"{fps_total:.1f}")
    c3.metric("GPU %", f"{gpu_util_pct:.1f}%")
    c4.metric("VRAM (MB)", f"{vram_used_mb:.0f}")

    # -----------------------------------------------------------------------
    # Time-since-last-heartbeat (Requirement 7.4)
    # -----------------------------------------------------------------------
    last_hb_ms: Optional[int] = health.get("last_heartbeat_ms")
    elapsed = _seconds_since(last_hb_ms)
    st.caption(f"Last heartbeat: {_fmt_elapsed(elapsed)}")

    # -----------------------------------------------------------------------
    # Per-stream benchmark table (Requirement 7.2)
    # -----------------------------------------------------------------------
    st.markdown("---")
    st.subheader("Per-stream Benchmark")

    benchmark: Optional[dict] = get_benchmark()

    if benchmark is None:
        st.info("Benchmark data unavailable.")
        return

    streams_data = benchmark.get("streams", [])
    if not streams_data:
        st.info("No benchmark data yet.")
        return

    rows = []
    for s in streams_data:
        if not isinstance(s, dict):
            continue
        rows.append({
            "Stream": s.get("stream_id", ""),
            "Camera": s.get("cam_id", ""),
            "FPS": round(float(s.get("fps", 0.0)), 2),
            "Infer ms (avg)": round(float(s.get("infer_ms_avg", 0.0)), 2),
            "Detections": s.get("detections", 0),
        })

    df = pd.DataFrame(rows)
    st.dataframe(df, width="stretch", hide_index=True)
