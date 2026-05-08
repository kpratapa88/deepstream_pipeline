"""
Biological E Peripheral Monitoring — main dashboard.
"""
import time

import streamlit as st
from dotenv import load_dotenv

load_dotenv()

from dashboard.api_client import get_benchmark, get_events, get_health  # noqa: E402
from dashboard.components.tiled_feed import render_tiled_feed  # noqa: E402
from dashboard.components.event_clips import render_event_clips  # noqa: E402
from dashboard.components.pipeline_control import render_pipeline_control  # noqa: E402

st.set_page_config(
    page_title="Biological E Peripheral Monitoring",
    page_icon="🔬",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Thresholds for colour coding ──────────────────────────────────────────────
_GPU_WARN  = 80
_GPU_CRIT  = 95
_VRAM_WARN = 6000
_VRAM_CRIT = 7500
_FPS_WARN  = 10
_FPS_CRIT  = 3


def _colour(val, warn, crit, invert=False):
    if invert:
        if val <= crit:  return "#d32f2f"
        if val <= warn:  return "#e65100"
        return "#2e7d32"
    else:
        if val >= crit:  return "#d32f2f"
        if val >= warn:  return "#e65100"
        return "#2e7d32"


def _stat(label, value, colour="#333"):
    return (
        f"<div style='display:flex;justify-content:space-between;"
        f"align-items:center;padding:5px 0;border-bottom:1px solid #eee;'>"
        f"<span style='color:#888;font-size:0.72rem;'>{label}</span>"
        f"<span style='color:{colour};font-size:0.78rem;font-weight:600;'>{value}</span>"
        f"</div>"
    )


# ── Global CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
#MainMenu, footer, header {visibility: hidden;}
[data-testid="stSidebar"] {
    background: #ffffff;
    border-right: 1px solid #e8eaed;
}
.main .block-container {
    padding-top: 0 !important;
    padding-bottom: 0 !important;
    max-width: 100% !important;
}
.be-navbar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    background: #ffffff;
    border-bottom: 2px solid #e8eaed;
    padding: 12px 28px;
    margin: -1rem -1rem 14px -1rem;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
}
.be-brand { display:flex; align-items:center; gap:14px; }
.be-icon {
    width:44px; height:44px;
    background: linear-gradient(135deg,#0072ff,#00c6ff);
    border-radius:10px;
    display:flex; align-items:center; justify-content:center;
    font-size:22px; flex-shrink:0;
    box-shadow: 0 2px 8px rgba(0,114,255,0.3);
}
.be-name {
    font-size:1.35rem; font-weight:800;
    color:#1a1a2e; letter-spacing:0.01em; line-height:1.15;
}
.be-tagline {
    font-size:0.7rem; color:#999;
    letter-spacing:0.12em; text-transform:uppercase;
}
.be-pills {
    display:flex; align-items:center; gap:6px; flex-wrap:wrap;
}
.be-pill {
    display:flex; flex-direction:column; align-items:center;
    background:#f8f9fb; border:1px solid #e8eaed;
    border-radius:8px; padding:5px 14px; min-width:64px;
}
.be-pill-val { font-size:0.9rem; font-weight:700; line-height:1.2; color:#1a1a2e; }
.be-pill-lbl { font-size:0.58rem; color:#aaa; text-transform:uppercase;
               letter-spacing:0.1em; }
</style>
""", unsafe_allow_html=True)

# ── Fetch data once per rerun ─────────────────────────────────────────────────
health    = get_health() or {}
benchmark = get_benchmark() or {}

online   = health.get("pipeline_online", False)
fps_tot  = health.get("fps_total", 0.0)
gpu      = health.get("gpu_util_pct", 0.0)
vram     = health.get("vram_used_mb", 0.0)
cpu      = health.get("cpu_util_pct", 0.0)
ram      = health.get("ram_used_mb", 0.0)
last_hb  = health.get("last_heartbeat_ms", 0)
streams_active = health.get("streams_active", 0)
elapsed  = (time.time() * 1000 - last_hb) / 1000 if last_hb else None
hb_str   = f"{elapsed:.0f}s ago" if elapsed is not None else "—"
dot_col  = "#22CC66" if online else "#FF3333"

# Per-stream FPS from benchmark
bm_streams = benchmark.get("streams", [])
if isinstance(bm_streams, dict):
    bm_streams = [{"stream_id": k, **v} for k, v in bm_streams.items()]
stream_fps = {str(s.get("stream_id", "")): s.get("fps", 0.0) for s in bm_streams}

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown(
        "<div style='padding:14px 0 6px 0;font-size:0.6rem;color:#bbb;"
        "letter-spacing:0.14em;text-transform:uppercase;'>Navigation</div>",
        unsafe_allow_html=True,
    )
    page = st.radio(
        "Navigation",
        options=["Live Monitor", "Event Log", "Event Clips", "Pipeline Control"],
        label_visibility="collapsed",
    )

    st.markdown("<div style='height:1px;background:#eee;margin:12px 0;'></div>",
                unsafe_allow_html=True)

    st.markdown(
        "<div style='font-size:0.6rem;color:#bbb;letter-spacing:0.14em;"
        "text-transform:uppercase;margin-bottom:8px;'>System Health</div>",
        unsafe_allow_html=True,
    )

    gpu_c  = _colour(gpu,  _GPU_WARN,  _GPU_CRIT)
    vram_c = _colour(vram, _VRAM_WARN, _VRAM_CRIT)
    cpu_c  = _colour(cpu,  70, 90)
    ram_c  = _colour(ram,  4000, 6000)

    st.markdown(
        _stat("Pipeline",  "🟢 Online" if online else "🔴 Offline",
              "#2e7d32" if online else "#d32f2f") +
        _stat("FPS (total)", f"{fps_tot:.1f}", _colour(fps_tot, _FPS_WARN, _FPS_CRIT, invert=True)) +
        _stat("GPU",  f"{gpu:.0f}%",    gpu_c) +
        _stat("VRAM", f"{vram:.0f} MB", vram_c) +
        _stat("CPU",  f"{cpu:.0f}%",    cpu_c) +
        _stat("RAM",  f"{ram:.0f} MB",  ram_c) +
        _stat("Heartbeat", hb_str, "#aaa"),
        unsafe_allow_html=True,
    )

    # Per-stream FPS
    if stream_fps:
        st.markdown(
            "<div style='height:1px;background:#eee;margin:12px 0;'></div>"
            "<div style='font-size:0.6rem;color:#bbb;letter-spacing:0.14em;"
            "text-transform:uppercase;margin-bottom:8px;'>Per-stream FPS</div>",
            unsafe_allow_html=True,
        )
        rows = ""
        for sid, fps_val in sorted(stream_fps.items(), key=lambda x: x[0]):
            fps_val = float(fps_val)
            c = _colour(fps_val, _FPS_WARN, _FPS_CRIT, invert=True)
            rows += _stat(f"CAM {sid}", f"{fps_val:.1f}", c)
        st.markdown(rows, unsafe_allow_html=True)

    st.markdown(
        "<div style='height:1px;background:#eee;margin:12px 0;'></div>"
        "<div style='font-size:0.58rem;color:#ddd;text-align:center;"
        "letter-spacing:0.06em;'>Biological E · Peripheral Monitoring</div>",
        unsafe_allow_html=True,
    )

# ── Navbar ────────────────────────────────────────────────────────────────────
st.markdown(
    f"<div class='be-navbar'>"
    f"  <div class='be-brand'>"
    f"    <div class='be-icon'>🔬</div>"
    f"    <div>"
    f"      <div class='be-name'>Biological E</div>"
    f"      <div class='be-tagline'>Peripheral Monitoring System</div>"
    f"    </div>"
    f"  </div>"
    f"  <div class='be-pills'>"
    f"    <div class='be-pill'>"
    f"      <span class='be-pill-val' style='color:{dot_col};'>●</span>"
    f"      <span class='be-pill-lbl'>{'Online' if online else 'Offline'}</span>"
    f"    </div>"
    f"    <div class='be-pill'>"
    f"      <span class='be-pill-val' style='color:{_colour(fps_tot,_FPS_WARN,_FPS_CRIT,True)};'>"
    f"        {fps_tot:.1f}</span>"
    f"      <span class='be-pill-lbl'>FPS</span>"
    f"    </div>"
    f"    <div class='be-pill'>"
    f"      <span class='be-pill-val' style='color:{_colour(gpu,_GPU_WARN,_GPU_CRIT)};'>"
    f"        {gpu:.0f}%</span>"
    f"      <span class='be-pill-lbl'>GPU</span>"
    f"    </div>"
    f"    <div class='be-pill'>"
    f"      <span class='be-pill-val' style='color:{_colour(vram,_VRAM_WARN,_VRAM_CRIT)};'>"
    f"        {vram:.0f}</span>"
    f"      <span class='be-pill-lbl'>VRAM MB</span>"
    f"    </div>"
    f"    <div class='be-pill'>"
    f"      <span class='be-pill-val' style='color:{_colour(cpu,70,90)};'>"
    f"        {cpu:.0f}%</span>"
    f"      <span class='be-pill-lbl'>CPU</span>"
    f"    </div>"
    f"    <div class='be-pill'>"
    f"      <span class='be-pill-val' style='color:{_colour(ram,4000,6000)};'>"
    f"        {ram:.0f}</span>"
    f"      <span class='be-pill-lbl'>RAM MB</span>"
    f"    </div>"
    f"    <div class='be-pill'>"
    f"      <span class='be-pill-val' style='color:#1a1a2e;font-weight:800;'>{streams_active}</span>"
    f"      <span class='be-pill-lbl'>Streams</span>"
    f"    </div>"
    f"    <div class='be-pill'>"
    f"      <span class='be-pill-val' style='color:#999;font-size:0.72rem;'>{hb_str}</span>"
    f"      <span class='be-pill-lbl'>Heartbeat</span>"
    f"    </div>"
    f"  </div>"
    f"</div>",
    unsafe_allow_html=True,
)

# ── Alert colours / icons ─────────────────────────────────────────────────────
_C = {
    "fall": "#FF3333", "fire": "#FF6600", "smoke": "#CCAA00",
    "crowd": "#AA33FF", "person": "#1565c0", "elephant": "#00AAFF", "bear": "#d84315", 
    "giraffe": "#FFCC00", "cow": "#558b2f", "horse": "#5d4037", "dog": "#ef6c00", 
    "cat": "#7b1fa2", "sheep": "#00897b", "zebra": "#424242",
}
_I = {
    "fall": "🚨", "fire": "🔥", "smoke": "💨", "crowd": "👥", "person": "👤",
    "elephant": "🐘", "bear": "🐻", "giraffe": "🦒", "cow": "🐄", "horse": "🐴",
    "dog": "🐕", "cat": "🐱", "sheep": "🐑", "zebra": "🦓",
}


def _alert_table(events: list) -> str:
    rows = ""
    for ev in reversed(events):
        atype  = ev.get("alert_type", "")
        cam    = ev.get("cam_id", "")
        loc    = ev.get("location", "")
        ts_utc = ev.get("timestamp_utc", "")
        # Extract time and add UTC timezone indicator
        ts     = ts_utc[-8:] + " UTC" if ts_utc else "—"
        conf   = ev.get("confidence", 0.0)
        colour = _C.get(atype, "#555")
        icon   = _I.get(atype, "⚠")
        rows += (
            f"<tr style='border-bottom:1px solid #f0f0f0;'>"
            f"<td style='padding:5px 8px;color:{colour};font-weight:700;"
            f"white-space:nowrap;'>{icon} {atype.upper()}</td>"
            f"<td style='padding:5px 8px;color:#333;'>{cam}</td>"
            f"<td style='padding:5px 8px;color:#666;font-size:0.72rem;'>{loc}</td>"
            f"<td style='padding:5px 8px;color:#999;font-size:0.72rem;"
            f"white-space:nowrap;'>{ts}</td>"
            f"<td style='padding:5px 8px;color:#999;font-size:0.72rem;'>{conf:.2f}</td>"
            f"</tr>"
        )
    header = (
        "<thead><tr style='background:#f8f9fb;position:sticky;top:0;"
        "border-bottom:2px solid #e8eaed;'>"
        + "".join(
            f"<th style='padding:6px 8px;color:#aaa;font-size:0.62rem;"
            f"font-weight:600;text-align:left;letter-spacing:0.1em;"
            f"text-transform:uppercase;'>{h}</th>"
            for h in ["Type", "Cam", "Location", "Time", "Conf"]
        )
        + "</tr></thead>"
    )
    return (
        f"<div style='border:1px solid #e8eaed;border-radius:8px;overflow:auto;"
        f"height:calc(100vh - 190px);background:#fff;"
        f"box-shadow:0 1px 4px rgba(0,0,0,0.04);'>"
        f"<table style='width:100%;border-collapse:collapse;font-size:0.78rem;'>"
        f"{header}<tbody>{rows}</tbody></table></div>"
    )


# ── Page: Live Monitor ────────────────────────────────────────────────────────
if page == "Live Monitor":
    # Clear any Event Clips state so it doesn't bleed into this page
    st.session_state["ec_modal_idx"] = None
    col_feed, col_alerts = st.columns([3, 2], gap="medium")

    with col_feed:
        render_tiled_feed()

    with col_alerts:
        st.markdown(
            "<div style='font-size:0.6rem;color:#bbb;letter-spacing:0.14em;"
            "text-transform:uppercase;margin-bottom:8px;'>Recent Alerts</div>",
            unsafe_allow_html=True,
        )
        events = get_events(event_type="alert", limit=50)
        if events:
            st.markdown(_alert_table(events), unsafe_allow_html=True)
        else:
            st.markdown(
                "<div style='color:#ccc;font-size:0.8rem;padding:40px 0;"
                "text-align:center;'>No alerts yet</div>",
                unsafe_allow_html=True,
            )

# ── Page: Event Log ───────────────────────────────────────────────────────────
elif page == "Event Log":
    st.markdown(
        "<div style='font-size:0.6rem;color:#bbb;letter-spacing:0.14em;"
        "text-transform:uppercase;margin-bottom:12px;'>Full Event Log</div>",
        unsafe_allow_html=True,
    )
    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        cam_filter = st.text_input("Camera", placeholder="e.g. CAM_01")
    with fc2:
        type_filter = st.selectbox("Type", ["", "alert", "heartbeat", "benchmark"])
    with fc3:
        limit = st.number_input("Max rows", min_value=10, max_value=500, value=100, step=10)

    filters = {}
    if cam_filter.strip():
        filters["cam_id"] = cam_filter.strip()
    if type_filter:
        filters["event_type"] = type_filter
    filters["limit"] = int(limit)

    all_events = get_events(**filters) or []
    if all_events:
        st.markdown(_alert_table(all_events), unsafe_allow_html=True)
    else:
        st.markdown(
            "<div style='color:#222;font-size:0.8rem;padding:40px 0;"
            "text-align:center;'>No events</div>",
            unsafe_allow_html=True,
        )

# ── Page: Event Clips ─────────────────────────────────────────────────────────
elif page == "Event Clips":
    render_event_clips()

# ── Page: Pipeline Control ────────────────────────────────────────────────────
elif page == "Pipeline Control":
    render_pipeline_control()

# ── Auto-refresh — skip on Pipeline Control and Event Clips to avoid disrupting UI ──
if page in ("Live Monitor", "Event Log"):
    time.sleep(0.5)
    st.rerun()
