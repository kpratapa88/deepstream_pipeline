"""
Tiled live feed — MJPEG streams embedded as native browser <img> tags.
Active streams are determined by the backend (snapshots received in last 5s).
When pipeline stops, tiles disappear automatically. When it restarts, they reappear.
"""
import math
import time

import streamlit as st

from dashboard.api_client import get_active_streams, get_health
from dashboard.config import BACKEND_URL

_C = {
    "fall":     "#d32f2f",
    "fire":     "#e65100",
    "smoke":    "#f57f17",
    "crowd":    "#6a1b9a",
    "elephant": "#0277bd",
    "bear":     "#e65100",
    "giraffe":  "#f9a825",
    "person":   "#1b5e20"
}
_I = {"fall": "🚨 FALL", "fire": "🔥 FIRE", "smoke": "💨 SMOKE", "crowd": "👥 CROWD",
      "elephant": "🐘 ELEPHANT", "bear": "🐻 BEAR", "giraffe": "🦒 GIRAFFE", "person": "👤 PERSON"}
_BADGE_TTL_MS = 1000


def _grid(n: int):
    """
    Dynamic grid: max 5 columns, scales to fit n streams.
    1→1, 2→2, 3→3, 4→2x2, 5→3+2, 6→3x2, 7→4+3, 8→4x2, 9→3x3, 10→5x2
    """
    if n <= 3:
        cols = n
    elif n <= 4:
        cols = 2
    elif n <= 6:
        cols = 3
    elif n <= 8:
        cols = 4
    else:
        cols = 5
    rows = math.ceil(n / cols)
    return cols, rows


def render_tiled_feed(detection_on: bool = True) -> int:
    # Backend returns only streams with snapshots in the last 5s.
    # Empty list = pipeline stopped → show waiting message.
    active_ids = get_active_streams()
    n = len(active_ids)

    if n == 0:
        st.markdown(
            "<div style='color:#bbb;font-size:0.85rem;padding:60px 0;text-align:center;'>"
            "⏳ Waiting for pipeline frames…</div>",
            unsafe_allow_html=True,
        )
        return 0

    # Alert highlights for badge overlay
    highlights, now_ms = {}, int(time.time() * 1000)
    if detection_on:
        h = get_health()
        if h:
            highlights = h.get("alert_highlights", {})

    cols, _ = _grid(n)
    tile_pct = 100 / cols

    html = (
        "<style>"
        "@keyframes fadeout{"
        "0%{opacity:1}"
        "60%{opacity:1}"
        "100%{opacity:0;pointer-events:none}"
        "}"
        "</style>"
        "<div style='display:flex;flex-wrap:wrap;gap:4px;'>"
    )

    for sid in active_ids:
        stream_url = f"{BACKEND_URL}/api/stream/{sid}"

        # Resolve active alert for this stream
        alert = None
        hi = highlights.get(str(sid)) or highlights.get(sid)
        if hi:
            ts = hi.get("timestamp_ms", 0) if isinstance(hi, dict) else (hi[1] if len(hi) > 1 else 0)
            if now_ms - ts < _BADGE_TTL_MS:
                alert = hi.get("alert_type") if isinstance(hi, dict) else hi[0]

        colour = _C.get(alert, "#222") if alert else "#222"
        border = f"2px solid {colour}" if alert else "1px solid #2a2a2a"

        badge = ""
        if alert:
            label = _I.get(alert, f"⚠ {alert.upper()}")
            badge = (
                f"<div style='position:absolute;top:0;left:0;right:0;"
                f"background:{colour}dd;color:#fff;font-size:0.65rem;"
                f"font-weight:700;padding:2px 6px;text-align:center;"
                f"letter-spacing:0.07em;z-index:2;"
                f"animation:fadeout 0.5s ease-in forwards;'>{label}</div>"
            )

        html += (
            f"<div style='position:relative;flex:0 0 calc({tile_pct:.1f}% - 4px);"
            f"border:{border};border-radius:6px;overflow:hidden;background:#f0f0f0;"
            f"box-shadow:0 1px 4px rgba(0,0,0,0.08);'>"
            f"{badge}"
            f"<img src='{stream_url}' style='width:100%;display:block;' />"
            f"<div style='font-size:0.62rem;color:#999;padding:2px 6px;"
            f"background:#fafafa;border-top:1px solid #eee;'>CAM {sid}</div>"
            f"</div>"
        )

    html += "</div>"
    st.markdown(html, unsafe_allow_html=True)
    return n
