"""
Event table component for the Streamlit dashboard.
Requirements: 4.1–4.6
"""
import time
from typing import Optional

import pandas as pd
import streamlit as st

from dashboard.api_client import get_events

_EVENT_COLOURS = {
    "fall":      ("#FF4444", "#FFFFFF"),
    "fire":      ("#FF6600", "#FFFFFF"),
    "smoke":     ("#CCAA00", "#000000"),
    "crowd":     ("#CC44FF", "#FFFFFF"),
    "heartbeat": ("#00AAAA", "#000000"),
    "benchmark": ("#2266CC", "#FFFFFF"),
    "clip":      ("#226622", "#FFFFFF"),
}

# Alert event types that can have clips
_ALERT_TYPES = {"fall", "fire", "smoke", "crowd"}


def _row_style(row: pd.Series) -> list[str]:
    key = row.get("Alert Type") or row.get("Event Type", "")
    bg, fg = _EVENT_COLOURS.get(key, ("", ""))
    style = f"background-color:{bg};color:{fg};" if bg else ""
    return [style] * len(row)


def render_event_table(filters: Optional[dict] = None) -> None:
    if filters is None:
        filters = {}

    events: Optional[list] = get_events(**filters)

    if events is None:
        st.warning("Could not reach backend — event table unavailable.")
        return
    if not events:
        st.info("No events match the current filters.")
        return

    # ── Build display dataframe ───────────────────────────────────────────────
    rows = []
    for ev in events:
        details = ev.get("details", {})
        if isinstance(details, dict):
            details_str = ", ".join(f"{k}={v}" for k, v in details.items() if k not in ("clip_path", "clip_url"))
        else:
            details_str = str(details)

        clip_url = ev.get("clip_url", "")
        rows.append({
            "Timestamp":  ev.get("timestamp_utc", ""),
            "Camera":     ev.get("cam_id", ""),
            "Location":   ev.get("location", ""),
            "Event Type": ev.get("event_type", ""),
            "Alert Type": ev.get("alert_type", ""),
            "Confidence": f"{ev.get('confidence', 0.0):.2f}" if ev.get("confidence") else "",
            "Details":    details_str,
            "Clip":       "✅ Ready" if clip_url else ("⏳ Pending" if ev.get("alert_type") in _ALERT_TYPES else ""),
            "_clip_url":  clip_url,
            "_msg_id":    ev.get("message_id", ""),
        })

    df_display = pd.DataFrame(rows)
    display_cols = ["Timestamp", "Camera", "Location", "Event Type", "Alert Type", "Confidence", "Details", "Clip"]
    styled = df_display[display_cols].style.apply(_row_style, axis=1)
    st.dataframe(styled, width="stretch", hide_index=True)

    # ── Clip playback section ─────────────────────────────────────────────────
    clip_rows = [r for r in rows if r["_clip_url"]]
    if not clip_rows:
        pending = [r for r in rows if r["Clip"] == "⏳ Pending"]
        if pending:
            import time as _time
            msgs = []
            for r in pending[:5]:  # show at most 5
                ts = r["Timestamp"] or "unknown time"
                msgs.append(f"{r['Camera']} · {r['Alert Type']} @ {ts}")
            st.caption(
                f"⏳ {len(pending)} clip(s) being written:\n" + "\n".join(msgs)
                + ("\n…" if len(pending) > 5 else "")
                + "\nClips appear here within ~5s of the alert."
            )
        return

    st.markdown("---")
    st.subheader(f"🎬 Clips ({len(clip_rows)})")

    for r in clip_rows:
        clip_url = r["_clip_url"]
        msg_id   = r["_msg_id"]
        label    = f"{r['Timestamp'] or 'unknown time'}  ·  {r['Camera']}  ·  {r['Alert Type'] or r['Event Type']}"

        with st.expander(label, expanded=False):
            play_key = f"play_{msg_id or clip_url}"

            col_info, col_btn = st.columns([5, 1])
            with col_info:
                st.caption(f"URL: `{clip_url}`")
            with col_btn:
                if st.button("▶ Play", key=play_key):
                    st.session_state[f"show_{play_key}"] = True

            if st.session_state.get(f"show_{play_key}"):
                _render_clip(clip_url, play_key)


def _render_clip(clip_url: str, play_key: str) -> None:
    retry_key = f"retry_{play_key}"
    retries   = st.session_state.get(retry_key, 0)
    try:
        import requests
        # Use GET with stream=True to check availability without downloading the full file
        resp = requests.get(clip_url, stream=True, timeout=5)
        resp.close()
        if resp.status_code == 200:
            st.video(clip_url)
            st.session_state[retry_key] = 0
        else:
            _retry(retry_key, retries)
    except Exception:
        _retry(retry_key, retries)


def _retry(retry_key: str, retries: int) -> None:
    st.warning("Clip not ready yet — retrying in 2s…")
    st.session_state[retry_key] = retries + 1
    time.sleep(2)
    st.rerun()
