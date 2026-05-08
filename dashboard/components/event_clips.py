"""
Event Clips page — searchable, paginated table with modal video player.
10 clips per page. Search across type/camera/location.
"""
import streamlit as st

from dashboard.api_client import get_events, BACKEND_URL

_C = {
    "fall":     "#d32f2f", "fire":     "#e65100", "smoke":    "#f57f17",
    "crowd":    "#6a1b9a", "person":   "#1565c0", "elephant": "#0277bd",
    "bear":     "#d84315", "giraffe":  "#f9a825", "cow":      "#558b2f",
    "horse":    "#5d4037", "dog":      "#ef6c00", "cat":      "#7b1fa2",
    "sheep":    "#00897b", "zebra":    "#424242",
}
_I = {
    "fall": "🚨", "fire": "🔥", "smoke": "💨", "crowd": "👥", "person": "👤",
    "elephant": "🐘", "bear": "🐻", "giraffe": "🦒", "cow": "🐄", "horse": "🐴",
    "dog": "🐕", "cat": "🐱", "sheep": "🐑", "zebra": "🦓",
}
_ALL_TYPES = ["", "fall", "fire", "smoke", "crowd", "person",
              "elephant", "bear", "giraffe", "cow", "horse", "dog", "cat", "sheep", "zebra"]
PAGE_SIZE = 10


def _ts_parts(ts_utc: str) -> tuple[str, str]:
    if not ts_utc:
        return "—", "—"
    try:
        return ts_utc[:10], ts_utc[11:19] + " UTC"
    except Exception:
        return ts_utc, ""


def _matches_search(ev: dict, query: str) -> bool:
    if not query:
        return True
    q = query.lower()
    return any(q in str(ev.get(f, "")).lower()
               for f in ("alert_type", "cam_id", "location", "timestamp_utc"))


def render_event_clips() -> None:
    # ── Session state init ────────────────────────────────────────────────────
    # Use prefixed keys to avoid collision with other pages
    if "ec_modal_idx" not in st.session_state:
        st.session_state["ec_modal_idx"] = None
    if "ec_page" not in st.session_state:
        st.session_state["ec_page"] = 0
    if "ec_search_val" not in st.session_state:
        st.session_state["ec_search_val"] = ""
    if "ec_type_val" not in st.session_state:
        st.session_state["ec_type_val"] = ""
    if "ec_cam_val" not in st.session_state:
        st.session_state["ec_cam_val"] = ""

    # ── Page header ───────────────────────────────────────────────────────────
    st.markdown(
        "<div style='font-size:0.6rem;color:#bbb;letter-spacing:0.14em;"
        "text-transform:uppercase;margin-bottom:12px;'>Event Clips</div>",
        unsafe_allow_html=True,
    )

    # ── Filters row ───────────────────────────────────────────────────────────
    f1, f2, f3, f4, f5 = st.columns([2.5, 1.8, 1.5, 0.8, 0.8])
    with f1:
        search_q = st.text_input(
            "Search", placeholder="🔍  type, camera, location…",
            value=st.session_state["ec_search_val"],
            key="ec_search_input",
            label_visibility="collapsed",
        )
    with f2:
        type_idx = _ALL_TYPES.index(st.session_state["ec_type_val"]) \
                   if st.session_state["ec_type_val"] in _ALL_TYPES else 0
        type_f = st.selectbox("Alert type", _ALL_TYPES, index=type_idx,
                              key="ec_type_input", label_visibility="collapsed")
    with f3:
        cam_f = st.text_input(
            "Camera", placeholder="CAM_01",
            value=st.session_state["ec_cam_val"],
            key="ec_cam_input",
            label_visibility="collapsed",
        )
    with f4:
        if st.button("🔄", key="ec_refresh", use_container_width=True, help="Refresh"):
            st.session_state["ec_modal_idx"] = None
            st.session_state["ec_page"] = 0
            st.rerun()
    with f5:
        if st.button("✕ Clear", key="ec_clear", use_container_width=True):
            # Reset all filter values in session state
            st.session_state["ec_search_val"] = ""
            st.session_state["ec_type_val"]   = ""
            st.session_state["ec_cam_val"]    = ""
            st.session_state["ec_page"]       = 0
            st.session_state["ec_modal_idx"]  = None
            st.rerun()

    # Persist current filter values so Clear can reset them
    st.session_state["ec_search_val"] = search_q
    st.session_state["ec_type_val"]   = type_f
    st.session_state["ec_cam_val"]    = cam_f

    # ── Fetch ─────────────────────────────────────────────────────────────────
    params: dict = {"limit": 500, "event_type": "alert"}
    if cam_f.strip():
        params["cam_id"] = cam_f.strip()

    all_events  = get_events(**params) or []
    clip_events = [
        e for e in reversed(all_events)
        if e.get("clip_url")
        and (not type_f or e.get("alert_type") == type_f)
        and _matches_search(e, search_q)
    ]

    total       = len(clip_events)
    total_pages = max(1, -(-total // PAGE_SIZE))

    # Clamp page
    if st.session_state["ec_page"] >= total_pages:
        st.session_state["ec_page"] = max(0, total_pages - 1)
    page     = st.session_state["ec_page"]
    page_evs = clip_events[page * PAGE_SIZE : (page + 1) * PAGE_SIZE]

    # ── Modal ─────────────────────────────────────────────────────────────────
    modal_idx = st.session_state.get("ec_modal_idx")
    if modal_idx is not None and modal_idx < total:
        ev       = clip_events[modal_idx]
        atype    = ev.get("alert_type", "")
        cam      = ev.get("cam_id", "")
        loc      = ev.get("location", "")
        conf     = ev.get("confidence", 0.0)
        clip_url = ev.get("clip_url", "")
        colour   = _C.get(atype, "#555")
        icon     = _I.get(atype, "⚠")
        date_s, time_s = _ts_parts(ev.get("timestamp_utc", ""))

        with st.container(border=True):
            h1, h2 = st.columns([8, 1])
            with h1:
                st.markdown(
                    f"<span style='color:{colour};font-size:1.05rem;font-weight:700;'>"
                    f"{icon} {atype.upper()}</span>"
                    f"<span style='color:#666;font-size:0.85rem;margin-left:10px;'>"
                    f"{cam} · {loc} · {date_s} {time_s}</span>",
                    unsafe_allow_html=True,
                )
            with h2:
                if st.button("✕ Close", key="ec_close", use_container_width=True):
                    st.session_state["ec_modal_idx"] = None
                    st.rerun()

            st.divider()
            vid_col, det_col = st.columns([3, 2], gap="medium")

            with vid_col:
                try:
                    import requests as _req
                    r = _req.get(clip_url, stream=True, timeout=3)
                    r.close()
                    if r.status_code == 200:
                        st.video(clip_url)
                    else:
                        st.warning("⏳ Clip not ready yet — try again in a moment.")
                except Exception:
                    st.warning("Could not reach clip URL.")

            with det_col:
                st.markdown(
                    "<div style='font-size:0.6rem;color:#aaa;letter-spacing:0.14em;"
                    "text-transform:uppercase;margin-bottom:10px;'>Event Details</div>",
                    unsafe_allow_html=True,
                )

                def _row(label, val, vc="#333"):
                    return (
                        f"<div style='display:flex;justify-content:space-between;"
                        f"padding:7px 0;border-bottom:1px solid #f0f0f0;'>"
                        f"<span style='color:#aaa;font-size:0.75rem;'>{label}</span>"
                        f"<span style='color:{vc};font-size:0.78rem;font-weight:600;'>"
                        f"{val}</span></div>"
                    )

                conf_c = "#2e7d32" if conf >= 0.7 else "#e65100" if conf >= 0.4 else "#d32f2f"
                card = (
                    f"<div style='background:#fafafa;border:1px solid #e8eaed;"
                    f"border-radius:8px;padding:12px 16px;'>"
                    + _row("Alert Type", f"{icon} {atype.upper()}", colour)
                    + _row("Camera",     cam)
                    + _row("Location",   loc)
                    + _row("Date",       date_s)
                    + _row("Time",       time_s)
                    + _row("Confidence", f"{conf:.2f}", conf_c)
                )
                details = ev.get("details", {})
                if isinstance(details, dict):
                    for k, v in details.items():
                        if k not in ("clip_path", "clip_url", "stream_id"):
                            card += _row(k.replace("_", " ").title(), str(v))
                card += "</div>"
                st.markdown(card, unsafe_allow_html=True)

        st.divider()

    # ── Empty state ───────────────────────────────────────────────────────────
    if not clip_events:
        st.markdown(
            "<div style='color:#ccc;font-size:0.85rem;padding:80px 0;"
            "text-align:center;border:1px dashed #e8eaed;border-radius:12px;'>"
            "📭 No clips found — try adjusting filters or wait for an alert"
            "</div>",
            unsafe_allow_html=True,
        )
        return

    # ── Pagination bar (no dropdown) ──────────────────────────────────────────
    pg1, pg2, pg3, pg4 = st.columns([3, 1, 1, 3])
    with pg1:
        start = page * PAGE_SIZE + 1
        end   = min(start + PAGE_SIZE - 1, total)
        st.markdown(
            f"<div style='font-size:0.75rem;color:#888;padding-top:8px;'>"
            f"Showing <b>{start}–{end}</b> of <b>{total}</b> clip"
            f"{'s' if total != 1 else ''}</div>",
            unsafe_allow_html=True,
        )
    with pg2:
        prev_clicked = st.button("◀ Prev", key="ec_prev", use_container_width=True,
                                 disabled=(page == 0))
        if prev_clicked and page > 0:
            st.session_state["ec_page"] -= 1
            st.session_state["ec_modal_idx"] = None
            st.rerun()
    with pg3:
        next_clicked = st.button("Next ▶", key="ec_next", use_container_width=True,
                                 disabled=(page >= total_pages - 1))
        if next_clicked and page < total_pages - 1:
            st.session_state["ec_page"] += 1
            st.session_state["ec_modal_idx"] = None
            st.rerun()
    with pg4:
        st.markdown(
            f"<div style='font-size:0.75rem;color:#aaa;padding-top:8px;text-align:right;'>"
            f"Page {page + 1} of {total_pages}</div>",
            unsafe_allow_html=True,
        )

    # ── Table header ──────────────────────────────────────────────────────────
    st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)
    HDR = ("font-size:0.62rem;color:#aaa;font-weight:600;"
           "text-transform:uppercase;letter-spacing:0.1em;"
           "padding:6px 0;border-bottom:2px solid #e8eaed;")
    cols = st.columns([0.3, 1.4, 1.2, 1.6, 1.1, 1.2, 0.7, 0.8])
    for c, lbl in zip(cols, ["#", "Type", "Camera", "Location", "Date", "Time (UTC)", "Conf", "Action"]):
        c.markdown(f"<div style='{HDR}'>{lbl}</div>", unsafe_allow_html=True)

    # ── Table rows ────────────────────────────────────────────────────────────
    for local_i, ev in enumerate(page_evs):
        global_i = page * PAGE_SIZE + local_i
        atype    = ev.get("alert_type", "")
        cam      = ev.get("cam_id", "")
        loc      = ev.get("location", "")
        conf     = ev.get("confidence", 0.0)
        colour   = _C.get(atype, "#555")
        icon     = _I.get(atype, "⚠")
        date_s, time_s = _ts_parts(ev.get("timestamp_utc", ""))
        is_sel   = st.session_state.get("ec_modal_idx") == global_i
        row_bg   = f"background:{colour}11;" if is_sel else ""
        CELL     = f"padding:7px 0;font-size:0.8rem;{row_bg}"
        row      = st.columns([0.3, 1.4, 1.2, 1.6, 1.1, 1.2, 0.7, 0.8])

        row[0].markdown(f"<div style='{CELL}color:#bbb;'>{global_i + 1}</div>", unsafe_allow_html=True)
        row[1].markdown(
            f"<div style='{CELL}'><span style='color:{colour};font-weight:700;'>"
            f"{icon} {atype.upper()}</span></div>",
            unsafe_allow_html=True,
        )
        row[2].markdown(f"<div style='{CELL}color:#333;'>{cam}</div>", unsafe_allow_html=True)
        row[3].markdown(f"<div style='{CELL}color:#555;'>{loc}</div>", unsafe_allow_html=True)
        row[4].markdown(f"<div style='{CELL}color:#777;'>{date_s}</div>", unsafe_allow_html=True)
        row[5].markdown(f"<div style='{CELL}color:#777;'>{time_s}</div>", unsafe_allow_html=True)
        conf_c = "#2e7d32" if conf >= 0.7 else "#e65100" if conf >= 0.4 else "#d32f2f"
        row[6].markdown(
            f"<div style='{CELL}font-weight:600;color:{conf_c};'>{conf:.2f}</div>",
            unsafe_allow_html=True,
        )
        with row[7]:
            if st.button("▶ Play", key=f"ec_play_{global_i}", use_container_width=True,
                         type="primary" if is_sel else "secondary"):
                st.session_state["ec_modal_idx"] = global_i
                st.session_state["ec_page"] = page
                st.rerun()
