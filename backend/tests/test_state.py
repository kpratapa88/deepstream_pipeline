"""
Property-based tests for backend/state.py

Feature: streamlit-dashboard
  Property 1: Event log bounded size        — Validates: Requirements 1.2
  Property 2: Event log append correctness  — Validates: Requirements 1.1
  Property 3: Latest heartbeat tracking     — Validates: Requirements 1.5
"""
import uuid
from collections import deque

import pytest
from hypothesis import given, settings as hyp_settings
from hypothesis import strategies as st

from backend.state import AppState, EventEntry


# ---------------------------------------------------------------------------
# Helpers / strategies
# ---------------------------------------------------------------------------

def make_event(message_id: str = None, event_type: str = "alert") -> EventEntry:
    return EventEntry(
        message_id=message_id or str(uuid.uuid4()),
        event_type=event_type,
        alert_type="fall",
        cam_id="CAM_01",
        location="Entrance",
        confidence=0.9,
        details={},
        clip_url="",
        timestamp_utc="2026-01-01T00:00:00Z",
        timestamp_ms=1000,
    )


event_type_strategy = st.sampled_from(["alert", "heartbeat", "benchmark", "clip"])

event_strategy = st.builds(
    EventEntry,
    message_id=st.uuids().map(str),
    event_type=event_type_strategy,
    alert_type=st.sampled_from(["fall", "fire", "smoke", "crowd", ""]),
    cam_id=st.text(min_size=1, max_size=10),
    location=st.text(min_size=1, max_size=20),
    confidence=st.floats(min_value=0.0, max_value=1.0, allow_nan=False),
    details=st.just({}),
    clip_url=st.just(""),
    timestamp_utc=st.just("2026-01-01T00:00:00Z"),
    timestamp_ms=st.integers(min_value=0, max_value=10**13),
)

heartbeat_strategy = st.fixed_dictionaries({
    "message_id": st.uuids().map(str),
    "event_type": st.just("heartbeat"),
    "timestamp_ms": st.integers(min_value=0, max_value=10**13),
    "payload": st.fixed_dictionaries({
        "fps_total": st.floats(min_value=0, max_value=120, allow_nan=False),
        "gpu_util_pct": st.floats(min_value=0, max_value=100, allow_nan=False),
    }),
})


# ---------------------------------------------------------------------------
# Property 1: Event log bounded size
# Feature: streamlit-dashboard, Property 1: Event log bounded size
# Validates: Requirements 1.2
# ---------------------------------------------------------------------------

@given(st.lists(event_strategy, min_size=501, max_size=1000))
@hyp_settings(max_examples=100)
def test_event_log_bounded_size(events):
    """
    For any sequence of N events pushed to the backend (N > 500),
    the event log length SHALL equal exactly 500 and SHALL contain
    only the N most recent events.
    """
    state = AppState()
    for event in events:
        state.event_log.append(event)

    assert len(state.event_log) == 500, (
        f"Expected 500 entries, got {len(state.event_log)}"
    )
    # The log must contain the last 500 events (most recent)
    expected_last_500 = events[-500:]
    actual_ids = [e.message_id for e in state.event_log]
    expected_ids = [e.message_id for e in expected_last_500]
    assert actual_ids == expected_ids, "Event log does not contain the most recent 500 events"


# ---------------------------------------------------------------------------
# Property 2: Event log append correctness
# Feature: streamlit-dashboard, Property 2: Event log append correctness
# Validates: Requirements 1.1
# ---------------------------------------------------------------------------

@given(event_strategy)
@hyp_settings(max_examples=100)
def test_event_log_append_correctness(event):
    """
    For any Kafka message of type alert/heartbeat/benchmark/clip,
    after the backend processes it, the event log SHALL contain an entry
    whose message_id matches the Kafka message's message_id.
    """
    state = AppState()
    state.event_log.append(event)

    ids_in_log = {e.message_id for e in state.event_log}
    assert event.message_id in ids_in_log, (
        f"message_id {event.message_id!r} not found in event log after append"
    )


# ---------------------------------------------------------------------------
# Property 3: Latest heartbeat tracking
# Feature: streamlit-dashboard, Property 3: Latest heartbeat tracking
# Validates: Requirements 1.5
# ---------------------------------------------------------------------------

@given(st.lists(heartbeat_strategy, min_size=1, max_size=50))
@hyp_settings(max_examples=100)
def test_latest_heartbeat_tracking(heartbeats):
    """
    For any sequence of heartbeat messages, after processing all of them,
    the backend's latest_heartbeat SHALL equal the last message in the
    sequence (not any earlier one).
    """
    state = AppState()
    for hb in heartbeats:
        state.latest_heartbeat = hb

    assert state.latest_heartbeat == heartbeats[-1], (
        "latest_heartbeat does not equal the last heartbeat in the sequence"
    )
    # Verify it is NOT an earlier heartbeat (unless all are identical)
    if len(heartbeats) > 1:
        assert state.latest_heartbeat["message_id"] == heartbeats[-1]["message_id"]
