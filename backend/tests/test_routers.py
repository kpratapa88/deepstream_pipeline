"""
Property and unit tests for backend REST API endpoints.

Feature: streamlit-dashboard
  Property 4: Event filter correctness  — Validates: Requirements 2.1
  Property 5: Snapshot round-trip       — Validates: Requirements 2.4
"""
import uuid

import pytest
from fastapi.testclient import TestClient
from hypothesis import given, settings as hyp_settings
from hypothesis import strategies as st

from backend.main import app
from backend.state import AppState, EventEntry, app_state

client = TestClient(app)


# ---------------------------------------------------------------------------
# Helpers / strategies
# ---------------------------------------------------------------------------

CAM_IDS = ["CAM_01", "CAM_02", "CAM_03"]
EVENT_TYPES = ["alert", "heartbeat", "benchmark", "clip"]
ALERT_TYPES = ["fall", "fire", "smoke", "crowd", ""]


def make_entry(
    cam_id: str = "CAM_01",
    event_type: str = "alert",
    timestamp_ms: int = 1000,
) -> EventEntry:
    return EventEntry(
        message_id=str(uuid.uuid4()),
        event_type=event_type,
        alert_type="fall",
        cam_id=cam_id,
        location="Entrance",
        confidence=0.9,
        details={},
        clip_url="",
        timestamp_utc="2026-01-01T00:00:00Z",
        timestamp_ms=timestamp_ms,
    )


event_strategy = st.builds(
    EventEntry,
    message_id=st.uuids().map(str),
    event_type=st.sampled_from(EVENT_TYPES),
    alert_type=st.sampled_from(ALERT_TYPES),
    cam_id=st.sampled_from(CAM_IDS),
    location=st.text(min_size=1, max_size=20),
    confidence=st.floats(min_value=0.0, max_value=1.0, allow_nan=False),
    details=st.just({}),
    clip_url=st.just(""),
    timestamp_utc=st.just("2026-01-01T00:00:00Z"),
    timestamp_ms=st.integers(min_value=0, max_value=10**13),
)


# ---------------------------------------------------------------------------
# Property 4: Event filter correctness
# Feature: streamlit-dashboard, Property 4: Event filter correctness
# Validates: Requirements 2.1
# ---------------------------------------------------------------------------

@given(
    events=st.lists(event_strategy, min_size=1, max_size=100),
    cam_id=st.sampled_from(CAM_IDS),
    event_type=st.sampled_from(EVENT_TYPES),
)
@hyp_settings(max_examples=100)
def test_event_filter_correctness(events, cam_id, event_type):
    """
    For any event log containing events of mixed cam_ids and event_types,
    calling GET /api/events?cam_id=X SHALL return only events where cam_id == X,
    and calling GET /api/events?event_type=Y SHALL return only events where
    event_type == Y.
    Validates: Requirements 2.1
    """
    # Populate shared state
    app_state.event_log.clear()
    for e in events:
        app_state.event_log.append(e)

    # --- cam_id filter ---
    resp = client.get(f"/api/events?cam_id={cam_id}")
    assert resp.status_code == 200
    result = resp.json()
    for row in result:
        assert row["cam_id"] == cam_id, (
            f"cam_id filter returned row with cam_id={row['cam_id']!r}, expected {cam_id!r}"
        )

    # --- event_type filter ---
    resp2 = client.get(f"/api/events?event_type={event_type}")
    assert resp2.status_code == 200
    result2 = resp2.json()
    for row in result2:
        assert row["event_type"] == event_type, (
            f"event_type filter returned row with event_type={row['event_type']!r}, "
            f"expected {event_type!r}"
        )


# ---------------------------------------------------------------------------
# Property 5: Snapshot round-trip
# Feature: streamlit-dashboard, Property 5: Snapshot round-trip
# Validates: Requirements 2.4
# ---------------------------------------------------------------------------

@given(
    stream_id=st.integers(min_value=0, max_value=15),
    jpeg_bytes=st.binary(min_size=100, max_size=50_000),
)
@hyp_settings(max_examples=100)
def test_snapshot_round_trip(stream_id, jpeg_bytes):
    """
    For any JPEG byte sequence pushed to POST /internal/snapshot/{stream_id},
    a subsequent GET /api/snapshot/{stream_id} SHALL return the exact same bytes.
    Validates: Requirements 2.4
    """
    # Push snapshot
    push_resp = client.post(
        f"/internal/snapshot/{stream_id}",
        content=jpeg_bytes,
        headers={"Content-Type": "application/octet-stream"},
    )
    assert push_resp.status_code == 204

    # Retrieve snapshot
    get_resp = client.get(f"/api/snapshot/{stream_id}")
    assert get_resp.status_code == 200
    assert get_resp.content == jpeg_bytes, "Retrieved snapshot bytes differ from pushed bytes"


# ---------------------------------------------------------------------------
# Unit tests — snapshot 404 when missing
# ---------------------------------------------------------------------------

def test_snapshot_404_when_missing():
    """GET /api/snapshot/{id} returns 404 when no snapshot has been pushed."""
    # Use a stream_id unlikely to have been set by other tests
    app_state.snapshots.pop(9999, None)
    resp = client.get("/api/snapshot/9999")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Unit tests — events endpoint basics
# ---------------------------------------------------------------------------

def test_events_returns_list():
    app_state.event_log.clear()
    app_state.event_log.append(make_entry())
    resp = client.get("/api/events")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)
    assert len(resp.json()) == 1


def test_events_since_ms_filter():
    app_state.event_log.clear()
    app_state.event_log.append(make_entry(timestamp_ms=1000))
    app_state.event_log.append(make_entry(timestamp_ms=5000))
    resp = client.get("/api/events?since_ms=3000")
    assert resp.status_code == 200
    result = resp.json()
    assert all(r["timestamp_ms"] >= 3000 for r in result)


def test_events_limit():
    app_state.event_log.clear()
    for _ in range(20):
        app_state.event_log.append(make_entry())
    resp = client.get("/api/events?limit=5")
    assert resp.status_code == 200
    assert len(resp.json()) == 5


# ---------------------------------------------------------------------------
# Unit tests — health endpoint
# ---------------------------------------------------------------------------

def test_health_pipeline_offline_when_no_heartbeat():
    app_state.latest_heartbeat = {}
    resp = client.get("/api/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["pipeline_online"] is False


def test_benchmark_empty_when_no_data():
    app_state.latest_benchmark = {}
    resp = client.get("/api/benchmark")
    assert resp.status_code == 200
    assert resp.json() == {"streams": []}


# ---------------------------------------------------------------------------
# Unit tests — internal clips endpoint
# ---------------------------------------------------------------------------

def test_post_clip_registers_and_adds_event():
    initial_len = len(app_state.clip_registry)
    initial_log_len = len(app_state.event_log)

    payload = {
        "stream_id": 0,
        "cam_id": "CAM_01",
        "location": "Entrance",
        "alert_type": "fall",
        "clip_path": "output/clips/CAM_01_fall_123.mp4",
        "clip_url": "http://localhost:8000/api/clips/CAM_01_fall_123.mp4",
        "triggered_at_ms": 1745847045000,
    }
    resp = client.post("/internal/clips", json=payload)
    assert resp.status_code == 201
    assert len(app_state.clip_registry) == initial_len + 1
    assert len(app_state.event_log) == initial_log_len + 1
    last_entry = list(app_state.event_log)[-1]
    assert last_entry.event_type == "clip"
    assert last_entry.clip_url == payload["clip_url"]
