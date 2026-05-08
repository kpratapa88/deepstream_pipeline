"""
Property-based and unit tests for services/clip_writer.py

Feature: streamlit-dashboard
  Property 7: Frame buffer rolling eviction  — Validates: Requirements 5.1
  Property 8: Clip deduplication             — Validates: Requirements 5.6
"""
import time

import numpy as np
import pytest
from hypothesis import given, settings as hyp_settings
from hypothesis import strategies as st

from services.clip_writer import ClipWriter, StreamFrameBuffer, DEDUP_WINDOW_S


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_frame(h: int = 4, w: int = 4) -> np.ndarray:
    """Return a small random BGR frame."""
    return np.random.randint(0, 256, (h, w, 3), dtype=np.uint8)


# ---------------------------------------------------------------------------
# Property 7: Frame buffer rolling eviction
# Feature: streamlit-dashboard, Property 7: Frame buffer rolling eviction
# Validates: Requirements 5.1
# ---------------------------------------------------------------------------

@given(
    maxsize=st.integers(min_value=1, max_value=200),
    extra=st.integers(min_value=1, max_value=50),
)
@hyp_settings(max_examples=100)
def test_frame_buffer_rolling_eviction(maxsize: int, extra: int):
    """
    For any frame buffer with maxsize=M, after pushing M+K frames (K > 0),
    the buffer SHALL contain exactly M frames and they SHALL be the most
    recent M frames (frames K+1 through M+K).
    """
    buf = StreamFrameBuffer(maxsize=maxsize)
    total = maxsize + extra

    # Push total frames; tag each with a unique value in channel 0, pixel (0,0)
    frames = []
    for i in range(total):
        f = make_frame()
        f[0, 0, 0] = i % 256  # unique tag (mod 256 for dtype)
        frames.append(f)
        buf.push(f)

    snapshot = buf.snapshot()

    # Buffer must hold exactly maxsize frames
    assert len(snapshot) == maxsize, (
        f"Expected {maxsize} frames, got {len(snapshot)}"
    )

    # Must be the most recent maxsize frames
    expected = frames[-maxsize:]
    for actual_frame, expected_frame in zip(snapshot, expected):
        assert np.array_equal(actual_frame, expected_frame), (
            "Buffer does not contain the most recent frames"
        )


# ---------------------------------------------------------------------------
# Property 8: Clip deduplication
# Feature: streamlit-dashboard, Property 8: Clip deduplication
# Validates: Requirements 5.6
# ---------------------------------------------------------------------------

@given(
    stream_id=st.integers(min_value=0, max_value=9),
    alert_type=st.sampled_from(["fall", "fire", "smoke", "crowd"]),
    gap_s=st.floats(min_value=0.0, max_value=DEDUP_WINDOW_S - 0.01),
)
@hyp_settings(max_examples=100)
def test_clip_deduplication(stream_id: int, alert_type: str, gap_s: float):
    """
    For any two alerts of the same type on the same stream fired within
    DEDUP_WINDOW_S seconds of each other, the second alert SHALL be
    identified as a duplicate.
    """
    writer = ClipWriter()

    first_ts = 1000.0
    second_ts = first_ts + gap_s  # within dedup window

    # Record the first clip
    writer.record_clip_time(stream_id, alert_type, first_ts)

    # The second alert (within window) must be detected as duplicate
    assert writer.is_duplicate(stream_id, alert_type, now_s=second_ts), (
        f"Expected duplicate for gap={gap_s:.3f}s < {DEDUP_WINDOW_S}s, "
        f"but is_duplicate returned False"
    )


def test_clip_deduplication_after_window():
    """After DEDUP_WINDOW_S seconds, the same alert type is NOT a duplicate."""
    writer = ClipWriter()
    stream_id, alert_type = 0, "fall"
    first_ts = 1000.0
    after_ts = first_ts + DEDUP_WINDOW_S + 0.1

    writer.record_clip_time(stream_id, alert_type, first_ts)
    assert not writer.is_duplicate(stream_id, alert_type, now_s=after_ts), (
        "Alert after dedup window should NOT be a duplicate"
    )
