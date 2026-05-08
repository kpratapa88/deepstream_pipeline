"""
Property-based tests for pipeline config generation and control endpoints.

Feature: streamlit-dashboard
  Property 9: Pipeline config generation — Validates: Requirements 9.3
"""
import json
import os

import pytest
from hypothesis import given, settings as hyp_settings
from hypothesis import strategies as st
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock

from backend.main import app
from backend.routers.pipeline import _build_rules_json, PipelineStartRequest, StreamConfig

client = TestClient(app)


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

MODEL_NAMES = ["coco", "fire_smoke", "pose", "combined"]
CLASS_NAMES = ["person", "fire", "smoke", "fall", "crowd", "car", "dog"]
FEATURE_NAMES = ["fall_detection", "crowd_density"]

stream_config_strategy = st.builds(
    StreamConfig,
    video_path=st.text(min_size=1, max_size=50, alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd"), whitelist_characters="/_-.")).map(lambda s: f"videos/{s}.mp4"),
    model=st.sampled_from(MODEL_NAMES),
    detect=st.lists(st.sampled_from(CLASS_NAMES), min_size=1, max_size=4, unique=True),
    features=st.lists(st.sampled_from(FEATURE_NAMES), min_size=0, max_size=2, unique=True),
)

pipeline_start_request_strategy = st.builds(
    PipelineStartRequest,
    streams=st.lists(stream_config_strategy, min_size=1, max_size=6),
    inference_interval=st.integers(min_value=0, max_value=9),
    kafka_broker=st.just("localhost:9092"),
)


# ---------------------------------------------------------------------------
# Property 9: Pipeline config generation
# Feature: streamlit-dashboard, Property 9: Pipeline config generation
# Validates: Requirements 9.3
# ---------------------------------------------------------------------------

@given(pipeline_start_request_strategy)
@hyp_settings(max_examples=100)
def test_pipeline_config_generation(request: PipelineStartRequest):
    """
    For any valid POST /api/pipeline/start request body, the generated
    rules.json SHALL contain a stream_configs entry for each submitted stream,
    with the correct model name, detect classes, and features.
    Validates: Requirements 9.3
    """
    rules = _build_rules_json(request)

    stream_configs = rules.get("stream_configs", {})

    # One entry per submitted stream
    assert len(stream_configs) == len(request.streams), (
        f"Expected {len(request.streams)} stream_configs entries, "
        f"got {len(stream_configs)}"
    )

    for idx, stream in enumerate(request.streams):
        key = str(idx)
        assert key in stream_configs, f"Missing stream_configs entry for index {idx}"

        cfg = stream_configs[key]
        models = cfg.get("models", [])
        assert len(models) >= 1, f"Stream {idx} has no models"

        model_entry = models[0]

        # Correct model name
        assert model_entry["name"] == stream.model, (
            f"Stream {idx}: expected model {stream.model!r}, got {model_entry['name']!r}"
        )

        # Correct detect classes
        assert model_entry["detect"] == stream.detect, (
            f"Stream {idx}: expected detect {stream.detect}, got {model_entry['detect']}"
        )

        # Features present when non-empty
        if stream.features:
            assert "features" in model_entry, f"Stream {idx}: features missing from model entry"
            assert model_entry["features"] == stream.features, (
                f"Stream {idx}: expected features {stream.features}, "
                f"got {model_entry.get('features')}"
            )
        else:
            # features key absent or empty when not provided
            assert model_entry.get("features", []) == [], (
                f"Stream {idx}: expected no features, got {model_entry.get('features')}"
            )


# ---------------------------------------------------------------------------
# Unit tests — pipeline status and stop endpoints
# ---------------------------------------------------------------------------

def test_pipeline_status_returns_valid_state():
    """GET /api/pipeline/status returns a valid status string."""
    resp = client.get("/api/pipeline/status")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] in ("stopped", "starting", "running", "error")


def test_pipeline_stop_when_not_running():
    """POST /api/pipeline/stop when pipeline is stopped returns 200."""
    # Ensure pipeline is stopped
    with patch("backend.routers.pipeline.pipeline_launcher") as mock_launcher:
        mock_launcher.status.return_value = "stopped"
        mock_launcher.last_error = ""
        resp = client.post("/api/pipeline/stop")
    assert resp.status_code == 200


def test_pipeline_start_requires_streams():
    """POST /api/pipeline/start with empty streams returns 422."""
    resp = client.post("/api/pipeline/start", json={"streams": [], "inference_interval": 4})
    assert resp.status_code == 422
