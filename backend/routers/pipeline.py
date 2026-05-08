"""
Pipeline control endpoints.

POST /api/pipeline/start   — validate request, write rules.json, launch pipeline
GET  /api/pipeline/status  — current pipeline state
POST /api/pipeline/stop    — stop running pipeline

Requirements: 9.3, 9.4, 9.5, 9.6, 9.7
"""
import json
import logging
import os
import shutil
import tempfile
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, UploadFile, File
from pydantic import BaseModel

from backend.pipeline_manager import pipeline_launcher, STATUS_STOPPED

logger = logging.getLogger(__name__)
router = APIRouter()

# Upload directory for video files (Req 9.9)
_UPLOAD_DIR = os.environ.get("UPLOAD_DIR", "videos/uploads")


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class StreamConfig(BaseModel):
    video_path: str
    model: str
    detect: List[str]
    features: Optional[List[str]] = []


class PipelineStartRequest(BaseModel):
    streams: List[StreamConfig]
    inference_interval: Optional[int] = 4
    kafka_broker: Optional[str] = "localhost:9092"


class PipelineStatusResponse(BaseModel):
    status: str          # stopped | starting | running | error
    last_error: str = ""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_rules_json(request: PipelineStartRequest) -> dict:
    """
    Build a rules.json dict from a PipelineStartRequest.
    Requirements: 9.3
    """
    stream_configs: Dict[str, Any] = {}
    for idx, stream in enumerate(request.streams):
        model_entry: Dict[str, Any] = {
            "name": stream.model,
            "detect": stream.detect,
        }
        if stream.features:
            model_entry["features"] = stream.features

        stream_configs[str(idx)] = {
            "id": f"CAM_{idx + 1:02d}",
            "location": f"Stream_{idx}",
            "models": [model_entry],
            "description": f"Auto-generated stream {idx}",
        }

    return {
        "stream_configs": stream_configs,
        "models": {},   # models section kept empty; pipeline reads its own defaults
    }


def _write_temp_rules(rules: dict) -> str:
    """Write rules dict to a temp file and return the path."""
    os.makedirs(_UPLOAD_DIR, exist_ok=True)
    fd, path = tempfile.mkstemp(suffix=".json", prefix="rules_", dir=_UPLOAD_DIR)
    with os.fdopen(fd, "w") as f:
        json.dump(rules, f, indent=2)
    return path


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/api/pipeline/start", status_code=202)
def start_pipeline(request: PipelineStartRequest):
    """
    Validate request, write temp rules.json, launch pipeline.
    If a pipeline is already running, stop it first (Req 9.7).
    Requirements: 9.3, 9.4, 9.7
    """
    if not request.streams:
        raise HTTPException(status_code=422, detail="At least one stream config is required")

    # Stop existing pipeline if running (Req 9.7)
    current_status = pipeline_launcher.status()
    if current_status in ("starting", "running"):
        logger.info("Pipeline already running — stopping before restart")
        try:
            pipeline_launcher.stop()
        except Exception as exc:
            logger.warning("Error stopping existing pipeline: %s", exc)

    # Build and write rules.json
    rules = _build_rules_json(request)
    rules_path = _write_temp_rules(rules)
    logger.info("Wrote temp rules.json to %s", rules_path)

    # Launch
    try:
        pipeline_launcher.start(
            rules_path=rules_path,
            args={
                "kafka_broker": request.kafka_broker,
                "inference_interval": request.inference_interval,
            },
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to start pipeline: {exc}")

    return {"status": pipeline_launcher.status(), "rules_path": rules_path}


@router.get("/api/pipeline/status", response_model=PipelineStatusResponse)
def get_pipeline_status():
    """
    Return current pipeline state: stopped | starting | running | error.
    Requirements: 9.5
    """
    return PipelineStatusResponse(
        status=pipeline_launcher.status(),
        last_error=pipeline_launcher.last_error,
    )


@router.post("/api/pipeline/stop", status_code=200)
def stop_pipeline():
    """
    Send SIGTERM to the running pipeline and wait up to 10 seconds.
    Requirements: 9.6
    """
    current = pipeline_launcher.status()
    if current == STATUS_STOPPED:
        return {"status": STATUS_STOPPED, "message": "Pipeline was not running"}

    try:
        pipeline_launcher.stop()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Error stopping pipeline: {exc}")

    # Clear snapshots so the dashboard tiles disappear immediately
    from backend.state import app_state
    app_state.snapshots.clear()
    app_state.alert_highlights.clear()

    return {"status": pipeline_launcher.status()}
