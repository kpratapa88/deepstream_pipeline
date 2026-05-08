"""
GET /api/streams  — active streams from configs/rules.json
GET /api/models   — available models and their detectable classes
Requirements: 2.2, 9.10
"""
import json
import os
from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException

router = APIRouter()

_RULES_PATH = os.path.join("configs", "rules.json")


def _load_rules() -> dict:
    try:
        with open(_RULES_PATH, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="rules.json not found")
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=500, detail=f"rules.json parse error: {exc}")


@router.get("/api/streams")
def get_streams() -> List[Dict[str, Any]]:
    """
    Return the list of active streams with cam_id, location,
    active_models, and current crowd zone.
    Requirements: 2.2
    """
    rules = _load_rules()
    stream_configs = rules.get("stream_configs", {})

    streams = []
    for stream_id, cfg in stream_configs.items():
        streams.append({
            "stream_id": int(stream_id),
            "cam_id": cfg.get("id", f"CAM_{stream_id}"),
            "location": cfg.get("location", ""),
            "active_models": [m.get("name") for m in cfg.get("models", [])],
            "crowd_zone": cfg.get("crowd_zone", None),
            "description": cfg.get("description", ""),
        })

    # Sort by stream_id for consistent ordering
    streams.sort(key=lambda s: s["stream_id"])
    return streams


@router.get("/api/models")
def get_models() -> Dict[str, Any]:
    """
    Return available models and their detectable classes from rules.json.
    Requirements: 9.10
    """
    rules = _load_rules()
    models_cfg = rules.get("models", {})

    result = {}
    for model_name, model_data in models_cfg.items():
        result[model_name] = {
            "engine": model_data.get("engine", ""),
            "classes": list(model_data.get("classes", {}).keys()),
        }

    return result
