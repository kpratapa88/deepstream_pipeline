"""
Rule engine: loads stream configurations and model definitions from rules.json.
Provides per-stream allowed class sets and feature flags.
"""
import json
import os
from typing import Dict, Set, List, Optional


class StreamConfig:
    """Parsed configuration for a single stream."""

    def __init__(self, stream_id: int, raw: dict, model_registry: dict):
        self.stream_id = stream_id
        self.id        = raw.get("id", f"CAM_{stream_id + 1:02d}")
        self.location  = raw.get("location", "unknown")
        self.crowd     = raw.get("crowd")

        # Parse per-model subscriptions
        # models: list of {name, detect: [class_names], features: [...]}
        self.model_subscriptions: List[dict] = []
        for m in raw.get("models", []):
            model_name = m.get("name")
            if model_name not in model_registry:
                print(f"[RuleEngine] WARNING: stream {stream_id} references unknown model '{model_name}'")
                continue
            reg        = model_registry[model_name]
            detect     = m.get("detect", list(reg["classes"].keys()))
            features   = m.get("features", [])

            # Resolve class names → class IDs + thresholds
            class_ids  = set()
            thresholds = {}
            for cls_name in detect:
                cls_def = reg["classes"].get(cls_name)
                if cls_def:
                    cid = cls_def["class_id"]
                    class_ids.add(cid)
                    thresholds[cid] = cls_def.get("confidence_threshold", 0.3)

            self.model_subscriptions.append({
                "model_name": model_name,
                "gie_id":     reg["gie_id"],
                "class_ids":  class_ids,
                "thresholds": thresholds,
                "features":   features,
            })

    @property
    def active_models(self) -> Set[str]:
        return {s["model_name"] for s in self.model_subscriptions}

    def get_subscription(self, model_name: str) -> Optional[dict]:
        for s in self.model_subscriptions:
            if s["model_name"] == model_name:
                return s
        return None

    def allowed_classes(self, model_name: str) -> Set[int]:
        sub = self.get_subscription(model_name)
        return sub["class_ids"] if sub else set()

    def features(self, model_name: str) -> List[str]:
        sub = self.get_subscription(model_name)
        return sub["features"] if sub else []

    def has_feature(self, feature: str) -> bool:
        return any(feature in s["features"] for s in self.model_subscriptions)


class RuleEngine:
    """
    Loads rules.json and exposes stream configurations and model registry.
    """

    def __init__(self, rules_file: str, stream_overrides: dict = None):
        print(f"DEBUG: Initializing RuleEngine with {rules_file}")
        self.rules_file = rules_file
        raw             = self._load(rules_file)

        self.model_registry: Dict[str, dict] = raw.get("models", {})
        # Default template used for streams not explicitly defined in stream_configs
        self._stream_template: dict = raw.get("stream_template", {
            "models": [{"name": "coco", "detect": ["person"]}]
        })
        self.stream_configs: Dict[int, StreamConfig] = {}

        for sid_str, cfg in raw.get("stream_configs", {}).items():
            sid = int(sid_str)
            if stream_overrides and sid in stream_overrides:
                cfg = {**cfg, **stream_overrides[sid]}
                print(f"  [RuleEngine] stream{sid + 1} config overridden")
            self.stream_configs[sid] = StreamConfig(sid, cfg, self.model_registry)

    def get_stream(self, stream_id: int) -> "StreamConfig":
        """
        Get stream config by ID.
        If not explicitly defined, auto-generate from stream_template.
        This allows unlimited streams without pre-defining each one.
        """
        if stream_id not in self.stream_configs:
            # Auto-generate from template
            template = {
                **self._stream_template,
                "id":       f"CAM_{stream_id + 1:02d}",
                "location": f"Stream_{stream_id + 1}",
            }
            self.stream_configs[stream_id] = StreamConfig(
                stream_id, template, self.model_registry
            )
        return self.stream_configs[stream_id]

    def _load(self, path: str) -> dict:
        try:
            with open(path) as f:
                return json.load(f)
        except Exception as e:
            print(f"[RuleEngine] Error loading {path}: {e}")
            return {}

    def active_models(self) -> Set[str]:
        """All model names referenced by any stream."""
        models = set()
        for sc in self.stream_configs.values():
            models |= sc.active_models
        return models

    def streams_for_model(self, model_name: str) -> List[int]:
        """Stream IDs that use a given model."""
        return [sid for sid, sc in self.stream_configs.items()
                if model_name in sc.active_models]

    def log_metadata(self, metadata: dict) -> None:
        """Legacy compatibility — print JSON metadata."""
        if metadata:
            print(json.dumps(metadata, indent=2))
