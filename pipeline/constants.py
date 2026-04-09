"""
Pipeline-wide constants, config paths, and class definitions.
All tuneable parameters live here — no magic numbers elsewhere.
"""

# ── Config file paths ─────────────────────────────────────────────────────────
CONFIG_INFER_COCO = "configs/config_infer_primary_yolov11.txt"
CONFIG_INFER_FIRE = "configs/config_infer_fire_smoke.txt"
CONFIG_INFER_POSE = "configs/config_infer_pose.txt"
RULES_FILE        = "configs/rules.json"

# ── Model input dimensions ────────────────────────────────────────────────────
MODEL_W = 640
MODEL_H = 640

# ── Benchmark ─────────────────────────────────────────────────────────────────
BENCHMARK_INTERVAL_SEC = 5

# ── Known class sets (used as fallbacks) ─────────────────────────────────────
PERSON_CLASSES     = {0}
ANIMAL_CLASSES     = {14, 15, 16, 17, 18, 19, 20, 21, 22, 23}
FIRE_SMOKE_CLASSES = {0, 1}
FIRE_SMOKE_LABELS  = {0: "fire", 1: "smoke"}

# ── Fall detection ────────────────────────────────────────────────────────────
FALL_CLASS_ID    = 0
FALL_WINDOW_SIZE = 5
FALL_MIN_HITS    = 3

# ── GIE unique IDs (must match rules.json model definitions) ─────────────────
GIE_COCO  = 1
GIE_FIRE  = 2
GIE_POSE  = 4

# ── Max streams supported ─────────────────────────────────────────────────────
MAX_STREAMS = 10

# ── ANSI color palette ────────────────────────────────────────────────────────
C = {
    "reset":   "\033[0m",
    "bold":    "\033[1m",
    "dim":     "\033[2m",
    "green":   "\033[92m",
    "yellow":  "\033[93m",
    "cyan":    "\033[96m",
    "blue":    "\033[94m",
    "magenta": "\033[95m",
    "red":     "\033[91m",
    "white":   "\033[97m",
    "gray":    "\033[90m",
}

STREAM_COLORS = {
    0: C["green"],   1: C["yellow"],
    2: C["red"],     3: C["magenta"],
    4: C["green"],   5: C["yellow"],
    6: C["cyan"],    7: C["magenta"],
}

# ── Default stream→class mapping (overridden by rules.json) ──────────────────
def default_allowed_classes(stream_id: int) -> set:
    """Odd streams → person, even streams → animals (fallback only)."""
    return PERSON_CLASSES if stream_id % 2 == 0 else ANIMAL_CLASSES
