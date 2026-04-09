#!/bin/bash
# setup.sh
# Run this INSIDE the DeepStream container to:
#   1. Export PT models to ONNX
#   2. Build TensorRT FP16 engines (batch=1)
#
# Usage: bash setup.sh
# Optional: FIRE_SMOKE_PT=path/to/fire_smoke.pt bash setup.sh

set -e

MODEL_DIR="./models"
mkdir -p "$MODEL_DIR"

TRTEXEC=$(which trtexec 2>/dev/null || echo "/usr/src/tensorrt/bin/trtexec")
if [ ! -f "$TRTEXEC" ]; then
    echo "ERROR: trtexec not found. Run this script inside the DeepStream container."
    exit 1
fi

FIRE_SMOKE_PT="${FIRE_SMOKE_PT:-fire_smoke.pt}"

# ── Step 1: Export ONNX ───────────────────────────────────────────────────────
echo ""
echo "=== Step 1: Exporting models to ONNX ==="

python3 - <<'EOF'
import os, shutil
from ultralytics import YOLO

models = [
    ("yolo11n.pt",      "models/yolo11n.onnx"),
    ("yolo11n-pose.pt", "models/yolo11n-pose.onnx"),
]

fire_pt = os.environ.get("FIRE_SMOKE_PT", "fire_smoke.pt")
if os.path.exists(fire_pt):
    models.append((fire_pt, "models/fire_smoke.onnx"))
else:
    print(f"WARNING: {fire_pt} not found — skipping fire/smoke model")

for pt, dst in models:
    print(f"\nExporting {pt} -> {dst}")
    result = YOLO(pt).export(
        format="onnx", imgsz=640, dynamic=True, opset=12, simplify=True
    )
    src = str(result)
    if os.path.exists(src) and src != dst:
        shutil.move(src, dst)
    print(f"  Saved: {dst}")
EOF

# ── Step 2: Build TRT engines ─────────────────────────────────────────────────
echo ""
echo "=== Step 2: Building TensorRT engines (FP16, batch=1) ==="

build_engine() {
    local ONNX="$1"
    local ENGINE="$2"
    if [ ! -f "$ONNX" ]; then
        echo "SKIP: $ONNX not found"
        return
    fi
    if [ -f "$ENGINE" ]; then
        echo "EXISTS: $ENGINE (delete to rebuild)"
        return
    fi
    echo "Building $ENGINE ..."
    "$TRTEXEC" \
        --onnx="$ONNX" \
        --saveEngine="$ENGINE" \
        --fp16 \
        --minShapes=images:1x3x640x640 \
        --optShapes=images:1x3x640x640 \
        --maxShapes=images:1x3x640x640
    echo "Done: $ENGINE"
}

build_engine "$MODEL_DIR/yolo11n.onnx"      "$MODEL_DIR/yolo11n_b1.engine"
build_engine "$MODEL_DIR/yolo11n-pose.onnx" "$MODEL_DIR/yolo11n-pose_b1.engine"
build_engine "$MODEL_DIR/fire_smoke.onnx"   "$MODEL_DIR/fire_smoke_b1.engine"

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo "=== Setup complete ==="
echo ""
echo "Engines built:"
ls -lh "$MODEL_DIR"/*.engine 2>/dev/null || echo "  (none found)"
echo ""
echo "Run the pipeline:"
echo "  python3 main.py --streams 4"
echo "  python3 main.py --streams 4 --output output/demo.mp4"
