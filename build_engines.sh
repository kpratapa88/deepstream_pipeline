#!/bin/bash
# Convert an ONNX model to a TensorRT FP16 engine.
# Engine is saved as models/<name>_b<batch>.engine
# DeepStream picks it up automatically via the nvinfer config.
#
# Usage:
#   bash build_engines.sh models/fire_smoke.onnx
#   bash build_engines.sh models/combined.onnx
#   bash build_engines.sh models/yolo11n.onnx --batch 4
#   bash build_engines.sh models/yolo11n.onnx --fp32

set -e

# ── Args ──────────────────────────────────────────────────────────────────────
ONNX=""
BATCH=1
FP16=1

while [[ $# -gt 0 ]]; do
    case "$1" in
        --batch) BATCH="$2"; shift 2 ;;
        --fp32)  FP16=0;     shift   ;;
        *)       ONNX="$1";  shift   ;;
    esac
done

if [ -z "$ONNX" ]; then
    echo "Usage: bash build_engines.sh <path/to/model.onnx> [--batch N] [--fp32]"
    echo ""
    echo "Examples:"
    echo "  bash build_engines.sh models/fire_smoke.onnx"
    echo "  bash build_engines.sh models/combined.onnx --batch 4"
    echo "  bash build_engines.sh models/yolo11n.onnx --fp32"
    exit 1
fi

if [ ! -f "$ONNX" ]; then
    echo "ERROR: ONNX file not found: $ONNX"
    exit 1
fi

# ── Locate trtexec ────────────────────────────────────────────────────────────
TRTEXEC=$(which trtexec 2>/dev/null || echo "")
if [ -z "$TRTEXEC" ] || [ ! -f "$TRTEXEC" ]; then
    TRTEXEC="/usr/src/tensorrt/bin/trtexec"
fi
if [ ! -f "$TRTEXEC" ]; then
    echo "ERROR: trtexec not found. Run inside the DeepStream container."
    exit 1
fi

# ── Derive engine path: models/<stem>_b<batch>.engine ─────────────────────────
STEM=$(basename "$ONNX" .onnx)
ENGINE="models/${STEM}_b${BATCH}.engine"

echo "============================================================"
echo "  ONNX   : $ONNX"
echo "  Engine : $ENGINE"
echo "  Batch  : $BATCH"
echo "  Mode   : $([ $FP16 -eq 1 ] && echo FP16 || echo FP32)"
echo "============================================================"

if [ -f "$ENGINE" ]; then
    echo "EXISTS: $ENGINE"
    echo "Delete it to rebuild:  rm $ENGINE"
    exit 0
fi

FP16_FLAG=""
[ $FP16 -eq 1 ] && FP16_FLAG="--fp16"

"$TRTEXEC" \
    --onnx="$ONNX" \
    --saveEngine="$ENGINE" \
    $FP16_FLAG \
    --minShapes=images:${BATCH}x3x640x640 \
    --optShapes=images:${BATCH}x3x640x640 \
    --maxShapes=images:${BATCH}x3x640x640

if [ -f "$ENGINE" ]; then
    SIZE=$(du -sh "$ENGINE" | cut -f1)
    echo ""
    echo "Done: $ENGINE  ($SIZE)"
else
    echo "ERROR: Engine file was not created."
    exit 1
fi
