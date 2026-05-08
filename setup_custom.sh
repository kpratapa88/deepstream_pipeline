#!/bin/bash
# setup_custom.sh
# 1. Install ultralytics for model export
pip3 install ultralytics

# 2. Check for the custom model file
MODEL_PT="models/yolov11m_custom.pt"
if [ ! -f "$MODEL_PT" ]; then
    echo "Error: $MODEL_PT not found!"
    echo "Please place your trained YOLOv11m model in the models/ directory as 'yolov11m_custom.pt'."
    echo "For testing purposes, you can copy the nano model:"
    echo "  cp models/yolo11n.pt models/yolov11m_custom.pt"
    exit 1
fi

# 3. Export PT to ONNX with dynamic shapes
echo "--- Exporting $MODEL_PT to ONNX ---"
python3 -c "from ultralytics import YOLO; model = YOLO('$MODEL_PT'); model.export(format='onnx', imgsz=640, opset=12, dynamic=True)"

# 4. Convert ONNX to TensorRT Engine
ONNX_FILE="models/yolov11m_custom.onnx"
ENGINE_FILE="models/yolov11m_custom.engine"

if [ ! -f "$ONNX_FILE" ]; then
    echo "Error: ONNX export failed. $ONNX_FILE not created."
    exit 1
fi

echo "--- Converting ONNX to TensorRT Engine ---"
/usr/src/tensorrt/bin/trtexec \
    --onnx="$ONNX_FILE" \
    --saveEngine="$ENGINE_FILE" \
    --fp16 \
    --minShapes=images:1x3x640x640 \
    --optShapes=images:4x3x640x640 \
    --maxShapes=images:4x3x640x640

echo "--- Custom Model Conversion Complete ---"
ls -l models/yolov11m_custom.*
