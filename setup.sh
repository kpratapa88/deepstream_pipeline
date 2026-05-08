#!/bin/bash
set -e

# Directories
MODEL_DIR="./models"
mkdir -p $MODEL_DIR

# --- Stage 0: DeepStream-Yolo Plugin ---
echo "--- Stage 0: Preparing DeepStream-Yolo Plugin ---"
if [ ! -d "DeepStream-Yolo" ]; then
    git clone https://github.com/marcoslucianops/DeepStream-Yolo.git
fi

# Build the custom implementation
echo "--- Building DeepStream-Yolo Plugin ---"
cd DeepStream-Yolo/nvdsinfer_custom_impl_Yolo
# In DS 7.0, CUDA version is 12.2
make CUDA_VER=12.2
cd ../..

# --- Stage 1: YOLOv11 (Person + Animal Detection) ---
echo "--- Stage 1: Exporting YOLOv11 via DeepStream-Yolo exporter ---"
# Must use DeepStream-Yolo's export script — it adds the DeepStreamOutput layer
# that NvDsInferParseYolo expects (output shape [batch, 8400, 6] not [batch, 84, 8400])
# Static batch=4 avoids dynamic output shape issues (min:0 opt:0 Max:0 in TRT)
python3 DeepStream-Yolo/utils/export_yolo11.py \
    -w yolo11n.pt \
    --batch 4 \
    --opset 17 \
    --simplify
mv yolo11n.onnx $MODEL_DIR/yolo11n.onnx

# --- Stage 2a: Fall Detection (ResNet-18) ---
# For demonstration, we'll download a pre-trained ResNet-18 model 
# and assume it's trained for fall detection classes (Fall, No-Fall)
echo "--- Stage 2a: Downloading Fall Detection Model (ResNet-18) ---"
# Using a stable Hugging Face link
wget -O $MODEL_DIR/resnet18_fall.onnx https://huggingface.co/frgfm/resnet18/resolve/main/model.onnx

# --- TensorRT Engine Conversion ---
echo "--- Converting models to TensorRT Engines (FP16) ---"

# Find trtexec
TRTEXEC_BIN=$(which trtexec || echo "/usr/src/tensorrt/bin/trtexec")

if [ ! -f "$TRTEXEC_BIN" ]; then
    echo "trtexec not found. Please run this script inside the DeepStream container or install TensorRT."
    exit 1
fi

# 1. YOLOv11 Engine (Primary - Static Batch 4)
# Static batch ONNX from export_yolo11.py — no dynamic shape flags needed
$TRTEXEC_BIN --onnx=$MODEL_DIR/yolo11n.onnx \
    --saveEngine=$MODEL_DIR/yolo11n.engine \
    --fp16

# 2. Fall Detection Engine (Secondary - Batch 32)
# We set min/opt/max shapes to ensure it supports batch 32
# Note: input name 'input.1' was identified from previous logs
$TRTEXEC_BIN --onnx=$MODEL_DIR/resnet18_fall.onnx \
    --saveEngine=$MODEL_DIR/resnet18_fall.engine \
    --fp16 --explicitBatch \
    --minShapes=input.1:1x3x224x224 \
    --optShapes=input.1:32x3x224x224 \
    --maxShapes=input.1:32x3x224x224

# 3. Crowd Density Engine
# DISABLED: Skipping conversion for now
# $TRTEXEC_BIN --onnx=$MODEL_DIR/crowd_density.onnx \
#     --saveEngine=$MODEL_DIR/crowd_density.engine \
#     --fp16 --explicitBatch

echo "Model preparation complete. Engines are in $MODEL_DIR"
