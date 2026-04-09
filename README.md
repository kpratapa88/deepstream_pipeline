# NVIDIA DeepStream Multi-Stream AI Pipeline

A DeepStream 7.0 Python pipeline processing 4 simultaneous video streams with multi-model inference, event detection, and live benchmarking.

## Stream Assignment

| Stream | Camera | Detection | Model |
|--------|--------|-----------|-------|
| 1 | CAM_01 Entrance | Person + Fall Detection | YOLOv11n + YOLOv11n-pose |
| 2 | CAM_02 Barn_West | Animals | YOLOv11n (COCO) |
| 3 | CAM_03 Factory_Floor | Fire & Smoke | Custom YOLOv11n |
| 4 | CAM_04 Main_Hall | Person + Crowd Density | YOLOv11n |

## Features

- 4-stream muxed pipeline with `nvstreammux`
- 3 parallel TRT inference models: COCO detection, fire/smoke, pose estimation
- Fall detection via YOLOv11-pose keypoint torso angle analysis
- Crowd density zone classification (CLEAR / LOW / MEDIUM / HIGH / CRITICAL)
- Per-stream event alerts: fall, fire, smoke, crowd zone change
- Live benchmark console: FPS, GPU%, CPU%, VRAM per stream
- Tiled 2×2 OSD display with bounding boxes and pose skeleton overlay
- Optional MP4 file output via CPU x264 encoder
- Configurable stream count and inference interval

## Prerequisites

- NVIDIA GPU (RTX 3060+ or Jetson Orin)
- NVIDIA Driver 535+
- [NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html)
- Docker

## Quick Start

### 1. Build the Docker image

```bash
docker build -t ds-pipeline:latest .
```

### 2. Start the container

```bash
docker run -it --rm \
  --runtime=nvidia \
  --gpus all \
  -e DISPLAY=$DISPLAY \
  -v /tmp/.X11-unix:/tmp/.X11-unix \
  -v $(pwd):/app \
  -w /app \
  ds-pipeline:latest bash
```

For RTSP streams, add port mapping:
```bash
docker run -it --rm \
  --runtime=nvidia \
  --gpus all \
  -e DISPLAY=$DISPLAY \
  -v /tmp/.X11-unix:/tmp/.X11-unix \
  -v $(pwd):/app \
  -w /app \
  -p 8554:8554 \
  ds-pipeline:latest bash
```

### 3. Prepare models (inside container)

Place your `fire_smoke.pt` in the project root, then run:

```bash
bash setup.sh
```

This will:
- Export `yolo11n.pt`, `yolo11n-pose.pt`, and `fire_smoke.pt` to ONNX
- Build TensorRT FP16 engines for all three models

### 4. Add video sources

```bash
mkdir -p videos
# Copy sample1.mp4 ... sample4.mp4 into videos/
```

Or use RTSP streams via environment variables (see Usage below).

### 5. Run the pipeline

```bash
python3 pipeline_python_yolo.py --streams 4
```

## Usage

```bash
# 4 streams with display
python3 pipeline_python_yolo.py --streams 4

# 4 streams + save output to file
python3 pipeline_python_yolo.py --streams 4 --output output/demo.mp4

# Tune inference interval (0=every frame, 2=every 3rd frame, 4=every 5th frame)
python3 pipeline_python_yolo.py --streams 4 --interval 2

# RTSP sources via environment variables
STREAM_URI_1=rtsp://cam1/stream \
STREAM_URI_2=rtsp://cam2/stream \
STREAM_URI_3=rtsp://cam3/stream \
STREAM_URI_4=rtsp://cam4/stream \
python3 pipeline_python_yolo.py --streams 4

# More streams (auto-adjusts tiler grid and batch size)
python3 pipeline_python_yolo.py --streams 8 --interval 4
```

## Project Structure

```
pipeline_python_yolo.py              # Main pipeline entry point
setup.sh                             # Model export + TRT engine build script
configs/
  config_infer_primary_yolov11.txt   # COCO detection config (GIE_1)
  config_infer_fire_smoke.txt        # Fire/smoke detection config (GIE_2)
  config_infer_pose.txt              # Pose estimation config (GIE_4)
  rules.json                         # Per-stream config and crowd thresholds
  config_tracker_NvDCF.yml           # NvDCF tracker config
utils/
  rule_engine.py                     # Stream config and rules loader
models/                              # ONNX + TRT engine files (gitignored)
videos/                              # Input video files (gitignored)
output/                              # Recorded output files (gitignored)
Dockerfile
labels.txt                           # COCO class labels
```

## Configuration

### Stream settings — `configs/rules.json`

| Field | Values | Description |
|-------|--------|-------------|
| `model` | `"coco"` / `"fire_smoke"` | Which inference model to use |
| `features` | `["fall_detection"]` / `["crowd_density"]` | Secondary features to enable |
| `crowd` | `{low, medium, high, critical}` | Person count thresholds per zone |
| `interested_gie_classes` | list of class IDs | Which classes to detect per stream |

### Inference interval — `--interval N`

| Value | Behavior | GPU load |
|-------|----------|----------|
| `0` | Every frame | Highest |
| `2` | Every 3rd frame (default) | Balanced |
| `4` | Every 5th frame | Lowest |

Bounding boxes are rendered on every frame using cached detections from the last inference frame.

## Troubleshooting

**No display window** — Run `xhost +local:docker` on the host before starting the container.

**Engine rebuild on every run** — Ensure `models/yolo11n_b1.engine` exists. Run `bash setup.sh` to build engines.

**GPU at 100%** — Increase `--interval` (e.g. `--interval 4`) to reduce inference frequency.

**Low FPS** — With 3 models and `batch-size=1`, each stream runs 3 inferences per frame. Use `--interval 2` or higher.
