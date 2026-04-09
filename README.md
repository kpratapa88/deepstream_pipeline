# NVIDIA DeepStream Multi-Stream AI Pipeline

A production-grade DeepStream 7.0 Python pipeline for real-time multi-stream AI inference. Supports unlimited streams, dynamic per-stream model configuration, fall detection, fire/smoke detection, and crowd density monitoring.

---

## Table of Contents

1. [Features](#features)
2. [Architecture](#architecture)
3. [Prerequisites](#prerequisites)
4. [Setup — WSL2 (Windows)](#setup--wsl2-windows)
5. [Setup — GCP VM (Linux)](#setup--gcp-vm-linux)
6. [Model Preparation](#model-preparation)
7. [Running the Pipeline](#running-the-pipeline)
8. [Stream Configuration](#stream-configuration)
9. [CLI Reference](#cli-reference)
10. [Project Structure](#project-structure)
11. [Troubleshooting](#troubleshooting)

---

## Features

- **Unlimited streams** — configurable at runtime, GPU memory is the only limit
- **3 parallel TRT models** — COCO detection (GIE_1), fire/smoke (GIE_2), pose estimation (GIE_4)
- **Fall detection** — YOLOv11-pose keypoint torso angle analysis with temporal window confirmation
- **Crowd density** — rolling person count with zone classification (CLEAR/LOW/MEDIUM/HIGH/CRITICAL)
- **Per-stream model assignment** — each stream can run any combination of models and features
- **Dynamic CLI configuration** — override any stream's models/features without editing files
- **Live benchmark** — per-stream FPS, inference latency, GPU%, CPU%, VRAM
- **Tiled OSD display** — bounding boxes, pose skeleton, crowd overlay
- **Optional MP4 recording** — CPU x264 encoder (no extra GPU memory)

---

## Architecture

```
4–N Video Streams
      │
      ▼
 nvstreammux (batch)
      │
      ▼
 nvinfer GIE_1 (YOLOv11n COCO)     ← person, animals
      │
      ▼
 nvinfer GIE_2 (fire_smoke)        ← fire, smoke
      │
      ▼
 nvinfer GIE_4 (YOLOv11n-pose)     ← keypoints for fall detection
      │
      ▼
 Python Probes                      ← tensor parse, NMS, fall/crowd logic
      │
      ▼
 nvmultistreamtiler → nvdsosd → sink (display / MP4)
```

**Inference models:**

| GIE | Model | Task | Output tensor |
|-----|-------|------|---------------|
| GIE_1 | YOLOv11n (COCO) | Person + animal detection | `[84, 8400]` |
| GIE_2 | YOLOv11n (custom) | Fire & smoke detection | `[6, 8400]` |
| GIE_4 | YOLOv11n-pose | Pose estimation | `[56, 8400]` |

---

## Prerequisites

### Hardware

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| GPU | NVIDIA RTX 3060 (8 GB VRAM) | RTX 3080 / A10 / T4 |
| CPU | 8-core | 16-core |
| RAM | 16 GB | 32 GB |
| Storage | 20 GB free | 50 GB SSD |

### Software

| Requirement | Version |
|-------------|---------|
| NVIDIA Driver | 535+ |
| Docker Engine | 24+ |
| NVIDIA Container Toolkit | Latest |

---

## Setup — WSL2 (Windows)

### Step 1 — Enable WSL2 and install Ubuntu

Open PowerShell as Administrator:

```powershell
wsl --install -d Ubuntu-22.04
wsl --set-default-version 2
```

Restart your machine, then open Ubuntu from the Start menu.

### Step 2 — Install NVIDIA Driver on Windows host

Download and install the latest NVIDIA Game Ready or Studio driver from:
https://www.nvidia.com/Download/index.aspx

Verify GPU is visible inside WSL2:

```bash
nvidia-smi
```

> WSL2 shares the Windows GPU driver — no separate Linux driver needed.

### Step 3 — Install Docker Desktop

1. Download Docker Desktop for Windows: https://www.docker.com/products/docker-desktop/
2. Enable **"Use WSL 2 based engine"** in Docker Desktop settings
3. Enable your Ubuntu distro under **Resources → WSL Integration**

Verify:

```bash
docker --version
docker run --rm --gpus all nvidia/cuda:12.2.0-base-ubuntu22.04 nvidia-smi
```

### Step 4 — Install NVIDIA Container Toolkit

Inside WSL2 Ubuntu terminal:

```bash
curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | \
  sudo gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg

curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list | \
  sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' | \
  sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list

sudo apt-get update
sudo apt-get install -y nvidia-container-toolkit
sudo nvidia-ctk runtime configure --runtime=docker
sudo systemctl restart docker
```

### Step 5 — Set up X11 display (for visualization)

Install **VcXsrv** on Windows: https://sourceforge.net/projects/vcxsrv/

Launch XLaunch with:
- Multiple windows
- Start no client
- ✅ **Disable access control** (required)

Then in WSL2:

```bash
export DISPLAY=$(cat /etc/resolv.conf | grep nameserver | awk '{print $2}'):0.0
xhost +
```

Add to `~/.bashrc` to persist:

```bash
echo 'export DISPLAY=$(cat /etc/resolv.conf | grep nameserver | awk '"'"'{print $2}'"'"'):0.0' >> ~/.bashrc
```

### Step 6 — Clone and build

```bash
git clone <repository-url>
cd ds-pipeline

# Build Docker image (~15-20 min first time)
docker build -t ds-pipeline:latest .
```

---

## Setup — GCP VM (Linux)

### Step 1 — Create a GPU VM

In GCP Console or via gcloud:

```bash
gcloud compute instances create ds-pipeline-vm \
  --zone=us-central1-a \
  --machine-type=n1-standard-8 \
  --accelerator=type=nvidia-tesla-t4,count=1 \
  --image-family=ubuntu-2204-lts \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=100GB \
  --maintenance-policy=TERMINATE \
  --metadata="install-nvidia-driver=true"
```

SSH into the VM:

```bash
gcloud compute ssh ds-pipeline-vm --zone=us-central1-a
```

### Step 2 — Install NVIDIA Driver

```bash
# Add NVIDIA package repository
curl -fsSL https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2204/x86_64/cuda-keyring_1.1-1_all.deb \
  -o cuda-keyring.deb
sudo dpkg -i cuda-keyring.deb
sudo apt-get update

# Install driver
sudo apt-get install -y nvidia-driver-535
sudo reboot
```

After reboot, verify:

```bash
nvidia-smi
```

### Step 3 — Install Docker

```bash
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
newgrp docker
docker --version
```

### Step 4 — Install NVIDIA Container Toolkit

```bash
curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | \
  sudo gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg

curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list | \
  sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' | \
  sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list

sudo apt-get update
sudo apt-get install -y nvidia-container-toolkit
sudo nvidia-ctk runtime configure --runtime=docker
sudo systemctl restart docker

# Verify
docker run --rm --gpus all nvidia/cuda:12.2.0-base-ubuntu22.04 nvidia-smi
```

### Step 5 — Set up virtual display (headless visualization)

For GCP VMs without a physical display, use Xvfb:

```bash
sudo apt-get install -y xvfb x11-utils
Xvfb :99 -screen 0 1920x1080x24 &
export DISPLAY=:99
```

Add to `~/.bashrc`:

```bash
echo 'export DISPLAY=:99' >> ~/.bashrc
```

To view the output remotely, use X11 forwarding when SSHing:

```bash
gcloud compute ssh ds-pipeline-vm --zone=us-central1-a -- -X
```

### Step 6 — Clone and build

```bash
git clone <repository-url>
cd ds-pipeline

# Build Docker image (~15-20 min first time)
docker build -t ds-pipeline:latest .
```

---

## Model Preparation

### Step 1 — Place model weights

Copy your custom fire/smoke model to the project root:

```bash
cp /path/to/fire_smoke.pt ./fire_smoke.pt
```

`yolo11n.pt` and `yolo11n-pose.pt` are downloaded automatically by `setup.sh`.

### Step 2 — Start the container

**WSL2:**
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

**GCP VM (headless):**
```bash
docker run -it --rm \
  --runtime=nvidia \
  --gpus all \
  -e DISPLAY=:99 \
  -v /tmp/.X11-unix:/tmp/.X11-unix \
  -v $(pwd):/app \
  -w /app \
  ds-pipeline:latest bash
```

### Step 3 — Run setup.sh inside the container

```bash
bash setup.sh
```

This exports PT → ONNX and builds TensorRT FP16 engines. Takes 5–15 min per model on first run.

To use a custom fire/smoke model path:

```bash
FIRE_SMOKE_PT=/path/to/my_model.pt bash setup.sh
```

### Step 4 — Verify engines

```bash
ls -lh models/*.engine
# Expected:
# models/fire_smoke_b1.engine   ~15 MB
# models/yolo11n_b1.engine       ~9 MB
# models/yolo11n-pose_b1.engine ~10 MB
```

---

## Running the Pipeline

### Add video sources

```bash
mkdir -p videos
cp /path/to/video1.mp4 videos/sample1.mp4
# ... repeat for sample2.mp4, sample3.mp4, sample4.mp4
```

Or set RTSP sources via environment variables:

```bash
export STREAM_URI_1=rtsp://192.168.1.10:554/stream
export STREAM_URI_2=rtsp://192.168.1.11:554/stream
export STREAM_URI_3=rtsp://192.168.1.12:554/stream
export STREAM_URI_4=rtsp://192.168.1.13:554/stream
```

### Basic run

```bash
# 4 streams, default config from rules.json
python3 main.py --streams 4

# With output recording
python3 main.py --streams 4 --output output/demo.mp4

# Reduce GPU load (infer every 3rd frame)
python3 main.py --streams 4 --interval 2

# Maximum quality (infer every frame)
python3 main.py --streams 4 --interval 0

# Load test with 8 streams
python3 main.py --streams 8 --interval 4
```

---

## Stream Configuration

### Option 1 — Edit `configs/rules.json` (permanent)

Each stream entry supports:

```json
"0": {
    "id":       "CAM_01",
    "location": "Entrance",
    "models": [
        {
            "name":     "coco",
            "detect":   ["person"],
            "features": ["fall_detection"]
        },
        {
            "name":   "fire_smoke",
            "detect": ["fire", "smoke"]
        }
    ],
    "crowd": {"low": 3, "medium": 8, "high": 15, "critical": 25}
}
```

**Available models:** `coco`, `fire_smoke`

**Available detect classes (coco):** `person`, `bird`, `cat`, `dog`, `horse`, `sheep`, `cow`, `elephant`, `bear`, `zebra`, `giraffe`

**Available detect classes (fire_smoke):** `fire`, `smoke`

**Available features:** `fall_detection`, `crowd_density`

### Option 2 — Use a different rules file (no default file change)

```bash
python3 main.py --streams 4 --config configs/rules_night.json
```

### Option 3 — Inline CLI override (no file edit needed)

Override one or more streams at runtime using `--stream-config N:JSON`:

```bash
# Change stream 3 to detect only fire
python3 main.py --streams 4 \
  --stream-config '2:{"models":[{"name":"fire_smoke","detect":["fire"]}]}'

# Override all 4 streams
python3 main.py --streams 4 \
  --stream-config '0:{"models":[{"name":"coco","detect":["person"],"features":["fall_detection"]}]}' \
  --stream-config '1:{"models":[{"name":"coco","detect":["cow","horse","sheep"]}]}' \
  --stream-config '2:{"models":[{"name":"fire_smoke","detect":["fire","smoke"]}]}' \
  --stream-config '3:{"models":[{"name":"coco","detect":["person"],"features":["crowd_density"]}]}'

# Stream with multiple models simultaneously
python3 main.py --streams 4 \
  --stream-config '0:{"models":[{"name":"coco","detect":["person"],"features":["fall_detection"]},{"name":"fire_smoke","detect":["fire","smoke"]}]}'
```

> Stream index in `--stream-config` is **0-based** (stream1 = `0`, stream2 = `1`, etc.)

### Auto-generated streams

Streams beyond what's defined in `rules.json` are auto-generated from `stream_template`:

```json
"stream_template": {
    "models": [{"name": "coco", "detect": ["person"]}]
}
```

This allows running `--streams 20` without pre-defining all 20 entries.

---

## CLI Reference

```
python3 main.py [OPTIONS]

Options:
  --streams N          Number of video streams (default: 4, no upper limit)
  --output PATH        Save tiled output to MP4 file
  --interval N         nvinfer frame skip: 0=every frame, 2=every 3rd (default), 4=every 5th
  --config PATH        Path to rules.json (default: configs/rules.json)
  --stream-config N:JSON  Override stream N config inline (repeatable)
```

**Environment variables:**

```bash
STREAM_URI_1=rtsp://...   # Source URI for stream 1 (default: videos/sample1.mp4)
STREAM_URI_2=rtsp://...   # Source URI for stream 2
# ... up to STREAM_URI_N
DISPLAY=:0                # X11 display for visualization
```

---

## Project Structure

```
ds-pipeline/
├── main.py                          # Entry point
├── setup.sh                         # Model export + TRT engine build
├── Dockerfile                       # Container definition
├── requirements.txt                 # Python dependencies reference
├── labels.txt                       # COCO class labels (80 classes)
├── .gitignore
├── README.md
│
├── pipeline/                        # Core pipeline package
│   ├── __init__.py
│   ├── constants.py                 # Config paths, class sets, thresholds
│   ├── gst_pipeline.py              # GStreamer orchestration
│   ├── probe_handler.py             # GStreamer probe callbacks
│   ├── detection.py                 # NMS, tensor parsing, OSD injection
│   ├── fall_detector.py             # Pose keypoint fall analysis
│   ├── crowd_monitor.py             # Crowd density zone classification
│   ├── event_emitter.py             # Event deduplication + alerts
│   └── display.py                   # Console dashboard + benchmark
│
├── utils/
│   └── rule_engine.py               # Stream config loader
│
├── configs/
│   ├── rules.json                   # Stream assignments + model config
│   ├── config_infer_primary_yolov11.txt  # COCO nvinfer config (GIE_1)
│   ├── config_infer_fire_smoke.txt       # Fire/smoke nvinfer config (GIE_2)
│   ├── config_infer_pose.txt             # Pose nvinfer config (GIE_4)
│   └── config_tracker_NvDCF.yml          # Tracker config
│
├── models/                          # TRT engines + ONNX (gitignored)
├── videos/                          # Input video files (gitignored)
└── output/                          # Recorded output (gitignored)
```

---

## Troubleshooting

### No display window (WSL2)

```bash
# On Windows host, run before starting container:
xhost +local:docker

# Verify DISPLAY is set inside container:
echo $DISPLAY   # should show 172.x.x.x:0.0 or :0
```

### No display window (GCP VM)

```bash
# Start virtual display
Xvfb :99 -screen 0 1920x1080x24 &
export DISPLAY=:99

# Or use fakesink (no display, console output only)
# The pipeline auto-falls back to fakesink when DISPLAY is not set
```

### Engine rebuild on every run

The engine file is missing or was built with different parameters:

```bash
rm -f models/*.engine
bash setup.sh
```

### GPU at 100% / low FPS

```bash
# Reduce inference frequency
python3 main.py --streams 4 --interval 4

# Reduce stream count
python3 main.py --streams 2 --interval 2
```

### `pyds` not found

DeepStream Python bindings not installed. Rebuild the image:

```bash
docker build --no-cache -t ds-pipeline:latest .
```

### RTSP stream not connecting

```bash
# Test RTSP URL inside container
gst-launch-1.0 uridecodebin uri=rtsp://your-ip/stream ! fakesink
```

### `libavcodec.so` warning on startup

Non-fatal. If it causes issues:

```bash
apt-get install -y ffmpeg libavcodec-dev libavformat-dev
```

### Tensor all zeros (no detections)

The TRT engine has a dynamic output shape issue. Delete and rebuild:

```bash
rm -f models/yolo11n_b1.engine
bash setup.sh
```
