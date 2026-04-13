# NVIDIA DeepStream Multi-Stream AI Pipeline

A production-grade DeepStream 7.0 Python pipeline for real-time multi-stream AI inference. Supports unlimited streams, dynamic per-stream model configuration, fall detection, fire/smoke detection, crowd density monitoring, and Kafka event streaming.

---

## Table of Contents

1. [Features](#features)
2. [Architecture](#architecture)
3. [Prerequisites](#prerequisites)
4. [Setup — WSL2 (Windows)](#setup--wsl2-windows)
5. [Setup — GCP VM (Linux)](#setup--gcp-vm-linux)
6. [Kafka Setup](#kafka-setup)
7. [Model Preparation](#model-preparation)
8. [Running the Pipeline](#running-the-pipeline)
9. [Stream Configuration](#stream-configuration)
10. [CLI Reference](#cli-reference)
11. [Project Structure](#project-structure)
12. [Troubleshooting](#troubleshooting)

---

## Features

- **Unlimited streams** — configurable at runtime, GPU memory is the only limit
- **3 parallel TRT models** — COCO detection (GIE_1), fire/smoke (GIE_2), pose estimation (GIE_4)
- **Fall detection** — YOLOv11-pose keypoint torso angle analysis with temporal window confirmation
- **Crowd density** — rolling person count with zone classification (CLEAR/LOW/MEDIUM/HIGH/CRITICAL)
- **Per-stream model assignment** — each stream can run any combination of models and features
- **Dynamic CLI configuration** — override any stream's models/features without editing files
- **Kafka event streaming** — all detections and alerts published to `ds.*` topics in real time
- **Live benchmark** — per-stream FPS, inference latency, GPU%, CPU%, VRAM
- **Tiled OSD display** — bounding boxes, pose skeleton, crowd overlay
- **Optional MP4 recording** — CPU x264 encoder (no extra GPU memory)

---

## Architecture

```
4–N Video Streams (file:// or rtsp://)
      │
      ▼
 nvstreammux (GPU batch)
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
      ├──► Kafka Producer           ← ds.detections / ds.alerts / ds.heartbeat
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

**Kafka topics:**

| Topic | Content |
|-------|---------|
| `ds.detections` | Every bounding box from all streams and models |
| `ds.alerts` | Deduplicated fall / fire / smoke / crowd alerts |
| `ds.heartbeat` | GPU stats, FPS, active stream count (every 5s) |
| `ds.benchmark` | Per-stream inference metrics (every 5s) |

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
| Python (WSL host) | 3.8+ |

---

## Setup — WSL2 (Windows)

### Step 1 — Enable WSL2 and install Ubuntu

```powershell
wsl --install -d Ubuntu-22.04
wsl --set-default-version 2
```

Restart, then open Ubuntu from the Start menu.

### Step 2 — Install NVIDIA Driver on Windows host

Download from https://www.nvidia.com/Download/index.aspx and verify:

```bash
nvidia-smi
```

### Step 3 — Install Docker Desktop

1. Download from https://www.docker.com/products/docker-desktop/
2. Enable **"Use WSL 2 based engine"** in Docker Desktop settings
3. Enable your Ubuntu distro under **Resources → WSL Integration**

```bash
docker --version
docker run --rm --gpus all nvidia/cuda:12.2.0-base-ubuntu22.04 nvidia-smi
```

### Step 4 — Install NVIDIA Container Toolkit

```bash
curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | \
  sudo gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg

curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list | \
  sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' | \
  sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list

sudo apt-get update && sudo apt-get install -y nvidia-container-toolkit
sudo nvidia-ctk runtime configure --runtime=docker
sudo systemctl restart docker
```

### Step 5 — Set up X11 display (for visualization)

Install **VcXsrv** from https://sourceforge.net/projects/vcxsrv/ and launch with "Disable access control" checked.

```bash
export DISPLAY=$(cat /etc/resolv.conf | grep nameserver | awk '{print $2}'):0.0
xhost +
```

### Step 6 — Clone and build

```bash
git clone <repository-url>
cd ds-pipeline
docker build -t ds-pipeline:latest .
```

---

## Setup — GCP VM (Linux)

### Step 1 — Create a GPU VM

```bash
gcloud compute instances create ds-pipeline-vm \
  --zone=us-central1-a \
  --machine-type=n1-standard-8 \
  --accelerator=type=nvidia-tesla-t4,count=1 \
  --image-family=ubuntu-2204-lts \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=100GB \
  --maintenance-policy=TERMINATE
```

### Step 2 — Install NVIDIA Driver

```bash
curl -fsSL https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2204/x86_64/cuda-keyring_1.1-1_all.deb -o cuda-keyring.deb
sudo dpkg -i cuda-keyring.deb && sudo apt-get update
sudo apt-get install -y nvidia-driver-535
sudo reboot
```

### Step 3 — Install Docker + NVIDIA Container Toolkit

```bash
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER && newgrp docker

curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | \
  sudo gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg
curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list | \
  sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' | \
  sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list
sudo apt-get update && sudo apt-get install -y nvidia-container-toolkit
sudo nvidia-ctk runtime configure --runtime=docker && sudo systemctl restart docker
```

### Step 4 — Virtual display (headless)

```bash
sudo apt-get install -y xvfb
Xvfb :99 -screen 0 1920x1080x24 &
export DISPLAY=:99
```

### Step 5 — Clone and build

```bash
git clone <repository-url>
cd ds-pipeline
docker build -t ds-pipeline:latest .
```

---

## Kafka Setup

Kafka is optional. When `--kafka-broker` is not passed the pipeline runs in console-only mode.

### Step 1 — Start Kafka (WSL / Linux)

```bash
docker compose -f docker-compose.kafka.yml up -d
```

Wait ~15 seconds, then verify:

```bash
docker compose -f docker-compose.kafka.yml ps
# ds-kafka should show (healthy)
```

### Step 2 — Install Python client on WSL host

```bash
pip3 install confluent-kafka
```

> `confluent-kafka` is already included in the DeepStream Docker image via the Dockerfile.

### Step 3 — Create topics

```bash
python3 -c "from pipeline.kafka_producer import ensure_topics; ensure_topics('localhost:9092')"
```

### Step 4 — Run the pipeline with Kafka enabled

The DeepStream container runs with `--network host`, so `localhost:9092` works from inside it:

```bash
docker exec -it -w /app <container_name> \
  python3 main.py --streams 4 --kafka-broker localhost:9092
```

You should see:
```
[Kafka] broker=localhost:9092  server_id=server_1  gpu_id=0
[Kafka] producer connected → localhost:9092
```

### Step 5 — Run the consumer (WSL terminal)

```bash
# Alerts and heartbeat only
python3 kafka_consumer.py --broker localhost:9092 --topics ds.alerts ds.heartbeat

# All topics
python3 kafka_consumer.py --broker localhost:9092

# Save to file
python3 kafka_consumer.py --broker localhost:9092 --output-file events.jsonl
```

### Step 6 — Test without DeepStream (simulator)

```bash
python3 kafka_test_producer.py --broker localhost:9092
```

### Kafka UI

Open http://localhost:8080 to browse topics and messages visually.

> For the full Kafka setup guide including SASL/SSL, multi-GPU, remote consumption, and troubleshooting see [KAFKA_SETUP_GUIDE.md](KAFKA_SETUP_GUIDE.md).

---

## Model Preparation

### Step 1 — Place model weights

```bash
cp /path/to/fire_smoke.pt ./fire_smoke.pt
```

`yolo11n.pt` and `yolo11n-pose.pt` are downloaded automatically by `setup.sh`.

### Step 2 — Start the container

```bash
# WSL2
docker run -it --rm --network host \
  --runtime=nvidia --gpus all \
  -e DISPLAY=$DISPLAY \
  -v /tmp/.X11-unix:/tmp/.X11-unix \
  -v $(pwd):/app -w /app \
  ds-pipeline:latest bash

# GCP headless
docker run -it --rm --network host \
  --runtime=nvidia --gpus all \
  -e DISPLAY=:99 \
  -v $(pwd):/app -w /app \
  ds-pipeline:latest bash
```

> `--network host` is required for the container to reach Kafka on `localhost:9092`.

### Step 3 — Build TRT engines

```bash
bash setup.sh
```

Takes 5–15 min per model on first run.

### Step 4 — Verify engines

```bash
ls -lh models/*.engine
# models/fire_smoke_b1.engine   ~15 MB
# models/yolo11n_b1.engine       ~9 MB
# models/yolo11n-pose_b1.engine ~10 MB
```

---

## Running the Pipeline

### Add video sources

```bash
cp /path/to/video1.mp4 videos/sample1.mp4
# repeat for sample2–4
```

Or use RTSP:

```bash
export STREAM_URI_1=rtsp://192.168.1.10:554/stream
export STREAM_URI_2=rtsp://192.168.1.11:554/stream
```

### Basic run (console only)

```bash
python3 main.py --streams 4
```

### With Kafka

```bash
python3 main.py --streams 4 --kafka-broker localhost:9092
```

### With recording

```bash
python3 main.py --streams 4 --kafka-broker localhost:9092 --output output/demo.mp4
```

### Reduce GPU load

```bash
python3 main.py --streams 8 --interval 4 --kafka-broker localhost:9092
```

---

## Stream Configuration

### Edit `configs/rules.json` (permanent)

```json
"0": {
    "id": "CAM_01",
    "location": "Entrance",
    "models": [
        { "name": "coco",       "detect": ["person"], "features": ["fall_detection"] },
        { "name": "fire_smoke", "detect": ["fire", "smoke"] }
    ],
    "crowd": {"low": 3, "medium": 8, "high": 15, "critical": 25}
}
```

**Available models:** `coco`, `fire_smoke`
**COCO classes:** `person`, `bird`, `cat`, `dog`, `horse`, `sheep`, `cow`, `elephant`, `bear`, `zebra`, `giraffe`
**fire_smoke classes:** `fire`, `smoke`
**Features:** `fall_detection`, `crowd_density`

### Inline CLI override (no file edit)

```bash
python3 main.py --streams 4 \
  --stream-config '2:{"models":[{"name":"fire_smoke","detect":["fire","smoke"]}]}'
```

> Stream index is 0-based. See [CLI Reference](#cli-reference) for full options.

---

## CLI Reference

```
python3 main.py [OPTIONS]

Pipeline:
  --streams N              Number of video streams (default: 4)
  --output PATH            Save tiled output to MP4
  --interval N             Frame skip: 0=every frame, 2=every 3rd (default), 4=every 5th
  --config PATH            Path to rules.json (default: configs/rules.json)
  --stream-config N:JSON   Override stream N config inline (repeatable)

Kafka:
  --kafka-broker HOST:PORT     Kafka bootstrap server (omit to disable)
  --kafka-server-id ID         Server identifier in event payloads (default: server_1)
  --kafka-gpu-id N             GPU index in event payloads (default: 0)
  --kafka-security-protocol    PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL
  --kafka-sasl-mechanism       PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512
  --kafka-sasl-username        SASL username
  --kafka-sasl-password        SASL password
  --kafka-ssl-ca PATH          CA certificate path for TLS
```

**Environment variables:**

| Variable | Description |
|----------|-------------|
| `STREAM_URI_1` … `N` | Source URI (rtsp:// or file://) |
| `KAFKA_BROKER` | Kafka broker address |
| `SERVER_ID` | Server identifier |
| `GPU_ID` | GPU index |
| `DISPLAY` | X11 display |

---

## Project Structure

```
ds-pipeline/
├── main.py                          # Entry point
├── setup.sh                         # Model export + TRT engine build
├── Dockerfile                       # Container definition
├── docker-compose.kafka.yml         # Kafka + Zookeeper + Kafka UI stack
├── kafka_consumer.py                # Platform Server consumer
├── kafka_test_producer.py           # Event simulator (no GPU needed)
├── labels.txt                       # COCO class labels (80 classes)
├── .gitignore
├── README.md
├── KAFKA_SETUP_GUIDE.md             # Full Kafka setup and troubleshooting guide
│
├── pipeline/
│   ├── __init__.py
│   ├── constants.py                 # Config paths, class sets, thresholds
│   ├── gst_pipeline.py              # GStreamer orchestration
│   ├── probe_handler.py             # GStreamer probe callbacks
│   ├── detection.py                 # NMS, tensor parsing, OSD injection
│   ├── fall_detector.py             # Pose keypoint fall analysis
│   ├── crowd_monitor.py             # Crowd density zone classification
│   ├── event_emitter.py             # Event deduplication + Kafka routing
│   ├── kafka_producer.py            # Async Kafka producer
│   └── display.py                   # Console dashboard + benchmark
│
├── utils/
│   └── rule_engine.py               # Stream config loader
│
├── configs/
│   ├── rules.json                   # Stream assignments + model config
│   ├── config_infer_primary_yolov11.txt
│   ├── config_infer_fire_smoke.txt
│   ├── config_infer_pose.txt
│   └── config_tracker_NvDCF.yml
│
├── models/                          # TRT engines + ONNX (gitignored)
├── videos/                          # Input video files (gitignored)
└── output/                          # Recorded output (gitignored)
```

---

## Troubleshooting

### Kafka: no events received by consumer

```bash
# 1. Verify topics exist
docker exec ds-kafka kafka-topics --bootstrap-server localhost:9092 --list

# 2. Check messages are in the topic
docker exec ds-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic ds.alerts \
  --from-beginning --max-messages 5 --timeout-ms 5000

# 3. Ensure confluent-kafka is installed in WSL
pip3 install confluent-kafka

# 4. Ensure pipeline was started with --kafka-broker
python3 main.py --streams 4 --kafka-broker localhost:9092
```

### Kafka: `confluent_kafka` not found inside container

```bash
docker exec <container> pip3 install confluent-kafka
# Or rebuild the image — it's now in the Dockerfile
docker build -t ds-pipeline:latest .
```

### Kafka: producer disabled warning at startup

The `--kafka-broker` flag was not passed. Always include it:
```bash
python3 main.py --streams 4 --kafka-broker localhost:9092
```

### No display window (WSL2)

```bash
xhost +local:docker
echo $DISPLAY   # should show an IP:0.0 address
```

### No display window (GCP VM)

```bash
Xvfb :99 -screen 0 1920x1080x24 &
export DISPLAY=:99
# Pipeline auto-falls back to fakesink when DISPLAY is not set
```

### Engine rebuild on every run

```bash
rm -f models/*.engine && bash setup.sh
```

### GPU at 100% / low FPS

```bash
python3 main.py --streams 4 --interval 4
```

### `pyds` not found

```bash
docker build --no-cache -t ds-pipeline:latest .
```

### RTSP stream not connecting

```bash
gst-launch-1.0 uridecodebin uri=rtsp://your-ip/stream ! fakesink
```

### Tensor all zeros (no detections)

```bash
rm -f models/yolo11n_b1.engine && bash setup.sh
```
