# NVIDIA DeepStream 3-Stage Inference Cascade with YOLOv11

This project implements a complete DeepStream 7.0 pipeline using Docker and Python. It processes 4 simultaneous video streams through a 2-stage inference cascade:
1.  **Stage 1 (Primary):** Person Detection using YOLOv11.
2.  **Stage 2a (Secondary):** Fall Detection (ResNet-18 Placeholder) operating on detected persons.

*Note: Crowd Density (Stage 2b) is currently disabled.*

## Features
- **4-Stream Muxing:** Handles 4 RTSP or local file sources in parallel.
- **Cascaded Inference:** Chain of GIEs (Primary + 2 Secondaries).
- **Tracking:** Independent person tracking using NvDCF.
- **Rules Engine:** Configurable alerts based on detections, confidence, and crowd density.
- **Output:** Structured JSON metadata and event logging.

## Prerequisites
- **Hardware:** NVIDIA GPU (T4, RTX 3060+, or Jetson Orin).
- **Host OS:** WSL2 (Windows) or Ubuntu 22.04.
- **Drivers:** NVIDIA Driver 535+ installed.
- **Tools:**
    - [NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html)
    - Docker & Docker Compose.

## Setup Instructions

### 1. Build the Docker Image
First, build the container which includes all dependencies and DeepStream Python bindings:
```bash
docker-compose build
```

### 2. Model Preparation
Run the `setup.sh` script **inside the container** to download weights and convert them to TensorRT engines. This ensures all tools like `trtexec` and `ultralytics` are available:
```bash
docker-compose run --rm deepstream-pipeline /app/setup.sh
```

### 3. Launch the Pipeline
Start the 4-stream pipeline:
```bash
docker-compose up
```

### 3. Configuration
- **Sources:** Modify `docker-compose.yml` environment variables `STREAM_URI_1` to `STREAM_URI_4` to point to your RTSP streams or local video files.
- **Rules:** Edit `rules.yaml` to adjust alert thresholds, crowd limits, and webhook URLs.
- **Inference:** Adjust `configs/config_infer_*.txt` for model parameters.

## Architecture
- `nvstreammux`: Muxes the 4 sources.
- `nvinfer (PGIE)`: Detects persons (YOLOv11).
- `nvtracker`: Tracks persons across frames.
- `nvinfer (SGIE1)`: Classifies fall events on person bboxes.
- `nvinfer (SGIE2)`: Predicts crowd density on full frames.
- `Python Probes`: Extracts metadata, evaluates rules, and logs results.

## Output
- **Metadata:** All detections are logged to `output/metadata.json`.
- **Events:** Triggered alerts are logged to `output/events.json` and optionally sent to a webhook.

## Troubleshooting
- **nvtracker Error:** In DeepStream 7.0, many `nvtracker` properties like `enable-batch-process` have been removed from the GStreamer element properties and must be configured via the low-level YAML config file.
- **Missing Plugins:** Warnings about `librivermax.so` or `libavcodec.so` are common if host libraries are missing. We have added `ffmpeg` and related development libraries to the Dockerfile to minimize these.
- **GPU Not Found:** Ensure `nvidia-smi` works on the host and Docker is configured with the `nvidia` runtime (or use the `deploy` block in `docker-compose.yml`).
