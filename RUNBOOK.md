# Biological E Peripheral Monitoring — Run Commands

## Prerequisites

All commands run from the project root: `/home/kishore/code_refactored_dynamic/app`

---

## 1. First-Time Setup

### Build the DeepStream Docker image
```bash
docker build -t ds-pipeline:latest .
```

### Build TRT engines (inside container, takes 5–15 min)
```bash
export DISPLAY=$(cat /etc/resolv.conf | grep nameserver | awk '{print $2}'):0.0
xhost +

docker run -it --rm --network host \
  --runtime=nvidia --gpus all \
  -e DISPLAY=$DISPLAY \
  -v /tmp/.X11-unix:/tmp/.X11-unix \
  -v $(pwd):/app -w /app \
  ds-pipeline:latest bash

# Inside container:
bash setup.sh
exit
```

### Install dashboard dependencies (WSL host)
```bash
pip3 install -r requirements-dashboard.txt
```

---

## 2. Start Kafka

```bash
docker compose -f docker-compose.kafka.yml up -d
```

Verify (wait ~15s):
```bash
docker compose -f docker-compose.kafka.yml ps
# ds-kafka should show (healthy)
```

Create topics (first time only):
```bash
docker exec ds-kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic ds.alerts --partitions 1 --replication-factor 1

docker exec ds-kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic ds.heartbeat --partitions 1 --replication-factor 1

docker exec ds-kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic ds.benchmark --partitions 1 --replication-factor 1

docker exec ds-kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic ds.clips --partitions 1 --replication-factor 1

docker exec ds-kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic ds.detections --partitions 1 --replication-factor 1
```

Kafka UI: http://localhost:8080

---

## 3. Start the FastAPI Backend

```bash
PYTHONPATH=$(pwd) uvicorn backend.main:app --host 0.0.0.0 --port 8000 --no-access-log
```

Verify: http://localhost:8000/api/health

---

## 4. Start the Clip Writer

```bash
PYTHONPATH=$(pwd) python3 services/clip_writer.py
```

---

## 5. Start the Streamlit Dashboard

```bash
PYTHONPATH=$(pwd) streamlit run dashboard/app.py
```

Dashboard: http://localhost:8501

---

---

## 6. Start the DeepStream Pipeline

### Common Options

| Flag | Default | Description |
|------|---------|-------------|
| `--streams N` | `4` | Number of video streams |
| `--interval N` | `2` | Frame skip: `0`=every frame, `2`=every 3rd, `4`=every 5th |
| `--config PATH` | `configs/rules.json` | Stream configuration file |
| `--output PATH` | _(none)_ | Save tiled output to MP4 |
| `--kafka-broker HOST:PORT` | _(env)_ | Kafka server; omit to disable |
| `--kafka-server-id ID` | `server_1` | Server identifier in Kafka messages |
| `--kafka-gpu-id N` | `0` | GPU index in Kafka messages |
| `--kafka-detections` | _(off)_ | Also publish raw detections to `ds.detections` |
| `--stream-config N:JSON` | _(none)_ | Override stream N config inline (repeatable) |

### Basic — 4 streams, no Kafka
```bash
BACKEND_URL=http://localhost:8000 python3 main.py --streams 4
```

### Standard — 4 streams with Kafka + dashboard
```bash
BACKEND_URL=http://localhost:8000 python3 main.py \
  --streams 4 \
  --kafka-broker localhost:9092
```

### Reduce GPU load — infer every 5th frame
```bash
BACKEND_URL=http://localhost:8000 python3 main.py \
  --streams 4 \
  --interval 4 \
  --kafka-broker localhost:9092
```

### High accuracy — infer every frame
```bash
BACKEND_URL=http://localhost:8000 python3 main.py \
  --streams 2 \
  --interval 0 \
  --kafka-broker localhost:9092
```

### Many streams — 8 streams with reduced inference
```bash
BACKEND_URL=http://localhost:8000 python3 main.py \
  --streams 8 \
  --interval 4 \
  --kafka-broker localhost:9092
```

### Custom rules file
```bash
BACKEND_URL=http://localhost:8000 python3 main.py \
  --streams 4 \
  --config configs/rules_night.json \
  --kafka-broker localhost:9092
```

### Record tiled output to MP4
```bash
BACKEND_URL=http://localhost:8000 python3 main.py \
  --streams 4 \
  --output output/recording.mp4 \
  --kafka-broker localhost:9092
```

### Override a single stream inline (no file edit)
```bash
# Override stream 2 to use fire_smoke model only
BACKEND_URL=http://localhost:8000 python3 main.py \
  --streams 4 \
  --stream-config '2:{"models":[{"name":"fire_smoke","detect":["fire","smoke"]}]}' \
  --kafka-broker localhost:9092

# Override multiple streams
BACKEND_URL=http://localhost:8000 python3 main.py \
  --streams 4 \
  --stream-config '0:{"models":[{"name":"coco","detect":["person"],"features":["fall_detection"]}]}' \
  --stream-config '1:{"models":[{"name":"fire_smoke","detect":["fire","smoke"]}]}' \
  --kafka-broker localhost:9092
```

### Enable raw detection publishing (high volume)
```bash
BACKEND_URL=http://localhost:8000 python3 main.py \
  --streams 4 \
  --kafka-broker localhost:9092 \
  --kafka-detections
```

### Enterprise Kafka with TLS/SASL
```bash
BACKEND_URL=http://localhost:8000 python3 main.py \
  --streams 4 \
  --kafka-broker broker.prod:9093 \
  --kafka-security-protocol SASL_SSL \
  --kafka-sasl-mechanism SCRAM-SHA-512 \
  --kafka-sasl-username myuser \
  --kafka-sasl-password mypassword \
  --kafka-ssl-ca /etc/ssl/certs/ca.pem
```

### RTSP streams (set env vars before running)
```bash
export STREAM_URI_1=rtsp://192.168.1.10:554/stream1
export STREAM_URI_2=rtsp://192.168.1.11:554/stream2
export STREAM_URI_3=rtsp://192.168.1.12:554/stream3
export STREAM_URI_4=rtsp://192.168.1.13:554/stream4

BACKEND_URL=http://localhost:8000 python3 main.py \
  --streams 4 \
  --kafka-broker localhost:9092
```

### Inside Docker container (recommended for production)
```bash
export DISPLAY=$(cat /etc/resolv.conf | grep nameserver | awk '{print $2}'):0.0
xhost +

docker run -it --rm --network host \
  --runtime=nvidia --gpus all \
  -e DISPLAY=$DISPLAY \
  -e BACKEND_URL=http://localhost:8000 \
  -v /tmp/.X11-unix:/tmp/.X11-unix \
  -v $(pwd):/app -w /app \
  ds-pipeline:latest \
  python3 main.py --streams 4 --kafka-broker localhost:9092
```

### Headless (no display window, dashboard only)
```bash
# Unset DISPLAY so pipeline uses fakesink automatically
unset DISPLAY

BACKEND_URL=http://localhost:8000 python3 main.py \
  --streams 4 \
  --kafka-broker localhost:9092
```

---

## 7. All Services — Quick Reference

| Terminal | Command |
|----------|---------|
| 1 | `docker compose -f docker-compose.kafka.yml up -d` |
| 2 | `PYTHONPATH=$(pwd) uvicorn backend.main:app --host 0.0.0.0 --port 8000 --no-access-log` |
| 3 | `PYTHONPATH=$(pwd) python3 services/clip_writer.py` |
| 4 | `PYTHONPATH=$(pwd) streamlit run dashboard/app.py` |
| 5 | `BACKEND_URL=http://localhost:8000 python3 main.py --streams 4 --kafka-broker localhost:9092` |

---

## 8. Stop Everything

```bash
# Stop pipeline (Ctrl+C in terminal 5, or from Dashboard → Pipeline Control → Stop)

# Stop dashboard, clip writer, backend (Ctrl+C in each terminal)

# Stop Kafka
docker compose -f docker-compose.kafka.yml down
```

---

## 9. Environment Variables (.env)

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKER` | `localhost:9092` | Kafka bootstrap server |
| `BACKEND_URL` | `http://localhost:8000` | FastAPI backend URL |
| `CLIPS_DIR` | `output/clips` | Directory for MP4 clips |
| `BACKEND_PORT` | `8000` | Backend listen port |
| `PIPELINE_LAUNCHER` | `subprocess` | `subprocess` or `docker` |
| `PIPELINE_CONTAINER` | `ds-pipeline` | Docker container name |
| `BUFFER_FRAMES` | `90` | Clip writer frame buffer size |
| `CLIP_FRAME_SOURCE` | `queue` | `queue` (PoC) or `appsink` (prod) |

---

## 10. Useful Debug Commands

```bash
# Check pipeline log (when launched via docker exec)
docker exec ds-pipeline cat /tmp/pipeline.log

# Check Kafka topics
docker exec ds-kafka kafka-topics --bootstrap-server localhost:9092 --list

# Consume alerts live
docker exec ds-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic ds.alerts --from-beginning

# Test snapshot endpoint
curl -I "http://localhost:8000/api/snapshot/0?mode=osd"

# Check health
curl -s http://localhost:8000/api/health | python3 -m json.tool

# Check active streams
curl -s http://localhost:8000/api/active_streams

# View recent alerts
curl -s "http://localhost:8000/api/events?event_type=alert&limit=5" | python3 -m json.tool
```

---

## 11. Dashboard Pages

| Page | URL | Description |
|------|-----|-------------|
| Live Monitor | http://localhost:8501 | MJPEG live feed + real-time alerts |
| Event Log | sidebar → Event Log | Full filterable event history |
| Event Clips | sidebar → Event Clips | Play recorded event clips |
| Pipeline Control | sidebar → Pipeline Control | Configure and launch pipeline |

---

## 12. Stream Configuration Reference (rules.json)

### Available Models

| Model | Classes | Features |
|-------|---------|----------|
| `coco` | person, bird, cat, dog, horse, sheep, cow, elephant, bear, zebra, giraffe | `fall_detection`, `crowd_density` |
| `fire_smoke` | fire, smoke | — |
| `combined` | cat, chicken, cow, dog, fire, horse, people, sheep, smoke | — |
| `pose` | _(keypoints only, used internally for fall detection)_ | — |

### Stream config structure
```json
"0": {
    "id":       "CAM_01",
    "location": "Main_Entrance",
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
    "crowd": {"low": 3, "medium": 8, "high": 15, "critical": 25},
    "description": "Entrance — person + fire/smoke"
}
```

### Inline override examples
```bash
# Stream 0: person + fall detection
--stream-config '0:{"models":[{"name":"coco","detect":["person"],"features":["fall_detection"]}]}'

# Stream 1: fire and smoke only
--stream-config '1:{"models":[{"name":"fire_smoke","detect":["fire","smoke"]}]}'

# Stream 2: all animals
--stream-config '2:{"models":[{"name":"coco","detect":["cow","horse","sheep","elephant","bear","giraffe","zebra"]}]}'

# Stream 3: crowd density
--stream-config '3:{"models":[{"name":"coco","detect":["person"],"features":["crowd_density"]}],"crowd":{"low":5,"medium":15,"high":30,"critical":50}}'
```

### STREAM_URI environment variables
```bash
# File sources (default pattern: videos/sampleN.mp4)
export STREAM_URI_1=file:///app/videos/sample1.mp4
export STREAM_URI_2=file:///app/videos/sample2.mp4

# RTSP sources
export STREAM_URI_1=rtsp://192.168.1.10:554/ch0
export STREAM_URI_2=rtsp://192.168.1.11:554/ch0

# Mixed
export STREAM_URI_1=file:///app/videos/sample1.mp4
export STREAM_URI_2=rtsp://192.168.1.11:554/ch0
```
