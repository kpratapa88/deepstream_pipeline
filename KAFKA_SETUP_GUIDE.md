# Kafka Integration — Step-by-Step Setup Guide

---

## Why the Consumer Wasn't Receiving Messages (Root Cause)

This is a classic Kafka advertised listener trap in WSL2 + Docker.

Kafka has two addresses:
1. The **bootstrap address** — what you connect to initially
2. The **advertised address** — what Kafka tells your client to reconnect to for actual data

The original config had `KAFKA_ADVERTISED_LISTENERS: PLAINTEXT_HOST://localhost:9092`.

What happens:
- DeepStream container connects to `localhost:9092` → bootstrap OK → Kafka says "reconnect to `localhost:9092`" → inside the container `localhost` = the container itself, not Kafka → **connection fails silently, messages never arrive**
- WSL consumer connects to `localhost:9092` → bootstrap OK → Kafka says "reconnect to `localhost:9092`" → in WSL `localhost` = WSL host, not the Kafka container → **same silent failure**

The fix is three separate listeners, each advertising the correct address for its access path:

```
INTERNAL      kafka:29092      → container-to-container (kafka-ui, etc.)
LOCALHOST     localhost:9092   → WSL host processes (kafka_consumer.py)
DOCKER_HOST   172.17.0.1:9093  → DeepStream container via Docker bridge IP
```

`172.17.0.1` is the Docker bridge gateway — always reachable from any container back to the host, and from the host to containers.

---

## Network Map (WSL2 Setup)

```
WSL2 Host
├── kafka_consumer.py          → localhost:9092  (LOCALHOST listener)
│
├── Docker Engine
│   ├── ds-kafka container     ← listens on 0.0.0.0:9092, 9093, 29092
│   ├── ds-zookeeper container
│   ├── ds-kafka-ui container  → kafka:29092     (INTERNAL listener)
│   │
│   └── ds-pipeline container  → 172.17.0.1:9093 (DOCKER_HOST listener)
│       (DeepStream)
│
└── Docker bridge: 172.17.0.1 (host-side gateway, reachable from all containers)
```

---

## What Gets Published

| Topic | Content | When |
|-------|---------|------|
| `ds.detections` | Every bounding box detection (all streams, all models) | Every probe callback |
| `ds.alerts` | Deduplicated high-priority events: fall, fire, smoke, crowd | On confirmed event (3s cooldown) |
| `ds.heartbeat` | GPU/CPU stats, FPS, active stream count | Every benchmark tick (5s) |
| `ds.benchmark` | Per-stream FPS, inference latency, detection counts | Every benchmark tick (5s) |

---

## Message Envelope (all topics)

```json
{
  "schema_version": "1.0",
  "message_id":     "uuid-v4",
  "event_type":     "fall | fire | smoke | crowd | detection | heartbeat | benchmark",
  "server_id":      "server_1",
  "gpu_id":         0,
  "hostname":       "inference-node-01",
  "timestamp_utc":  "2026-04-13T10:22:01Z",
  "timestamp_ms":   1744539721000,
  "payload":        { ... }
}
```

---

## Part 1 — Fix and Restart Kafka

### Step 1 — Confirm the Docker bridge IP is 172.17.0.1

Run this in WSL:

```bash
ip route show | grep docker
# Expected: 172.17.0.0/16 dev docker0
# or
docker network inspect bridge | grep Gateway
# Expected: "Gateway": "172.17.0.1"
```

If your gateway is different (e.g. `172.18.0.1`), update `DOCKER_HOST` in `docker-compose.kafka.yml` to match before continuing.

### Step 2 — Tear down the old Kafka stack

```bash
docker compose -f docker-compose.kafka.yml down -v
```

The `-v` removes old volumes so Kafka starts clean with the new listener config.

### Step 3 — Start the fixed stack

```bash
docker compose -f docker-compose.kafka.yml up -d
```

Wait ~20 seconds, then verify all three containers are healthy:

```bash
docker compose -f docker-compose.kafka.yml ps
```

Expected:
```
NAME            STATUS
ds-zookeeper    Up (healthy)
ds-kafka        Up (healthy)
ds-kafka-ui     Up
```

### Step 4 — Verify all three listeners are active

```bash
docker exec ds-kafka kafka-broker-api-versions --bootstrap-server localhost:9092
```

Should return API version list without errors.

```bash
# Also verify the bridge listener
docker exec ds-kafka kafka-broker-api-versions --bootstrap-server 172.17.0.1:9093
```

Both should succeed.

### Step 5 — Create topics

```bash
python3 -c "
from pipeline.kafka_producer import ensure_topics
ensure_topics('localhost:9092')
"
```

---

## Part 2 — Run the DeepStream Pipeline (Producer)

The DeepStream container must use `172.17.0.1:9093` — the Docker bridge address — not `localhost`.

### Step 6 — Start the pipeline with the correct broker address

```bash
# Inside the DeepStream container
python3 main.py --streams 4 \
  --kafka-broker 172.17.0.1:9093 \
  --kafka-server-id server_1 \
  --kafka-gpu-id 0
```

Or via environment variable (recommended):

```bash
docker run -it --rm \
  --runtime=nvidia --gpus all \
  -e KAFKA_BROKER=172.17.0.1:9093 \
  -e SERVER_ID=server_1 \
  -e GPU_ID=0 \
  -v $(pwd):/app -w /app \
  ds-pipeline:latest \
  python3 main.py --streams 4
```

You should see:
```
[Kafka] producer connected → 172.17.0.1:9093 (server=server_1, gpu=0)
```

If you see `[Kafka] producer disabled` or a connection error, the bridge IP is wrong — re-check Step 1.

---

## Part 3 — Run the Consumer (WSL Host)

The consumer runs directly in WSL (not in a container), so it uses `localhost:9092`.

### Step 7 — Install the Python client in WSL

```bash
pip install confluent-kafka
```

### Step 8 — Run the consumer

```bash
# Alerts + heartbeat only (recommended starting point)
python3 kafka_consumer.py \
  --broker localhost:9092 \
  --topics ds.alerts ds.heartbeat

# All topics (detections are counted but not printed — very noisy)
python3 kafka_consumer.py --broker localhost:9092

# Print every detection too
python3 kafka_consumer.py --broker localhost:9092 --verbose-detections

# Save everything to file
python3 kafka_consumer.py \
  --broker localhost:9092 \
  --output-file events.jsonl
```

Expected output when the pipeline is running:
```
Platform Consumer started
  topics : ds.alerts, ds.heartbeat
  Ctrl+C to stop

🚨  ALERT  [CAM_01] Main_Entrance     type=fall   conf=0.870  server=server_1 gpu=0
🔥  ALERT  [CAM_03] Factory_Floor     type=fire   conf=0.912  server=server_1 gpu=0
♥  heartbeat  server=server_1  gpu=0  streams=4  fps=112.4  gpu_util=79.1%
```

---

## Part 4 — Test Without DeepStream (Simulator)

Use this to verify the full path is working before running the GPU pipeline.

### Step 9 — Run the simulator from WSL (publishes via localhost:9092)

```bash
python3 kafka_test_producer.py --broker localhost:9092 --interval 0.5
```

In a second terminal, run the consumer:

```bash
python3 kafka_consumer.py --broker localhost:9092 --topics ds.alerts ds.heartbeat
```

You should see alerts appearing in the consumer immediately.

### Step 10 — Test the Docker bridge path (same path as DeepStream)

```bash
python3 kafka_test_producer.py --broker 172.17.0.1:9093 --count 20
```

Consumer should receive these too. This confirms the bridge listener works.

---

## Part 5 — Diagnose If Still Not Working

Run these checks in order.

### Check 1 — Can WSL reach Kafka on localhost?

```bash
nc -zv localhost 9092
# Expected: Connection to localhost 9092 port [tcp/*] succeeded!
```

### Check 2 — Can a container reach Kafka on the bridge?

```bash
docker run --rm alpine sh -c "apk add -q netcat-openbsd && nc -zv 172.17.0.1 9093"
# Expected: 172.17.0.1 (172.17.0.1:9093) open
```

### Check 3 — Are messages actually in Kafka?

```bash
docker exec ds-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic ds.alerts \
  --from-beginning \
  --max-messages 5
```

If messages appear here but not in `kafka_consumer.py`, the issue is in the consumer code or group offset.

### Check 4 — Reset consumer offset to read old messages

By default the consumer starts at `latest` (only new messages). To read everything already in the topic:

```bash
python3 kafka_consumer.py \
  --broker localhost:9092 \
  --topics ds.alerts \
  --group-id debug-$(date +%s)
```

Using a fresh `--group-id` forces offset reset to `latest` for a new group, but you can also patch the consumer temporarily:

```python
# In kafka_consumer.py, change:
"auto.offset.reset": "earliest",   # read from beginning of topic
```

### Check 5 — Check Kafka logs for listener errors

```bash
docker logs ds-kafka 2>&1 | grep -i "advertised\|listener\|error" | tail -20
```

Look for lines like:
```
INFO [SocketServer] Created data-plane acceptor and processors for endpoint : LOCALHOST://0.0.0.0:9092
INFO [SocketServer] Created data-plane acceptor and processors for endpoint : DOCKER_HOST://0.0.0.0:9093
INFO [SocketServer] Created data-plane acceptor and processors for endpoint : INTERNAL://0.0.0.0:29092
```

All three should appear. If only one or two show up, the compose file wasn't applied — re-run Step 2 and Step 3.

### Check 6 — Verify the advertised address the producer is getting

Add this temporary debug snippet inside the DeepStream container:

```python
from confluent_kafka.admin import AdminClient
a = AdminClient({"bootstrap.servers": "172.17.0.1:9093"})
meta = a.list_topics(timeout=5)
for broker in meta.brokers.values():
    print(f"Broker {broker.id}: {broker.host}:{broker.port}")
```

The host should be `172.17.0.1`, not `localhost` or `kafka`. If it shows `localhost`, the `DOCKER_HOST` listener isn't being used — check the broker address passed to the pipeline.

---

## Part 6 — Consume from a Separate Physical Machine

### Step 11 — Find the WSL2 IP from Windows

In PowerShell on Windows:

```powershell
wsl hostname -I
# e.g. 172.28.144.5
```

### Step 12 — Add a fourth listener for external access

Edit `docker-compose.kafka.yml` and add `EXTERNAL` listener:

```yaml
KAFKA_LISTENERS: >-
  INTERNAL://0.0.0.0:29092,
  LOCALHOST://0.0.0.0:9092,
  DOCKER_HOST://0.0.0.0:9093,
  EXTERNAL://0.0.0.0:9094

KAFKA_ADVERTISED_LISTENERS: >-
  INTERNAL://kafka:29092,
  LOCALHOST://localhost:9092,
  DOCKER_HOST://172.17.0.1:9093,
  EXTERNAL://<WSL2_IP>:9094

KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: >-
  INTERNAL:PLAINTEXT,
  LOCALHOST:PLAINTEXT,
  DOCKER_HOST:PLAINTEXT,
  EXTERNAL:PLAINTEXT
```

Add port mapping:
```yaml
ports:
  - "9092:9092"
  - "9093:9093"
  - "9094:9094"
```

Restart the stack, then on the remote machine:

```bash
python3 kafka_consumer.py --broker <WSL2_IP>:9094 --topics ds.alerts
```

---

## Part 7 — Multiple GPU Instances

Each GPU container uses the same `172.17.0.1:9093` broker address. The `server_id` and `gpu_id` in each message envelope distinguish which GPU sent what.

```bash
# GPU 0
docker run -d --gpus '"device=0"' \
  -e KAFKA_BROKER=172.17.0.1:9093 \
  -e SERVER_ID=server_1 -e GPU_ID=0 \
  -v $(pwd):/app -w /app \
  ds-pipeline:latest python3 main.py --streams 10

# GPU 1
docker run -d --gpus '"device=1"' \
  -e KAFKA_BROKER=172.17.0.1:9093 \
  -e SERVER_ID=server_1 -e GPU_ID=1 \
  -v $(pwd):/app -w /app \
  ds-pipeline:latest python3 main.py --streams 10
```

One consumer on the WSL host receives events from all GPUs:

```bash
python3 kafka_consumer.py \
  --broker localhost:9092 \
  --topics ds.alerts ds.heartbeat ds.benchmark \
  --group-id platform-server
```

---

## Environment Variables Reference

| Variable | Used by | Value |
|----------|---------|-------|
| `KAFKA_BROKER` | DeepStream container | `172.17.0.1:9093` |
| `KAFKA_BROKER` | WSL host consumer/simulator | `localhost:9092` |
| `SERVER_ID` | Pipeline container | `server_1` |
| `GPU_ID` | Pipeline container | `0`, `1`, `2`, `3` |
| `KAFKA_SASL_USERNAME` | Both | SASL username if auth enabled |
| `KAFKA_SASL_PASSWORD` | Both | SASL password if auth enabled |
| `KAFKA_SSL_CA` | Both | CA cert path if TLS enabled |
