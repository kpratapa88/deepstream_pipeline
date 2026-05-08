# Kafka Integration — Step-by-Step Setup Guide

This guide covers everything from spinning up Kafka locally to running the
pipeline with live event publishing and consuming events from another machine.

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

Every message shares this outer structure:

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

### Alert payload (ds.alerts)

```json
{
  "stream_id":  0,
  "cam_id":     "CAM_01",
  "location":   "Main_Entrance",
  "alert_type": "fall",
  "confidence": 0.87,
  "details":    { "bbox": { "x1": 120, "y1": 80, "x2": 280, "y2": 420, "w": 160, "h": 340 } }
}
```

### Detection payload (ds.detections)

```json
{
  "stream_id":  2,
  "cam_id":     "CAM_03",
  "location":   "Factory_Floor",
  "model":      "fire_smoke",
  "class_name": "fire",
  "confidence": 0.91,
  "bbox":       { "x1": 400, "y1": 200, "x2": 560, "y2": 380, "w": 160, "h": 180 }
}
```

### Heartbeat payload (ds.heartbeat)

```json
{
  "streams_active": 4,
  "fps_total":      112.4,
  "gpu_util_pct":   79.1,
  "vram_used_mb":   5120.0
}
```

---

## Part 1 — Run Kafka Locally (Development / POC)

### Step 1 — Install Docker and Docker Compose

```bash
# Ubuntu / Debian
sudo apt-get install -y docker.io docker-compose-plugin
sudo usermod -aG docker $USER
newgrp docker
```

### Step 2 — Start Kafka

```bash
docker compose -f docker-compose.kafka.yml up -d
```

Wait ~15 seconds for Kafka to be healthy:

```bash
docker compose -f docker-compose.kafka.yml ps
# ds-kafka should show "healthy"
```

### Step 3 — Verify Kafka is reachable

```bash
docker exec ds-kafka kafka-broker-api-versions --bootstrap-server localhost:9092
```

You should see a list of API versions — Kafka is ready.

### Step 4 — Install Python client

```bash
pip install confluent-kafka
```

### Step 5 — Create topics (auto-created on first use, but explicit is better)

```bash
python3 -c "
from pipeline.kafka_producer import ensure_topics
ensure_topics('localhost:9092')
"
```

Expected output:
```
[Kafka] created topic: ds.detections
[Kafka] created topic: ds.alerts
[Kafka] created topic: ds.heartbeat
[Kafka] created topic: ds.benchmark
```

---

## Part 2 — Test Without DeepStream (Simulator)

Use this to verify Kafka is working before running the full GPU pipeline.

### Step 6 — Run the test event simulator

```bash
# Send random events from all 4 streams every 0.5s
python3 kafka_test_producer.py --broker localhost:9092

# Fast burst — 500 events then stop
python3 kafka_test_producer.py --broker localhost:9092 --count 500 --interval 0
```

Sample output:
```
Kafka Test Producer
  broker    : localhost:9092
  server_id : server_1  gpu_id: 0
  streams   : 4
  interval  : 0.5s
  count     : ∞

[Kafka] created topic: ds.detections  (already exists)
[Kafka] producer connected → localhost:9092 (server=server_1, gpu=0)
  [     1] [CAM_01] detection  model=coco       class=person  conf=0.823
  [     2] [CAM_03] ALERT      type=fire        conf=0.912
  [     3] [CAM_04] detection  model=coco       class=person  conf=0.741
  [     4] [CAM_02] detection  model=coco       class=cow     conf=0.654
  [    10] heartbeat sent
```

### Step 7 — Open Kafka UI (optional)

Open http://localhost:8080 in your browser.
Navigate to Topics → ds.alerts to see messages in real time.

---

## Part 3 — Consume Events (Same Machine)

### Step 8 — Run the consumer

Open a second terminal:

```bash
# Consume all topics (detections are counted but not printed — too noisy)
python3 kafka_consumer.py --broker localhost:9092

# Alerts only
python3 kafka_consumer.py --broker localhost:9092 --topics ds.alerts

# Alerts + heartbeat
python3 kafka_consumer.py --broker localhost:9092 --topics ds.alerts ds.heartbeat

# Print every detection too (verbose)
python3 kafka_consumer.py --broker localhost:9092 --verbose-detections

# Save everything to a JSONL file
python3 kafka_consumer.py --broker localhost:9092 --output-file events.jsonl
```

Sample consumer output:
```
Platform Consumer started
  topics : ds.detections, ds.alerts, ds.heartbeat, ds.benchmark
  Ctrl+C to stop

🚨  ALERT  [CAM_01] Main_Entrance     type=fall   conf=0.870  server=server_1 gpu=0 ts=2026-04-13T10:22:01Z
         details: {"bbox": {"x1": 120.0, "y1": 80.0, "x2": 280.0, "y2": 420.0}}
🔥  ALERT  [CAM_03] Factory_Floor     type=fire   conf=0.912  server=server_1 gpu=0 ts=2026-04-13T10:22:04Z
♥  heartbeat  server=server_1  gpu=0  streams=4  fps=112.4  gpu_util=79.1%  vram=5120.0 MB
```

---

## Part 4 — Run the Full Pipeline with Kafka

### Step 9 — Run the DeepStream pipeline with Kafka enabled

```bash
# Inside the DeepStream container
python3 main.py --streams 4 --kafka-broker localhost:9092

# With server/GPU identity (important for multi-server deployments)
python3 main.py --streams 4 \
  --kafka-broker localhost:9092 \
  --kafka-server-id server_1 \
  --kafka-gpu-id 0
```

You will see:
```
[Kafka] created topic: ds.detections  (already exists)
[Kafka] producer connected → localhost:9092 (server=server_1, gpu=0)
Starting pipeline — 4 streams
  stream1 [CAM_01] Main_Entrance → coco(0) [fall_detection]
  ...
```

Events now flow: DeepStream probe → EventEmitter → KafkaEventProducer → Kafka → Consumer.

---

## Part 5 — Consume from Another Server (Remote)

This is the production pattern: inference server publishes, platform server consumes.

### Step 10 — Expose Kafka to the network

By default `docker-compose.kafka.yml` binds to `localhost:9092`.
To allow remote connections, edit the compose file:

```yaml
# In docker-compose.kafka.yml, change KAFKA_ADVERTISED_LISTENERS:
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://<YOUR_SERVER_IP>:9092
```

Then restart:

```bash
docker compose -f docker-compose.kafka.yml down
docker compose -f docker-compose.kafka.yml up -d
```

Open port 9092 in your firewall:

```bash
# Ubuntu UFW
sudo ufw allow 9092/tcp

# GCP firewall rule
gcloud compute firewall-rules create kafka-ingress \
  --allow tcp:9092 \
  --source-ranges 0.0.0.0/0 \
  --description "Kafka broker"
```

### Step 11 — Run the consumer on the Platform Server

On the Platform Server (different machine):

```bash
# Install client
pip install confluent-kafka

# Consume alerts from the inference server
python3 kafka_consumer.py \
  --broker <INFERENCE_SERVER_IP>:9092 \
  --topics ds.alerts ds.heartbeat \
  --group-id platform-server

# Save all events to JSONL for downstream processing
python3 kafka_consumer.py \
  --broker <INFERENCE_SERVER_IP>:9092 \
  --output-file /var/log/ds-events/alerts.jsonl
```

### Step 12 — Run the simulator from the Platform Server (remote test)

```bash
python3 kafka_test_producer.py \
  --broker <INFERENCE_SERVER_IP>:9092 \
  --count 100
```

If you see events flowing, the network path is confirmed.

---

## Part 6 — Enterprise Setup (SASL/SSL Authentication)

For production deployments where Kafka requires authentication.

### Step 13 — Configure SASL_SSL on the broker

Add to your Kafka broker config (or managed cluster settings):

```properties
listeners=SASL_SSL://0.0.0.0:9093
advertised.listeners=SASL_SSL://<broker-host>:9093
security.inter.broker.protocol=SASL_SSL
sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512
sasl.enabled.mechanisms=SCRAM-SHA-512
ssl.keystore.location=/etc/kafka/ssl/kafka.keystore.jks
ssl.keystore.password=<keystore-password>
ssl.truststore.location=/etc/kafka/ssl/kafka.truststore.jks
ssl.truststore.password=<truststore-password>
```

### Step 14 — Run pipeline with SASL/SSL

```bash
python3 main.py --streams 4 \
  --kafka-broker broker.internal:9093 \
  --kafka-security-protocol SASL_SSL \
  --kafka-sasl-mechanism SCRAM-SHA-512 \
  --kafka-sasl-username ds-pipeline \
  --kafka-sasl-password <password> \
  --kafka-ssl-ca /etc/ssl/certs/ca-bundle.crt
```

Or via environment variables (recommended for containers):

```bash
export KAFKA_BROKER=broker.internal:9093
export KAFKA_SASL_USERNAME=ds-pipeline
export KAFKA_SASL_PASSWORD=<password>
export KAFKA_SSL_CA=/etc/ssl/certs/ca-bundle.crt

python3 main.py --streams 4 \
  --kafka-security-protocol SASL_SSL \
  --kafka-sasl-mechanism SCRAM-SHA-512
```

### Step 15 — Run consumer with SASL/SSL

```bash
python3 kafka_consumer.py \
  --broker broker.internal:9093 \
  --security-protocol SASL_SSL \
  --sasl-mechanism SCRAM-SHA-512 \
  --sasl-username platform-consumer \
  --sasl-password <password> \
  --ssl-ca /etc/ssl/certs/ca-bundle.crt \
  --topics ds.alerts ds.heartbeat
```

---

## Part 7 — Multiple GPU Instances (Production)

Each GPU runs its own pipeline container. All publish to the same Kafka cluster.
The consumer on the Platform Server receives events from all GPUs in one stream.

```bash
# GPU 0 — streams 1-10
docker run -d --gpus '"device=0"' \
  -e KAFKA_BROKER=<platform-ip>:9092 \
  -e SERVER_ID=server_1 -e GPU_ID=0 \
  -e STREAM_URI_1=rtsp://<nvr>/cam01 ... \
  ds-pipeline:latest python3 main.py --streams 10

# GPU 1 — streams 11-20
docker run -d --gpus '"device=1"' \
  -e KAFKA_BROKER=<platform-ip>:9092 \
  -e SERVER_ID=server_1 -e GPU_ID=1 \
  -e STREAM_URI_1=rtsp://<nvr>/cam11 ... \
  ds-pipeline:latest python3 main.py --streams 10

# GPU 2 — streams 21-30
docker run -d --gpus '"device=2"' \
  -e KAFKA_BROKER=<platform-ip>:9092 \
  -e SERVER_ID=server_1 -e GPU_ID=2 \
  -e STREAM_URI_1=rtsp://<nvr>/cam21 ... \
  ds-pipeline:latest python3 main.py --streams 10

# GPU 3 — streams 31-40
docker run -d --gpus '"device=3"' \
  -e KAFKA_BROKER=<platform-ip>:9092 \
  -e SERVER_ID=server_1 -e GPU_ID=3 \
  -e STREAM_URI_1=rtsp://<nvr>/cam31 ... \
  ds-pipeline:latest python3 main.py --streams 10
```

On the Platform Server, one consumer group handles all 40 streams:

```bash
python3 kafka_consumer.py \
  --broker localhost:9092 \
  --topics ds.alerts ds.heartbeat ds.benchmark \
  --group-id platform-server
```

---

## Troubleshooting

**`confluent_kafka` not found**
```bash
pip install confluent-kafka
```

**`[Kafka] producer disabled (confluent_kafka not installed)`**
The pipeline still runs — just without Kafka. Install the package and restart.

**Connection refused on port 9092**
```bash
# Check Kafka is running
docker compose -f docker-compose.kafka.yml ps

# Check port is open
nc -zv localhost 9092
```

**Remote connection refused**
- Ensure `KAFKA_ADVERTISED_LISTENERS` uses the server's actual IP, not `localhost`
- Check firewall allows port 9092 from the consumer's IP

**Messages not appearing in consumer**
```bash
# Check topic has messages
docker exec ds-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic ds.alerts \
  --from-beginning \
  --max-messages 5
```

**Consumer lag building up**
- Add more partitions: `num_partitions=8` in `ensure_topics()`
- Run multiple consumer instances with the same `--group-id`

**`[Kafka] queue full — dropped N messages`**
The background thread can't keep up. Options:
- Increase `linger.ms` and `batch.size` in `KafkaEventProducer`
- Reduce detection publish rate (only publish alerts, not all detections)
- Use a faster network path to the broker

---

## Environment Variables Reference

| Variable | Description | Default |
|----------|-------------|---------|
| `KAFKA_BROKER` | Broker address used by `--kafka-broker` default | `""` (disabled) |
| `SERVER_ID` | Server identifier in message envelope | `server_1` |
| `GPU_ID` | GPU index in message envelope | `0` |
| `KAFKA_SASL_USERNAME` | SASL username | `""` |
| `KAFKA_SASL_PASSWORD` | SASL password | `""` |
| `KAFKA_SSL_CA` | Path to CA certificate | `""` |
