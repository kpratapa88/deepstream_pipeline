"""
Enterprise Kafka producer for the DeepStream pipeline.

Features:
- Async, non-blocking — uses a background thread queue so GStreamer probes
  are never stalled waiting for network I/O.
- Guaranteed delivery — producer is configured with acks=all and retries.
- Schema-versioned JSON messages with envelope metadata.
- Per-topic routing: detections → ds.detections, alerts → ds.alerts,
  heartbeat → ds.heartbeat, benchmark → ds.benchmark.
- Graceful shutdown with queue drain.
- Falls back to console-only mode when Kafka is unavailable (POC-safe).
"""

import json
import os
import queue
import socket
import threading
import time
import uuid
from typing import Optional

try:
    from confluent_kafka import Producer, KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic
    _KAFKA_AVAILABLE = True
except ImportError:
    _KAFKA_AVAILABLE = False

from .constants import C

# ── Topic names ───────────────────────────────────────────────────────────────
TOPIC_DETECTIONS = "ds.detections"
TOPIC_ALERTS     = "ds.alerts"
TOPIC_HEARTBEAT  = "ds.heartbeat"
TOPIC_BENCHMARK  = "ds.benchmark"

ALL_TOPICS = [TOPIC_DETECTIONS, TOPIC_ALERTS, TOPIC_HEARTBEAT, TOPIC_BENCHMARK]

# ── Schema version — bump when message structure changes ─────────────────────
SCHEMA_VERSION = "1.0"


def _build_envelope(event_type: str, payload: dict,
                    server_id: str, gpu_id: int) -> dict:
    """Wrap a payload in a standard envelope for all topics."""
    return {
        "schema_version": SCHEMA_VERSION,
        "message_id":     str(uuid.uuid4()),
        "event_type":     event_type,
        "server_id":      server_id,
        "gpu_id":         gpu_id,
        "hostname":       socket.gethostname(),
        "timestamp_utc":  time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "timestamp_ms":   int(time.time() * 1000),
        "payload":        payload,
    }


class KafkaEventProducer:
    """
    Thread-safe, non-blocking Kafka producer.

    All publish calls enqueue a message and return immediately.
    A single background thread drains the queue and calls confluent_kafka
    Producer.produce() + poll().

    If Kafka is not reachable or confluent_kafka is not installed,
    the producer silently no-ops so the pipeline continues running.
    """

    _QUEUE_MAX  = 10_000   # drop oldest if queue fills (back-pressure)
    _POLL_MS    = 50        # producer poll interval in background thread
    _DRAIN_WAIT = 5.0       # seconds to wait for queue drain on shutdown

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        server_id: str = "server_1",
        gpu_id: int = 0,
        security_protocol: str = "PLAINTEXT",
        sasl_mechanism: Optional[str] = None,
        sasl_username: Optional[str] = None,
        sasl_password: Optional[str] = None,
        ssl_ca_location: Optional[str] = None,
        enabled: bool = True,
    ):
        self.server_id = server_id
        self.gpu_id    = gpu_id
        self.enabled   = enabled and _KAFKA_AVAILABLE

        self._producer: Optional[object] = None
        self._queue    = queue.Queue(maxsize=self._QUEUE_MAX)
        self._thread   = None
        self._running  = False
        self._dropped  = 0

        if not self.enabled:
            reason = "confluent_kafka not installed" if not _KAFKA_AVAILABLE else "disabled by config"
            print(f"{C['yellow']}[Kafka] producer disabled ({reason}) — console-only mode{C['reset']}")
            return

        # ── Build confluent_kafka config ──────────────────────────────────────
        conf = {
            "bootstrap.servers":            bootstrap_servers,
            "acks":                         "all",           # strongest durability
            "retries":                      5,
            "retry.backoff.ms":             500,
            "linger.ms":                    10,              # micro-batching
            "batch.size":                   65536,
            "compression.type":             "lz4",
            "enable.idempotence":           True,            # exactly-once producer
            "max.in.flight.requests.per.connection": 5,
            "delivery.timeout.ms":          30000,
            "socket.keepalive.enable":      True,
            "client.id":                    f"ds-pipeline-{server_id}-gpu{gpu_id}",
        }

        # ── TLS / SASL (enterprise auth) ──────────────────────────────────────
        if security_protocol != "PLAINTEXT":
            conf["security.protocol"] = security_protocol
        if sasl_mechanism:
            conf["sasl.mechanism"]  = sasl_mechanism
            conf["sasl.username"]   = sasl_username or ""
            conf["sasl.password"]   = sasl_password or ""
        if ssl_ca_location:
            conf["ssl.ca.location"] = ssl_ca_location

        try:
            self._producer = Producer(conf)
            print(f"{C['green']}[Kafka] producer connected → {bootstrap_servers} "
                  f"(server={server_id}, gpu={gpu_id}){C['reset']}")
        except KafkaException as e:
            print(f"{C['red']}[Kafka] producer init failed: {e} — console-only mode{C['reset']}")
            self.enabled = False
            return

        # ── Start background delivery thread ──────────────────────────────────
        self._running = True
        self._thread  = threading.Thread(
            target=self._delivery_loop,
            name=f"kafka-producer-gpu{gpu_id}",
            daemon=True,
        )
        self._thread.start()

    # ── Public publish methods ────────────────────────────────────────────────

    def publish_detection(self, stream_id: int, cam_id: str, location: str,
                          model: str, class_name: str, confidence: float,
                          bbox: dict) -> None:
        """Publish a raw detection event to ds.detections."""
        payload = {
            "stream_id":  stream_id,
            "cam_id":     cam_id,
            "location":   location,
            "model":      model,
            "class_name": class_name,
            "confidence": round(confidence, 4),
            "bbox":       bbox,   # {x1, y1, x2, y2, w, h}
        }
        self._enqueue(TOPIC_DETECTIONS, "detection", payload,
                      key=f"{cam_id}:{stream_id}")

    def publish_alert(self, stream_id: int, cam_id: str, location: str,
                      alert_type: str, confidence: float, details: dict) -> None:
        """Publish a high-priority alert to ds.alerts."""
        payload = {
            "stream_id":  stream_id,
            "cam_id":     cam_id,
            "location":   location,
            "alert_type": alert_type,   # fall | fire | smoke | crowd
            "confidence": round(confidence, 4),
            "details":    details,
        }
        self._enqueue(TOPIC_ALERTS, alert_type, payload,
                      key=f"{cam_id}:{alert_type}")

    def publish_heartbeat(self, streams_active: int, fps_total: float,
                          gpu_util_pct: float, vram_used_mb: float) -> None:
        """Publish a periodic heartbeat to ds.heartbeat."""
        payload = {
            "streams_active": streams_active,
            "fps_total":      round(fps_total, 2),
            "gpu_util_pct":   round(gpu_util_pct, 1) if gpu_util_pct is not None else None,
            "vram_used_mb":   round(vram_used_mb, 1) if vram_used_mb is not None else None,
        }
        self._enqueue(TOPIC_HEARTBEAT, "heartbeat", payload,
                      key=self.server_id)

    def publish_benchmark(self, stats: dict, elapsed: float) -> None:
        """Publish benchmark stats to ds.benchmark."""
        per_stream = {}
        for sid, s in stats.items():
            fps     = s["frames"] / elapsed if elapsed > 0 else 0
            inf_avg = (s["infer_ms_total"] / s["infer_calls"]) if s["infer_calls"] > 0 else 0
            per_stream[str(sid)] = {
                "fps":          round(fps, 2),
                "infer_ms_avg": round(inf_avg, 2),
                "detections":   s["detections"],
                "frames":       s["frames"],
            }
        payload = {
            "window_sec": round(elapsed, 2),
            "streams":    per_stream,
        }
        self._enqueue(TOPIC_BENCHMARK, "benchmark", payload,
                      key=self.server_id)

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _enqueue(self, topic: str, event_type: str,
                 payload: dict, key: str = "") -> None:
        if not self.enabled or self._producer is None:
            return
        msg = _build_envelope(event_type, payload, self.server_id, self.gpu_id)
        try:
            self._queue.put_nowait((topic, key, msg))
        except queue.Full:
            self._dropped += 1
            if self._dropped % 100 == 1:
                print(f"{C['yellow']}[Kafka] queue full — dropped {self._dropped} messages{C['reset']}")

    def _delivery_loop(self) -> None:
        """Background thread: drain queue → produce → poll."""
        while self._running or not self._queue.empty():
            try:
                topic, key, msg = self._queue.get(timeout=0.1)
                value = json.dumps(msg, separators=(",", ":")).encode()
                self._producer.produce(
                    topic,
                    value=value,
                    key=key.encode() if key else None,
                    on_delivery=self._on_delivery,
                )
                self._producer.poll(0)   # non-blocking poll
            except queue.Empty:
                self._producer.poll(self._POLL_MS / 1000)
            except KafkaException as e:
                print(f"{C['red']}[Kafka] produce error: {e}{C['reset']}")
            except Exception as e:
                print(f"{C['red']}[Kafka] unexpected error in delivery loop: {e}{C['reset']}")

    @staticmethod
    def _on_delivery(err, msg) -> None:
        if err:
            print(f"{C['red']}[Kafka] delivery failed: {err}{C['reset']}")

    def flush(self, timeout: float = None) -> None:
        """Block until all queued messages are delivered."""
        if self._producer:
            self._producer.flush(timeout or self._DRAIN_WAIT)

    def shutdown(self) -> None:
        """Gracefully stop the background thread and flush remaining messages."""
        if not self.enabled:
            return
        print(f"{C['dim']}[Kafka] shutting down producer (draining queue)...{C['reset']}")
        self._running = False
        if self._thread:
            self._thread.join(timeout=self._DRAIN_WAIT)
        self.flush()
        print(f"{C['dim']}[Kafka] producer shut down. dropped={self._dropped}{C['reset']}")


def ensure_topics(bootstrap_servers: str,
                  num_partitions: int = 4,
                  replication_factor: int = 1) -> None:
    """
    Create Kafka topics if they don't already exist.
    Call once at pipeline startup (idempotent).
    """
    if not _KAFKA_AVAILABLE:
        return
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    existing = set(admin.list_topics(timeout=5).topics.keys())
    to_create = [
        NewTopic(t, num_partitions=num_partitions,
                 replication_factor=replication_factor)
        for t in ALL_TOPICS if t not in existing
    ]
    if not to_create:
        print(f"{C['dim']}[Kafka] all topics already exist{C['reset']}")
        return
    results = admin.create_topics(to_create)
    for topic, fut in results.items():
        try:
            fut.result()
            print(f"{C['green']}[Kafka] created topic: {topic}{C['reset']}")
        except Exception as e:
            print(f"{C['yellow']}[Kafka] topic '{topic}': {e}{C['reset']}")
