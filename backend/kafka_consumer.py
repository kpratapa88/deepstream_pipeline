"""
Background Kafka consumer for the FastAPI backend.

Subscribes to ds.alerts, ds.heartbeat, ds.benchmark, ds.clips at the
latest offset and appends parsed messages to the shared AppState.

Runs in a daemon thread started by the FastAPI lifespan handler.
If the broker is unreachable, it logs the error and retries every 10 s.
"""
import json
import logging
import threading
import time
import uuid
from typing import Optional

from backend.config import settings
from backend.state import AppState, EventEntry, app_state

logger = logging.getLogger(__name__)

TOPICS = ["ds.alerts", "ds.heartbeat", "ds.benchmark", "ds.clips"]

# Retry interval when the broker is unreachable
_RETRY_INTERVAL_S = 10


# ---------------------------------------------------------------------------
# Message → EventEntry conversion
# ---------------------------------------------------------------------------

def _parse_alert(msg: dict) -> EventEntry:
    p = msg.get("payload", {})
    return EventEntry(
        message_id=msg.get("message_id", str(uuid.uuid4())),
        event_type="alert",
        alert_type=p.get("alert_type", ""),
        cam_id=p.get("cam_id", ""),
        location=p.get("location", ""),
        confidence=float(p.get("confidence", 0.0)),
        details=p.get("details", {}),
        clip_url="",
        timestamp_utc=msg.get("timestamp_utc", ""),
        timestamp_ms=int(msg.get("timestamp_ms", 0)),
    )


def _parse_heartbeat(msg: dict) -> EventEntry:
    p = msg.get("payload", {})
    return EventEntry(
        message_id=msg.get("message_id", str(uuid.uuid4())),
        event_type="heartbeat",
        alert_type="",
        cam_id=msg.get("server_id", ""),
        location="",
        confidence=0.0,
        details=p,
        clip_url="",
        timestamp_utc=msg.get("timestamp_utc", ""),
        timestamp_ms=int(msg.get("timestamp_ms", 0)),
    )


def _parse_benchmark(msg: dict) -> EventEntry:
    p = msg.get("payload", {})
    return EventEntry(
        message_id=msg.get("message_id", str(uuid.uuid4())),
        event_type="benchmark",
        alert_type="",
        cam_id=msg.get("server_id", ""),
        location="",
        confidence=0.0,
        details=p,
        clip_url="",
        timestamp_utc=msg.get("timestamp_utc", ""),
        timestamp_ms=int(msg.get("timestamp_ms", 0)),
    )


def _parse_clip(msg: dict) -> EventEntry:
    p = msg.get("payload", {})
    return EventEntry(
        message_id=msg.get("message_id", str(uuid.uuid4())),
        event_type="clip",
        alert_type=p.get("alert_type", ""),
        cam_id=p.get("cam_id", ""),
        location=p.get("location", ""),
        confidence=0.0,
        details=p,
        clip_url=p.get("clip_url", ""),
        timestamp_utc=msg.get("timestamp_utc", ""),
        timestamp_ms=int(msg.get("timestamp_ms", 0)),
    )


_PARSERS = {
    "ds.alerts":    _parse_alert,
    "ds.heartbeat": _parse_heartbeat,
    "ds.benchmark": _parse_benchmark,
    "ds.clips":     _parse_clip,
}


# ---------------------------------------------------------------------------
# Consumer thread
# ---------------------------------------------------------------------------

class BackendKafkaConsumer:
    """
    Wraps a confluent_kafka Consumer in a daemon thread.
    Appends parsed messages to the provided AppState instance.
    """

    def __init__(self, state: AppState = app_state):
        self._state = state
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the background consumer thread (idempotent)."""
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="kafka-consumer",
            daemon=True,
        )
        self._thread.start()
        logger.info("Kafka consumer thread started (broker=%s)", settings.KAFKA_BROKER)

    def stop(self) -> None:
        """Signal the consumer thread to stop and wait for it."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _run(self) -> None:
        """Main loop: connect → consume → retry on failure."""
        while not self._stop_event.is_set():
            consumer = self._create_consumer()
            if consumer is None:
                # Broker unreachable — wait and retry
                self._stop_event.wait(timeout=_RETRY_INTERVAL_S)
                continue
            try:
                consumer.subscribe(TOPICS)
                logger.info("Subscribed to topics: %s", TOPICS)
                self._consume_loop(consumer)
            except Exception as exc:
                logger.error("Kafka consumer error: %s — retrying in %ds", exc, _RETRY_INTERVAL_S)
            finally:
                try:
                    consumer.close()
                except Exception:
                    pass
            if not self._stop_event.is_set():
                self._stop_event.wait(timeout=_RETRY_INTERVAL_S)

    def _create_consumer(self):
        """Attempt to create a confluent_kafka Consumer; return None on failure."""
        try:
            from confluent_kafka import Consumer  # lazy import — optional dependency
        except ImportError:
            logger.warning("confluent_kafka not installed; Kafka consumer disabled")
            self._stop_event.set()
            return None

        conf = {
            "bootstrap.servers": settings.KAFKA_BROKER,
            "group.id": "backend-dashboard",
            "auto.offset.reset": "latest",   # only new messages (Req 1.3)
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 1000,
            "session.timeout.ms": 30000,
            "heartbeat.interval.ms": 10000,
        }
        try:
            consumer = Consumer(conf)
            # Quick connectivity check
            consumer.list_topics(timeout=5)
            return consumer
        except Exception as exc:
            logger.error(
                "Kafka broker unreachable (%s): %s — retrying in %ds",
                settings.KAFKA_BROKER, exc, _RETRY_INTERVAL_S,
            )
            return None

    def _consume_loop(self, consumer) -> None:
        """Poll loop — runs until stop_event is set or an exception is raised."""
        from confluent_kafka import KafkaError  # noqa: F401

        while not self._stop_event.is_set():
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                err = msg.error()
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                # Any other error — propagate to trigger reconnect
                raise Exception(str(err))

            topic = msg.topic()
            try:
                data = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                logger.warning("Bad message on %s: %s", topic, exc)
                continue

            self._dispatch(topic, data)

    def _dispatch(self, topic: str, data: dict) -> None:
        """Route a parsed Kafka message to the correct AppState field."""
        parser = _PARSERS.get(topic)
        if parser is None:
            return

        entry = parser(data)
        self._state.event_log.append(entry)  # deque auto-evicts oldest (Req 1.2)

        if topic == "ds.heartbeat":
            self._state.latest_heartbeat = data  # Req 1.5

        if topic == "ds.benchmark":
            self._state.latest_benchmark = data  # Req 1.5

        if topic == "ds.alerts":
            # Track alert highlight for tile border coloring
            stream_id = data.get("payload", {}).get("stream_id", 0)
            alert_type = data.get("payload", {}).get("alert_type", "")
            ts_ms = int(data.get("timestamp_ms", 0))
            self._state.alert_highlights[stream_id] = (alert_type, ts_ms)


# Module-level singleton used by the FastAPI lifespan
consumer = BackendKafkaConsumer()
