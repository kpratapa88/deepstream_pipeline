"""
Enterprise Kafka Consumer — Platform Server side.

Subscribes to all ds.* topics and routes messages to handlers.
Run this on the Platform Server (or any machine with network access to Kafka).

Usage:
    # Consume from local broker, print all events
    python3 kafka_consumer.py

    # Consume from remote broker
    python3 kafka_consumer.py --broker 192.168.1.50:9092

    # Consume only alerts
    python3 kafka_consumer.py --topics ds.alerts

    # Multiple topics
    python3 kafka_consumer.py --topics ds.alerts ds.heartbeat

    # With SASL/SSL (enterprise)
    python3 kafka_consumer.py \
        --broker broker:9093 \
        --security-protocol SASL_SSL \
        --sasl-mechanism SCRAM-SHA-512 \
        --sasl-username myuser \
        --sasl-password mypassword

    # Save alerts to a JSONL file
    python3 kafka_consumer.py --topics ds.alerts --output-file alerts.jsonl

    # Consumer group (multiple consumers share the load)
    python3 kafka_consumer.py --group-id platform-server-1
"""
import argparse
import json
import os
import signal
import sys
import time
from datetime import datetime

try:
    from confluent_kafka import Consumer, KafkaError, KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic
    _KAFKA_AVAILABLE = True
except ImportError:
    print("ERROR: confluent_kafka not installed.")
    print("       pip install confluent-kafka")
    sys.exit(1)

from pipeline.kafka_producer import (
    TOPIC_DETECTIONS, TOPIC_ALERTS, TOPIC_HEARTBEAT, TOPIC_BENCHMARK,
    ALL_TOPICS,
)

# ── ANSI colors (standalone, no pipeline import needed) ───────────────────────
R  = "\033[91m"   # red
Y  = "\033[93m"   # yellow
G  = "\033[92m"   # green
C  = "\033[96m"   # cyan
M  = "\033[95m"   # magenta
B  = "\033[94m"   # blue
DIM = "\033[2m"
BOLD = "\033[1m"
RST = "\033[0m"

ALERT_COLORS = {
    "fall":  R,
    "fire":  R,
    "smoke": Y,
    "crowd": M,
}

TOPIC_COLORS = {
    TOPIC_ALERTS:     R,
    TOPIC_DETECTIONS: G,
    TOPIC_HEARTBEAT:  C,
    TOPIC_BENCHMARK:  B,
}


# ── Message handlers ──────────────────────────────────────────────────────────

def handle_alert(msg: dict, raw_topic: str) -> None:
    p    = msg.get("payload", {})
    ts   = msg.get("timestamp_utc", "")
    atype = p.get("alert_type", "unknown")
    color = ALERT_COLORS.get(atype, Y)
    icon  = {"fall": "🚨", "fire": "🔥", "smoke": "💨", "crowd": "👥"}.get(atype, "📌")
    print(
        f"{color}{BOLD}{icon}  ALERT{RST}  "
        f"{BOLD}[{p.get('cam_id','?')}]{RST} "
        f"{p.get('location','?'):<16} "
        f"type={color}{atype}{RST}  "
        f"conf={p.get('confidence', 0):.3f}  "
        f"{DIM}server={msg.get('server_id','?')} gpu={msg.get('gpu_id','?')} ts={ts}{RST}"
    )
    details = p.get("details", {})
    if details:
        print(f"         {DIM}details: {json.dumps(details)}{RST}")


def handle_detection(msg: dict, raw_topic: str) -> None:
    p = msg.get("payload", {})
    print(
        f"{G}  det{RST}  "
        f"[{p.get('cam_id','?')}] "
        f"{p.get('location','?'):<16} "
        f"model={p.get('model','?'):<10} "
        f"class={p.get('class_name','?'):<10} "
        f"conf={p.get('confidence', 0):.3f}  "
        f"{DIM}bbox={p.get('bbox',{})}{RST}"
    )


def handle_heartbeat(msg: dict, raw_topic: str) -> None:
    p = msg.get("payload", {})
    print(
        f"{C}  ♥  heartbeat{RST}  "
        f"server={msg.get('server_id','?')}  "
        f"gpu={msg.get('gpu_id','?')}  "
        f"streams={p.get('streams_active','?')}  "
        f"fps={p.get('fps_total','?')}  "
        f"gpu_util={p.get('gpu_util_pct','?')}%  "
        f"vram={p.get('vram_used_mb','?')} MB  "
        f"{DIM}{msg.get('timestamp_utc','')}{RST}"
    )


def handle_benchmark(msg: dict, raw_topic: str) -> None:
    p       = msg.get("payload", {})
    streams = p.get("streams", {})
    total_fps = sum(s.get("fps", 0) for s in streams.values())
    print(
        f"{B}  📊 benchmark{RST}  "
        f"server={msg.get('server_id','?')}  "
        f"window={p.get('window_sec','?')}s  "
        f"total_fps={total_fps:.1f}  "
        f"streams={len(streams)}"
    )
    for sid, s in sorted(streams.items()):
        print(
            f"     {DIM}stream{sid}: fps={s.get('fps','?')}  "
            f"infer_ms={s.get('infer_ms_avg','?')}  "
            f"dets={s.get('detections','?')}{RST}"
        )


HANDLERS = {
    TOPIC_ALERTS:     handle_alert,
    TOPIC_DETECTIONS: handle_detection,
    TOPIC_HEARTBEAT:  handle_heartbeat,
    TOPIC_BENCHMARK:  handle_benchmark,
}


# ── Consumer ──────────────────────────────────────────────────────────────────

class PlatformConsumer:
    """
    Kafka consumer for the Platform Server.
    Subscribes to one or more ds.* topics and dispatches to handlers.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topics: list,
        group_id: str = "platform-server",
        security_protocol: str = "PLAINTEXT",
        sasl_mechanism: str = None,
        sasl_username: str = None,
        sasl_password: str = None,
        ssl_ca_location: str = None,
        output_file: str = None,
        detections_verbose: bool = False,
    ):
        self.topics             = topics
        self.output_file        = output_file
        self.detections_verbose = detections_verbose
        self._running           = False
        self._stats             = {t: 0 for t in ALL_TOPICS}
        self._fh                = None

        conf = {
            "bootstrap.servers":        bootstrap_servers,
            "group.id":                 group_id,
            "auto.offset.reset":        "latest",   # only new messages
            "enable.auto.commit":       True,
            "auto.commit.interval.ms":  1000,
            "session.timeout.ms":       30000,
            "heartbeat.interval.ms":    10000,
            "max.poll.interval.ms":     300000,
            "fetch.min.bytes":          1,
            "fetch.wait.max.ms":        100,
            "client.id":                f"platform-consumer-{group_id}",
        }

        if security_protocol != "PLAINTEXT":
            conf["security.protocol"] = security_protocol
        if sasl_mechanism:
            conf["sasl.mechanism"]  = sasl_mechanism
            conf["sasl.username"]   = sasl_username or ""
            conf["sasl.password"]   = sasl_password or ""
        if ssl_ca_location:
            conf["ssl.ca.location"] = ssl_ca_location

        self._consumer = Consumer(conf)

        if output_file:
            self._fh = open(output_file, "a", buffering=1)
            print(f"{DIM}[consumer] appending to {output_file}{RST}")

        # Auto-create topics so consumer never crashes on missing topics
        self._ensure_topics(bootstrap_servers)

    def _ensure_topics(self, bootstrap_servers: str) -> None:
        """Create ds.* topics if they don't exist yet."""
        try:
            admin = AdminClient({"bootstrap.servers": bootstrap_servers})
            existing = set(admin.list_topics(timeout=5).topics.keys())
            to_create = [
                NewTopic(t, num_partitions=4, replication_factor=1)
                for t in ALL_TOPICS if t not in existing
            ]
            if not to_create:
                print(f"{DIM}[consumer] all topics already exist{RST}")
                return
            results = admin.create_topics(to_create)
            for topic, fut in results.items():
                try:
                    fut.result()
                    print(f"{G}[consumer] created topic: {topic}{RST}")
                except Exception as e:
                    print(f"{Y}[consumer] topic '{topic}': {e}{RST}")
        except Exception as e:
            print(f"{Y}[consumer] could not auto-create topics: {e}{RST}")

    def run(self) -> None:
        self._consumer.subscribe(self.topics)
        self._running = True

        print(f"\n{BOLD}Platform Consumer started{RST}")
        print(f"  topics : {', '.join(self.topics)}")
        print(f"  Ctrl+C to stop\n")

        try:
            while self._running:
                msg = self._consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    code = msg.error().code()
                    if code == KafkaError._PARTITION_EOF:
                        # Normal — reached end of partition, keep polling
                        continue
                    if code == KafkaError.UNKNOWN_TOPIC_OR_PART:
                        # Topic doesn't exist yet — wait for producer to create it
                        print(f"{Y}[consumer] topic not ready yet, retrying...{RST}")
                        time.sleep(2)
                        continue
                    if code in (KafkaError._TRANSPORT, KafkaError._ALL_BROKERS_DOWN):
                        # Transient network issue — log and retry
                        print(f"{Y}[consumer] broker unreachable: {msg.error()} — retrying in 3s{RST}")
                        time.sleep(3)
                        continue
                    # Any other error — log it but keep running
                    print(f"{R}[consumer] kafka error: {msg.error()}{RST}")
                    continue

                topic = msg.topic()
                try:
                    data = json.loads(msg.value().decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    print(f"{R}[consumer] bad message on {topic}: {e}{RST}")
                    continue

                self._stats[topic] = self._stats.get(topic, 0) + 1

                # Skip verbose detection logging unless requested
                if topic == TOPIC_DETECTIONS and not self.detections_verbose:
                    self._stats[topic]  # still count
                    if self._fh:
                        self._fh.write(json.dumps(data) + "\n")
                    continue

                handler = HANDLERS.get(topic)
                if handler:
                    handler(data, topic)

                if self._fh:
                    self._fh.write(json.dumps(data) + "\n")

        except KeyboardInterrupt:
            pass
        finally:
            self._shutdown()

    def _shutdown(self) -> None:
        self._running = False
        self._consumer.close()
        if self._fh:
            self._fh.close()
        print(f"\n{BOLD}Consumer stopped.{RST}  Message counts:")
        for topic, count in self._stats.items():
            if count > 0:
                color = TOPIC_COLORS.get(topic, RST)
                print(f"  {color}{topic:<20}{RST}  {count}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Platform Server Kafka consumer for ds.* topics",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--broker",            default=os.getenv("KAFKA_BROKER", "localhost:9092"))
    p.add_argument("--topics",            nargs="+", default=ALL_TOPICS,
                   help="Topics to subscribe to")
    p.add_argument("--group-id",          default="platform-server",
                   help="Kafka consumer group ID")
    p.add_argument("--security-protocol", default="PLAINTEXT",
                   choices=["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"])
    p.add_argument("--sasl-mechanism",    default=None,
                   choices=["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"])
    p.add_argument("--sasl-username",     default=os.getenv("KAFKA_SASL_USERNAME", ""))
    p.add_argument("--sasl-password",     default=os.getenv("KAFKA_SASL_PASSWORD", ""))
    p.add_argument("--ssl-ca",            default=os.getenv("KAFKA_SSL_CA", ""))
    p.add_argument("--output-file",       default=None, metavar="PATH",
                   help="Append all consumed messages as JSONL to this file")
    p.add_argument("--verbose-detections", action="store_true",
                   help="Print every detection (can be very noisy)")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    PlatformConsumer(
        bootstrap_servers   = args.broker,
        topics              = args.topics,
        group_id            = args.group_id,
        security_protocol   = args.security_protocol,
        sasl_mechanism      = args.sasl_mechanism or None,
        sasl_username       = args.sasl_username or None,
        sasl_password       = args.sasl_password or None,
        ssl_ca_location     = args.ssl_ca or None,
        output_file         = args.output_file,
        detections_verbose  = args.verbose_detections,
    ).run()


if __name__ == "__main__":
    main()
