"""
Kafka Test Event Simulator
Sends randomised detection and alert events from all 4 streams
without needing DeepStream or a GPU.

Usage:
    # Against local Kafka
    python3 kafka_test_producer.py

    # Against remote broker
    python3 kafka_test_producer.py --broker 192.168.1.50:9092

    # With SASL/SSL (enterprise)
    python3 kafka_test_producer.py \
        --broker broker:9093 \
        --security-protocol SASL_SSL \
        --sasl-mechanism SCRAM-SHA-512 \
        --sasl-username myuser \
        --sasl-password mypassword

    # Send 200 events then stop
    python3 kafka_test_producer.py --count 200

    # Fast burst (no delay)
    python3 kafka_test_producer.py --interval 0
"""
import argparse
import os
import random
import signal
import sys
import time

from pipeline.kafka_producer import (
    KafkaEventProducer, ensure_topics,
    TOPIC_DETECTIONS, TOPIC_ALERTS, TOPIC_HEARTBEAT, TOPIC_BENCHMARK,
)

# ── Simulated stream metadata (mirrors rules.json) ────────────────────────────
STREAMS = [
    {"stream_id": 0, "cam_id": "CAM_01", "location": "Main_Entrance"},
    {"stream_id": 1, "cam_id": "CAM_02", "location": "Barn_West"},
    {"stream_id": 2, "cam_id": "CAM_03", "location": "Factory_Floor"},
    {"stream_id": 3, "cam_id": "CAM_04", "location": "Main_Hall"},
]

# ── Event pools per stream ────────────────────────────────────────────────────
STREAM_EVENTS = {
    0: [  # Main_Entrance — person + fall
        ("detection", "coco",       "person", 0),
        ("alert",     "fall",       "person", 0),
    ],
    1: [  # Barn_West — animals
        ("detection", "coco", "cow",   19),
        ("detection", "coco", "horse", 17),
        ("detection", "coco", "sheep", 18),
        ("detection", "coco", "dog",   16),
    ],
    2: [  # Factory_Floor — fire/smoke
        ("detection", "fire_smoke", "fire",  0),
        ("detection", "fire_smoke", "smoke", 1),
        ("alert",     "fire",       "fire",  0),
        ("alert",     "smoke",      "smoke", 1),
    ],
    3: [  # Main_Hall — crowd
        ("detection", "coco",  "person", 0),
        ("alert",     "crowd", "person", 0),
    ],
}


def _random_bbox(mux_w: int = 1280, mux_h: int = 720) -> dict:
    x1 = random.uniform(0, mux_w - 100)
    y1 = random.uniform(0, mux_h - 100)
    w  = random.uniform(40, 200)
    h  = random.uniform(60, 300)
    return {
        "x1": round(x1, 2), "y1": round(y1, 2),
        "x2": round(x1 + w, 2), "y2": round(y1 + h, 2),
        "w":  round(w, 2), "h": round(h, 2),
    }


def _random_crowd_details(stream_id: int) -> dict:
    count = random.randint(1, 40)
    zones = ["CLEAR", "LOW", "MEDIUM", "HIGH", "CRITICAL"]
    zone  = zones[min(count // 8, 4)]
    return {"person_count": count, "zone": zone, "avg": round(count * random.uniform(0.8, 1.2), 1)}


def send_event(producer: KafkaEventProducer, stream: dict) -> str:
    sid    = stream["stream_id"]
    cam_id = stream["cam_id"]
    loc    = stream["location"]

    event_pool = STREAM_EVENTS[sid]
    kind, model, class_name, class_id = random.choice(event_pool)
    conf = round(random.uniform(0.45, 0.98), 3)

    if kind == "detection":
        producer.publish_detection(
            stream_id  = sid,
            cam_id     = cam_id,
            location   = loc,
            model      = model,
            class_name = class_name,
            confidence = conf,
            bbox       = _random_bbox(),
        )
        return f"[{cam_id}] detection  model={model} class={class_name} conf={conf:.3f}"

    elif kind == "alert":
        if model == "crowd":
            details = _random_crowd_details(sid)
        elif model in ("fall",):
            details = {"bbox": _random_bbox()}
        else:
            details = {"class_name": class_name}
        producer.publish_alert(
            stream_id  = sid,
            cam_id     = cam_id,
            location   = loc,
            alert_type = model,
            confidence = conf,
            details    = details,
        )
        return f"[{cam_id}] ALERT      type={model} conf={conf:.3f}"

    return ""


def send_heartbeat(producer: KafkaEventProducer, n_streams: int) -> None:
    producer.publish_heartbeat(
        streams_active = n_streams,
        fps_total      = round(random.uniform(80, 120), 1),
        gpu_util_pct   = round(random.uniform(55, 85), 1),
        vram_used_mb   = round(random.uniform(3000, 6000), 1),
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Kafka test event simulator — no GPU required",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--broker",            default=os.getenv("KAFKA_BROKER", "localhost:9092"))
    p.add_argument("--server-id",         default=os.getenv("SERVER_ID", "server_1"))
    p.add_argument("--gpu-id",            type=int, default=int(os.getenv("GPU_ID", "0")))
    p.add_argument("--interval",          type=float, default=0.5,
                   help="Seconds between events (0 = as fast as possible)")
    p.add_argument("--count",             type=int, default=0,
                   help="Total events to send then exit (0 = run forever)")
    p.add_argument("--heartbeat-every",   type=int, default=10,
                   help="Send a heartbeat every N events")
    p.add_argument("--security-protocol", default="PLAINTEXT",
                   choices=["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"])
    p.add_argument("--sasl-mechanism",    default=None,
                   choices=["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"])
    p.add_argument("--sasl-username",     default=os.getenv("KAFKA_SASL_USERNAME", ""))
    p.add_argument("--sasl-password",     default=os.getenv("KAFKA_SASL_PASSWORD", ""))
    p.add_argument("--ssl-ca",            default=os.getenv("KAFKA_SSL_CA", ""))
    return p.parse_args()


def main() -> None:
    args = parse_args()

    print(f"Kafka Test Producer")
    print(f"  broker    : {args.broker}")
    print(f"  server_id : {args.server_id}  gpu_id: {args.gpu_id}")
    print(f"  streams   : {len(STREAMS)}")
    print(f"  interval  : {args.interval}s")
    print(f"  count     : {'∞' if args.count == 0 else args.count}")
    print()

    ensure_topics(args.broker)

    producer = KafkaEventProducer(
        bootstrap_servers = args.broker,
        server_id         = args.server_id,
        gpu_id            = args.gpu_id,
        security_protocol = args.security_protocol,
        sasl_mechanism    = args.sasl_mechanism or None,
        sasl_username     = args.sasl_username or None,
        sasl_password     = args.sasl_password or None,
        ssl_ca_location   = args.ssl_ca or None,
        enabled           = True,
    )

    # Graceful Ctrl+C
    stop = [False]
    def _sig(s, f): stop[0] = True
    signal.signal(signal.SIGINT,  _sig)
    signal.signal(signal.SIGTERM, _sig)

    sent = 0
    try:
        while not stop[0]:
            stream = random.choice(STREAMS)
            desc   = send_event(producer, stream)
            sent  += 1
            print(f"  [{sent:>6}] {desc}")

            if sent % args.heartbeat_every == 0:
                send_heartbeat(producer, len(STREAMS))
                print(f"  [{sent:>6}] heartbeat sent")

            if args.count > 0 and sent >= args.count:
                break

            if args.interval > 0:
                time.sleep(args.interval)
    finally:
        print(f"\nSent {sent} events. Flushing producer...")
        producer.shutdown()
        print("Done.")


if __name__ == "__main__":
    main()
