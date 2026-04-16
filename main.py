"""
DeepStream Multi-Stream AI Pipeline — Entry Point

Usage:
    # Basic 4-stream run
    python3 main.py --streams 4

    # With Kafka
    python3 main.py --streams 4 --kafka-broker localhost:9092

    # With Kafka + TLS/SASL (enterprise)
    python3 main.py --streams 4 \
        --kafka-broker broker:9093 \
        --kafka-security-protocol SASL_SSL \
        --kafka-sasl-mechanism SCRAM-SHA-512 \
        --kafka-sasl-username myuser \
        --kafka-sasl-password mypassword \
        --kafka-ssl-ca /etc/ssl/certs/ca.pem

    # Use a custom rules file
    python3 main.py --streams 4 --config configs/rules_night.json

    # Override stream 2 inline (no file edit needed)
    python3 main.py --streams 4 --stream-config '2:{"models":[{"name":"fire_smoke","detect":["fire","smoke"]}]}'

    # Save output to file
    python3 main.py --streams 4 --output output/demo.mp4

    # Reduce GPU load (infer every 5th frame)
    python3 main.py --streams 8 --interval 4
"""
import os
import json
import argparse
from pipeline.gst_pipeline import DeepStreamPipeline
from pipeline.kafka_producer import KafkaEventProducer, ensure_topics


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="NVIDIA DeepStream multi-stream AI pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # ── Pipeline ──────────────────────────────────────────────────────────────
    parser.add_argument(
        "--streams", type=int, default=4,
        help="Number of video streams to process",
    )
    parser.add_argument(
        "--output", type=str, default=None, metavar="PATH",
        help="Save tiled output to MP4 (e.g. output/demo.mp4)",
    )
    parser.add_argument(
        "--interval", type=int, default=2,
        help="nvinfer frame skip (0=every frame, 2=every 3rd, 4=every 5th)",
    )
    parser.add_argument(
        "--config", type=str, default="configs/rules.json", metavar="PATH",
        help="Path to rules.json stream configuration file",
    )
    parser.add_argument(
        "--stream-config", type=str, action="append", default=[], metavar="N:JSON",
        dest="stream_overrides",
        help="Override config for stream N with inline JSON (repeatable)",
    )

    # ── Kafka ─────────────────────────────────────────────────────────────────
    parser.add_argument(
        "--kafka-broker", type=str,
        default=os.getenv("KAFKA_BROKER", ""),
        metavar="HOST:PORT",
        help="Kafka bootstrap server (e.g. localhost:9092). "
             "Omit or leave empty to disable Kafka.",
    )
    parser.add_argument(
        "--kafka-server-id", type=str,
        default=os.getenv("SERVER_ID", "server_1"),
        help="Logical server identifier included in every Kafka message",
    )
    parser.add_argument(
        "--kafka-gpu-id", type=int,
        default=int(os.getenv("GPU_ID", "0")),
        help="GPU index included in every Kafka message",
    )
    parser.add_argument(
        "--kafka-security-protocol", type=str, default="PLAINTEXT",
        choices=["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"],
        help="Kafka security protocol",
    )
    parser.add_argument(
        "--kafka-sasl-mechanism", type=str, default=None,
        choices=["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"],
        help="SASL mechanism (required when protocol is SASL_*)",
    )
    parser.add_argument(
        "--kafka-sasl-username", type=str,
        default=os.getenv("KAFKA_SASL_USERNAME", ""),
        help="SASL username",
    )
    parser.add_argument(
        "--kafka-sasl-password", type=str,
        default=os.getenv("KAFKA_SASL_PASSWORD", ""),
        help="SASL password",
    )
    parser.add_argument(
        "--kafka-ssl-ca", type=str,
        default=os.getenv("KAFKA_SSL_CA", ""),
        metavar="PATH",
        help="Path to CA certificate file for TLS verification",
    )
    parser.add_argument(
        "--kafka-detections", action="store_true", default=False,
        help="Also publish every raw detection to ds.detections (high volume — off by default)",
    )

    return parser.parse_args()


def parse_stream_overrides(overrides: list) -> dict:
    result = {}
    for override in overrides:
        try:
            idx, _, json_str = override.partition(":")
            stream_id        = int(idx.strip())
            cfg              = json.loads(json_str.strip())
            result[stream_id] = cfg
            print(f"  [override] stream{stream_id + 1}: {json.dumps(cfg)}")
        except (ValueError, json.JSONDecodeError) as e:
            print(f"  [WARNING] Invalid --stream-config '{override}': {e}")
    return result


def resolve_sources(num_streams: int) -> list:
    sources = []
    print(f"Stream sources ({num_streams} streams):")
    for i in range(1, num_streams + 1):
        uri = os.getenv(f"STREAM_URI_{i}", f"videos/sample{i}.mp4")
        if not uri.startswith(("file://", "rtsp://")):
            abs_path = os.path.abspath(uri)
            status   = "OK" if os.path.exists(abs_path) else "NOT FOUND"
            print(f"  stream{i}: {abs_path} [{status}]")
            uri = f"file://{abs_path}"
        else:
            print(f"  stream{i}: {uri}")
        sources.append(uri)
    print()
    return sources


def build_kafka_producer(args) -> "KafkaEventProducer | None":
    """Construct KafkaEventProducer from CLI args, or return None if disabled."""
    if not args.kafka_broker:
        print(
            "\n[Kafka] WARNING: --kafka-broker not set — Kafka publishing DISABLED.\n"
            "        Events will only appear on console.\n"
            "        Inside Docker container use: --kafka-broker 172.17.0.1:9093\n"
            "        From WSL host directly use:  --kafka-broker localhost:9092\n"
        )
        return None

    print(f"[Kafka] broker={args.kafka_broker}  server_id={args.kafka_server_id}  gpu_id={args.kafka_gpu_id}")
    ensure_topics(args.kafka_broker)

    return KafkaEventProducer(
        bootstrap_servers   = args.kafka_broker,
        server_id           = args.kafka_server_id,
        gpu_id              = args.kafka_gpu_id,
        security_protocol   = args.kafka_security_protocol,
        sasl_mechanism      = args.kafka_sasl_mechanism or None,
        sasl_username       = args.kafka_sasl_username or None,
        sasl_password       = args.kafka_sasl_password or None,
        ssl_ca_location     = args.kafka_ssl_ca or None,
        enabled             = True,
        publish_detections  = args.kafka_detections,
    )


def main() -> None:
    args = parse_args()
    n    = max(1, args.streams)

    overrides = {}
    if args.stream_overrides:
        print("Stream config overrides:")
        overrides = parse_stream_overrides(args.stream_overrides)
        print()

    sources        = resolve_sources(n)
    kafka_producer = build_kafka_producer(args)

    DeepStreamPipeline(
        sources          = sources,
        output_file      = args.output,
        infer_interval   = args.interval,
        rules_file       = args.config,
        stream_overrides = overrides,
        kafka_producer   = kafka_producer,
    ).run()


if __name__ == "__main__":
    main()
