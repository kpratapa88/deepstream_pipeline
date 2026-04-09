"""
DeepStream Multi-Stream AI Pipeline — Entry Point

Usage:
    # Basic 4-stream run
    python3 main.py --streams 4

    # Use a custom rules file
    python3 main.py --streams 4 --config configs/rules_night.json

    # Override stream 2 inline (no file edit needed)
    python3 main.py --streams 4 --stream-config '2:{"models":[{"name":"fire_smoke","detect":["fire","smoke"]}]}'

    # Override multiple streams
    python3 main.py --streams 4 \\
        --stream-config '0:{"models":[{"name":"coco","detect":["person"],"features":["fall_detection"]}]}' \\
        --stream-config '2:{"models":[{"name":"fire_smoke","detect":["fire"]},{"name":"coco","detect":["person"]}]}'

    # Save output to file
    python3 main.py --streams 4 --output output/demo.mp4

    # Reduce GPU load (infer every 5th frame)
    python3 main.py --streams 8 --interval 4
"""
import os
import json
import argparse
from pipeline.gst_pipeline import DeepStreamPipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="NVIDIA DeepStream multi-stream AI pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--streams", type=int, default=4,
        help="Number of video streams to process (no hard limit — GPU memory is the constraint)",
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
        help=(
            "Override config for stream N with inline JSON (repeatable). "
            "Format: 'N:{...}' where N is 0-based stream index. "
            "Example: --stream-config '2:{\"models\":[{\"name\":\"fire_smoke\",\"detect\":[\"fire\"]}]}'"
        ),
    )
    return parser.parse_args()


def parse_stream_overrides(overrides: list) -> dict:
    """
    Parse --stream-config arguments into {stream_id: config_dict}.
    Format: 'N:{json}' where N is the 0-based stream index.
    """
    result = {}
    for override in overrides:
        try:
            idx, _, json_str = override.partition(":")
            stream_id = int(idx.strip())
            cfg       = json.loads(json_str.strip())
            result[stream_id] = cfg
            print(f"  [override] stream{stream_id + 1}: {json.dumps(cfg)}")
        except (ValueError, json.JSONDecodeError) as e:
            print(f"  [WARNING] Invalid --stream-config '{override}': {e}")
    return result


def resolve_sources(num_streams: int) -> list:
    """Build source URI list from STREAM_URI_N env vars or default file paths."""
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


def main() -> None:
    args = parse_args()
    n    = max(1, args.streams)  # no upper cap — GPU is the limit

    # Parse inline stream overrides
    overrides = {}
    if args.stream_overrides:
        print("Stream config overrides:")
        overrides = parse_stream_overrides(args.stream_overrides)
        print()

    sources = resolve_sources(n)

    DeepStreamPipeline(
        sources          = sources,
        output_file      = args.output,
        infer_interval   = args.interval,
        rules_file       = args.config,
        stream_overrides = overrides,
    ).run()


if __name__ == "__main__":
    main()
