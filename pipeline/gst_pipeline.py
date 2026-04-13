"""
GStreamer pipeline orchestration.
Builds, configures, and runs the multi-stream DeepStream pipeline.
"""
import os
import sys
import re
import math
import tempfile
import time
from collections import defaultdict
from typing import List

import gi
gi.require_version("Gst", "1.0")
from gi.repository import Gst, GLib

try:
    import pyds
except ImportError:
    print("Error: pyds not found. Ensure DeepStream Python bindings are installed.")
    sys.exit(1)

from utils.rule_engine import RuleEngine
from .constants import (
    CONFIG_INFER_COCO, CONFIG_INFER_FIRE, CONFIG_INFER_POSE,
    RULES_FILE, BENCHMARK_INTERVAL_SEC, C, STREAM_COLORS,
)
from .display import ConsoleDashboard, print_benchmark
from .fall_detector import FallDetector
from .crowd_monitor import CrowdMonitor
from .event_emitter import EventEmitter
from .probe_handler import ProbeHandler
from .kafka_producer import KafkaEventProducer, ensure_topics


def _patch_config(base_path: str, engine_name: str, batch: int,
                  gie_uid: int, interval: int = 0) -> tempfile.NamedTemporaryFile:
    """Write a patched nvinfer config to a temp file."""
    with open(base_path) as f:
        cfg = f.read()

    replacements = {
        r"^#?\s*model-engine-file=.*": f"model-engine-file=../models/{engine_name}_b{batch}.engine",
        r"^#?\s*batch-size=.*":        f"batch-size={batch}",
        r"^interval=.*":               f"interval={interval}",
        r"^gie-unique-id=.*":          f"gie-unique-id={gie_uid}",
    }
    for pattern, replacement in replacements.items():
        if re.search(pattern, cfg, re.MULTILINE):
            cfg = re.sub(pattern, replacement, cfg, flags=re.MULTILINE)
        else:
            cfg = cfg.replace("[property]", f"[property]\n{replacement}")

    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".txt", dir="configs", delete=False)
    tmp.write(cfg)
    tmp.close()
    return tmp


def _create_source_bin(index: int, uri: str):
    """Create a GStreamer source bin for a single stream URI."""
    nbin       = Gst.Bin.new(f"source-bin-{index:02d}")
    decodebin  = Gst.ElementFactory.make("uridecodebin",   f"uri-decode-bin-{index}")
    nvvidconv  = Gst.ElementFactory.make("nvvideoconvert", f"nvvidconv-{index}")
    capsfilter = Gst.ElementFactory.make("capsfilter",     f"capsfilter-{index}")
    queue      = Gst.ElementFactory.make("queue",          f"queue-{index}")

    if not all([decodebin, nvvidconv, capsfilter, queue]):
        return None

    decodebin.set_property("uri", uri)
    capsfilter.set_property("caps", Gst.Caps.from_string("video/x-raw(memory:NVMM), format=NV12"))

    for el in [decodebin, nvvidconv, capsfilter, queue]:
        nbin.add(el)
    nvvidconv.link(capsfilter)
    capsfilter.link(queue)

    def _on_pad_added(db, pad, data):
        caps = pad.get_current_caps() or pad.query_caps(None)
        st   = caps.get_structure(0)
        if st and st.get_name().startswith("video"):
            sink = nvvidconv.get_static_pad("sink")
            if not sink.is_linked():
                pad.link(sink)

    decodebin.connect("pad-added", _on_pad_added, nbin)
    nbin.add_pad(Gst.GhostPad.new("src", queue.get_static_pad("src")))
    return nbin


class DeepStreamPipeline:
    """
    Orchestrates the full multi-stream DeepStream pipeline.
    Composes all sub-components and manages the GLib main loop.
    """

    MUX_W = 1280
    MUX_H = 720

    def __init__(self, sources: List[str],
                 output_file: str = None,
                 infer_interval: int = 2,
                 rules_file: str = None,
                 stream_overrides: dict = None,
                 kafka_producer: "KafkaEventProducer" = None):
        self.sources        = sources
        self.output_file    = output_file
        self.infer_interval = infer_interval
        self.pipeline       = None
        self.loop           = GLib.MainLoop()

        # Core components
        self.rule_engine = RuleEngine(
            rules_file or RULES_FILE,
            stream_overrides=stream_overrides or {}
        )
        self.labels      = self._load_labels("labels.txt")
        self.stream_model = {
            int(k): list(sc.active_models)
            for k, sc in self.rule_engine.stream_configs.items()
        }

        # Sub-components
        self.dashboard     = ConsoleDashboard(num_streams=len(sources))
        self.fall_detector = FallDetector()
        self.crowd_monitor = CrowdMonitor()
        self.kafka         = kafka_producer
        self.event_emitter = EventEmitter(
            self.dashboard, self.rule_engine,
            kafka_producer=kafka_producer,
        )

        # Benchmark state
        self._bm = defaultdict(lambda: {
            "frames": 0, "detections": 0,
            "infer_ms_total": 0.0, "infer_calls": 0
        })
        self._bm_start = time.time()

        # Probe handler (created after mux dims are known)
        self._probe_handler = None

    def _load_labels(self, path: str) -> list:
        if os.path.exists(path):
            with open(path) as f:
                return [l.strip() for l in f.readlines()]
        return []

    def label_name(self, cid: int) -> str:
        return self.labels[cid] if 0 <= cid < len(self.labels) else f"class_{cid}"

    def _bus_call(self, bus, message, loop):
        t = message.type
        if t == Gst.MessageType.EOS:
            print("End-of-stream", flush=True)
            loop.quit()
        elif t == Gst.MessageType.ERROR:
            err, debug = message.parse_error()
            print(f"Error: {err.message}: {debug}")
            loop.quit()
        return True

    def _benchmark_tick(self) -> bool:
        now     = time.time()
        elapsed = now - self._bm_start
        if elapsed >= BENCHMARK_INTERVAL_SEC:
            print_benchmark(dict(self._bm), elapsed)
            if self.kafka:
                self.kafka.publish_benchmark(dict(self._bm), elapsed)
            for s in self._bm.values():
                s["frames"] = s["detections"] = s["infer_calls"] = 0
                s["infer_ms_total"] = 0.0
            self._bm_start = now
        return True

    def run(self) -> None:
        n = len(self.sources)

        # Validate configs
        for cfg in [CONFIG_INFER_COCO, CONFIG_INFER_FIRE, CONFIG_INFER_POSE]:
            if not os.path.exists(cfg):
                print(f"Error: Config not found: {cfg}")
                return

        n_streams = len(self.sources)

        # ── Ensure all active streams are registered in rule_engine ──────────
        for sid in range(n_streams):
            self.rule_engine.get_stream(sid)

        # ── Determine active models (only for active streams) ─────────────────
        active_models = set()
        for sid in range(n_streams):
            sc = self.rule_engine.get_stream(sid)
            active_models |= sc.active_models
        if any(self.rule_engine.get_stream(sid).has_feature("fall_detection")
               for sid in range(n_streams)):
            active_models.add("pose")

        print(f"{C['dim']}[models] active: {sorted(active_models)}{C['reset']}", flush=True)

        # ── Patch configs with runtime values ─────────────────────────────────
        tmp_configs = {}
        for model_name in active_models:
            model_def = self.rule_engine.model_registry.get(model_name, {})
            cfg_path  = model_def.get("config")
            engine    = model_def.get("engine", model_name)
            gie_id    = model_def.get("gie_id", 1)
            if not cfg_path:
                print(f"[WARNING] No config path for model '{model_name}' — skipping")
                continue
            ivl = self.infer_interval if model_name != "fire_smoke" else self.infer_interval * 2
            tmp_configs[model_name] = _patch_config(cfg_path, engine, 1, gie_id, interval=ivl)

        print(f"{C['dim']}[config] interval={self.infer_interval}{C['reset']}", flush=True)

        Gst.init(None)
        self.pipeline = Gst.Pipeline()

        # ── Create fixed elements ─────────────────────────────────────────────
        streammux = Gst.ElementFactory.make("nvstreammux",        "streammux")
        tiler     = Gst.ElementFactory.make("nvmultistreamtiler", "tiler")
        nvvidconv = Gst.ElementFactory.make("nvvideoconvert",     "converter")
        nvosd     = Gst.ElementFactory.make("nvdsosd",            "onscreendisplay")
        if not all([streammux, tiler, nvvidconv, nvosd]):
            print("Error: failed to create core pipeline elements"); sys.exit(1)

        streammux.set_property("width",  self.MUX_W)
        streammux.set_property("height", self.MUX_H)
        streammux.set_property("batch-size", n)
        streammux.set_property("batched-push-timeout", 40000)
        if any(u.startswith("rtsp://") for u in self.sources):
            streammux.set_property("live-source", 1)

        tiler_cols = math.ceil(math.sqrt(n))
        tiler_rows = math.ceil(n / tiler_cols)
        tiler.set_property("rows",    tiler_rows)
        tiler.set_property("columns", tiler_cols)
        tiler.set_property("width",   self.MUX_W)
        tiler.set_property("height",  self.MUX_H)
        nvosd.set_property("display-text", 1)
        nvosd.set_property("display-bbox", 1)
        nvinfer_elements = {}   # {model_name: nvinfer_element}
        queue_elements   = {}   # {model_name: queue_element}
        for model_name, tmp in tmp_configs.items():
            pgie = Gst.ElementFactory.make("nvinfer", f"infer-{model_name}")
            q    = Gst.ElementFactory.make("queue",   f"queue-{model_name}")
            if not pgie or not q:
                print(f"Error: failed to create nvinfer for model '{model_name}'")
                sys.exit(1)
            pgie.set_property("config-file-path", tmp.name)
            pgie.set_property("batch-size", 1)
            nvinfer_elements[model_name] = pgie
            queue_elements[model_name]   = q

        # ── Sink ──────────────────────────────────────────────────────────────
        display = os.environ.get("DISPLAY", "")
        sink    = None
        for name in (["nveglglessink", "nv3dsink"] if display else ["nv3dsink"]):
            s = Gst.ElementFactory.make(name, "sink")
            if s:
                sink = s
                break
        if not sink:
            sink = Gst.ElementFactory.make("fakesink", "sink")

        sink_name = sink.get_factory().get_name()
        sink.set_property("sync", 0 if sink_name == "fakesink" else 1)
        print(f"{C['dim']}[sink] {sink_name} (DISPLAY={display or 'not set'}){C['reset']}", flush=True)

        # ── File output (optional) ────────────────────────────────────────────
        tee = queue_disp = None
        file_elements = []

        if self.output_file:
            tee        = Gst.ElementFactory.make("tee",   "tee")
            queue_disp = Gst.ElementFactory.make("queue", "queue-disp")
            os.makedirs(os.path.dirname(self.output_file) or "output", exist_ok=True)
            q_file  = Gst.ElementFactory.make("queue",          "queue-file")
            vconv   = Gst.ElementFactory.make("nvvideoconvert",  "vidconv-cpu")
            caps    = Gst.ElementFactory.make("capsfilter",      "caps-cpu")
            enc     = Gst.ElementFactory.make("x264enc",         "encoder") or \
                      Gst.ElementFactory.make("nvv4l2h264enc",   "encoder")
            parser  = Gst.ElementFactory.make("h264parse",       "h264parse")
            muxer   = Gst.ElementFactory.make("mp4mux",          "mp4mux") or \
                      Gst.ElementFactory.make("qtmux",            "qtmux")
            fsink   = Gst.ElementFactory.make("filesink",        "filesink")

            if enc and enc.get_factory().get_name() == "x264enc":
                caps.set_property("caps", Gst.Caps.from_string("video/x-raw, format=I420"))
                enc.set_property("bitrate", 2000)
                enc.set_property("speed-preset", "ultrafast")
                enc.set_property("tune", "zerolatency")
            else:
                enc.set_property("bitrate", 4000000)

            fsink.set_property("location", self.output_file)
            fsink.set_property("sync", 0)
            file_elements = [q_file, vconv, caps, enc, parser, muxer, fsink]
            print(f"{C['dim']}[output] {self.output_file} ({enc.get_factory().get_name()}){C['reset']}", flush=True)

        # ── Add elements ──────────────────────────────────────────────────────
        # Build ordered model chain: streammux → [model→queue]* → tiler → osd → sink
        model_order = list(tmp_configs.keys())  # preserves insertion order
        core = [streammux]
        for mn in model_order:
            core.append(nvinfer_elements[mn])
            core.append(queue_elements[mn])
        core += [tiler, nvvidconv, nvosd]
        if tee:
            core += [tee, queue_disp]
        core.append(sink)

        for el in core + file_elements:
            self.pipeline.add(el)

        # ── Add sources ───────────────────────────────────────────────────────
        for i, uri in enumerate(self.sources):
            src_bin = _create_source_bin(i, uri)
            if not src_bin:
                print(f"Error: source bin failed for stream {i}")
                sys.exit(1)
            self.pipeline.add(src_bin)
            sinkpad = streammux.request_pad(
                streammux.get_pad_template("sink_%u"), f"sink_{i}", None)
            src_bin.get_static_pad("src").link(sinkpad)

        # ── Link main chain dynamically ───────────────────────────────────────
        prev = streammux
        for mn in model_order:
            prev.link(nvinfer_elements[mn])
            nvinfer_elements[mn].link(queue_elements[mn])
            prev = queue_elements[mn]
        prev.link(tiler)
        tiler.link(nvvidconv)
        nvvidconv.link(nvosd)

        if tee:
            nvosd.link(tee)
            tee.get_request_pad("src_%u").link(queue_disp.get_static_pad("sink"))
            queue_disp.link(sink)
            tee_file = tee.get_request_pad("src_%u")
            tee_file.link(file_elements[0].get_static_pad("sink"))
            for i in range(len(file_elements) - 1):
                file_elements[i].link(file_elements[i + 1])
        else:
            nvosd.link(sink)

        # ── Attach probes — last model's src pad has all tensors ──────────────
        last_model = model_order[-1]
        last_queue = queue_elements[last_model]

        self._probe_handler = ProbeHandler(
            rule_engine   = self.rule_engine,
            label_fn      = self.label_name,
            fall_detector = self.fall_detector,
            crowd_monitor = self.crowd_monitor,
            event_emitter = self.event_emitter,
            dashboard     = self.dashboard,
            mux_w         = self.MUX_W,
            mux_h         = self.MUX_H,
            bm            = self._bm,
        )

        # Attach probes:
        # - fire_smoke probe on its own nvinfer src pad
        # - coco probe on the last queue src (after all models ran, all tensors available)
        for mn in model_order:
            if mn in ("coco", "pose"):
                continue  # coco handled below, pose tensor read inside coco probe
            nvinfer_elements[mn].get_static_pad("src").add_probe(
                Gst.PadProbeType.BUFFER, self._probe_handler.make_probe(mn), 0
            )
        if "coco" in nvinfer_elements:
            last_queue.get_static_pad("src").add_probe(
                Gst.PadProbeType.BUFFER, self._probe_handler.make_probe("coco"), 0
            )

        # ── Bus & timer ───────────────────────────────────────────────────────
        bus = self.pipeline.get_bus()
        bus.add_signal_watch()
        bus.connect("message", self._bus_call, self.loop)
        GLib.timeout_add(BENCHMARK_INTERVAL_SEC * 1000, self._benchmark_tick)

        # ── Start ─────────────────────────────────────────────────────────────
        self._print_startup(n)
        self.pipeline.set_state(Gst.State.PLAYING)

        try:
            self.loop.run()
        except KeyboardInterrupt:
            print("\nInterrupted", flush=True)
        finally:
            elapsed = time.time() - self._bm_start
            print_benchmark(dict(self._bm), elapsed)
            if self._probe_handler:
                self._probe_handler.shutdown()
            if self.kafka:
                self.kafka.shutdown()
            self.pipeline.set_state(Gst.State.NULL)
            for tmp in tmp_configs.values():
                try:
                    os.unlink(tmp.name)
                except Exception:
                    pass

    def _print_startup(self, n: int) -> None:
        print(f"\n{C['bold']}Starting pipeline — {n} streams{C['reset']}")
        for sid, sc in sorted(self.rule_engine.stream_configs.items()):
            color = STREAM_COLORS.get(sid, C["white"])
            models_desc = ", ".join(
                f"{s['model_name']}({','.join(str(c) for c in s['class_ids'])})"
                for s in sc.model_subscriptions
            )
            features = [f for s in sc.model_subscriptions for f in s["features"]]
            feat_str = f" [{', '.join(features)}]" if features else ""
            print(f"  {color}stream{sid + 1}{C['reset']} [{sc.id}] "
                  f"{sc.location} → {C['bold']}{models_desc}{C['reset']}{feat_str}")
        print(f"  benchmark every {BENCHMARK_INTERVAL_SEC}s\n")
