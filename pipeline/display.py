"""
Console display utilities: ANSI formatting, benchmark printing, event dashboard.
No GStreamer or pyds imports — pure Python console I/O.
"""
import sys
import time
from collections import defaultdict
from .constants import C, STREAM_COLORS, BENCHMARK_INTERVAL_SEC

try:
    import psutil
    _PSUTIL = True
except ImportError:
    _PSUTIL = False

try:
    import nvidia_ml_py as pynvml
    pynvml.nvmlInit()
    _NVML_HANDLE = pynvml.nvmlDeviceGetHandleByIndex(0)
    _PYNVML = True
except Exception:
    try:
        import pynvml
        pynvml.nvmlInit()
        _NVML_HANDLE = pynvml.nvmlDeviceGetHandleByIndex(0)
        _PYNVML = True
    except Exception:
        _PYNVML = False


def conf_bar(conf: float, width: int = 10) -> str:
    filled = int(conf * width)
    bar    = "█" * filled + "░" * (width - filled)
    color  = C["green"] if conf >= 0.7 else (C["yellow"] if conf >= 0.4 else C["red"])
    return f"{color}{bar}{C['reset']} {conf:.2f}"


def pct_bar(pct, width: int = 20) -> str:
    if pct is None:
        return f"{'N/A':>{width + 5}}"
    filled = int(pct / 100 * width)
    bar    = "█" * filled + "░" * (width - filled)
    color  = C["green"] if pct < 60 else (C["yellow"] if pct < 85 else C["red"])
    return f"{color}{bar}{C['reset']} {pct:5.1f}%"


def _read_cpu_pct() -> float:
    """Read CPU usage from /proc/stat — works in any Linux container."""
    try:
        def _read():
            with open("/proc/stat") as f:
                line = f.readline()
            vals = list(map(int, line.split()[1:]))
            idle = vals[3]
            total = sum(vals)
            return idle, total

        idle1, total1 = _read()
        time.sleep(0.3)
        idle2, total2 = _read()
        idle_delta  = idle2  - idle1
        total_delta = total2 - total1
        if total_delta == 0:
            return 0.0
        return round((1.0 - idle_delta / total_delta) * 100.0, 1)
    except Exception:
        if _PSUTIL:
            return psutil.cpu_percent(interval=0.3)
        return 0.0


def get_system_stats():
    """Returns (cpu_pct, ram_used_mb, ram_total_mb, gpu_pct, gpu_mem_used_mb, gpu_mem_total_mb)."""
    cpu = _read_cpu_pct()
    ram_used = ram_total = gpu = gmem_used = gmem_total = None
    if _PSUTIL:
        vm        = psutil.virtual_memory()
        ram_used  = vm.used  / 1024 ** 2
        ram_total = vm.total / 1024 ** 2
    else:
        # Fallback: read from /proc/meminfo
        try:
            mem = {}
            with open("/proc/meminfo") as f:
                for line in f:
                    k, v = line.split(":")
                    mem[k.strip()] = int(v.split()[0])  # kB
            ram_total = mem.get("MemTotal", 0) / 1024
            ram_free  = mem.get("MemAvailable", 0) / 1024
            ram_used  = ram_total - ram_free
        except Exception:
            pass
    if _PYNVML:
        util       = pynvml.nvmlDeviceGetUtilizationRates(_NVML_HANDLE)
        mem        = pynvml.nvmlDeviceGetMemoryInfo(_NVML_HANDLE)
        gpu        = util.gpu
        gmem_used  = mem.used  / 1024 ** 2
        gmem_total = mem.total / 1024 ** 2
    return cpu, ram_used, ram_total, gpu, gmem_used, gmem_total


def print_benchmark(stats: dict, elapsed: float) -> None:
    w = 70
    cpu, ram_used, ram_total, gpu, gmem_used, gmem_total = get_system_stats()

    print(f"\n{C['bold']}{C['blue']}{'─' * w}")
    print(f"  📊 BENCHMARK  ({elapsed:.1f}s window)")
    print(f"{'─' * w}{C['reset']}")

    if _PSUTIL:
        print(f"  {C['bold']}CPU {C['reset']}{pct_bar(cpu)}  "
              f"{C['gray']}RAM {ram_used:.0f}/{ram_total:.0f} MB{C['reset']}")
    if _PYNVML:
        print(f"  {C['bold']}GPU {C['reset']}{pct_bar(gpu)}  "
              f"{C['gray']}VRAM {gmem_used:.0f}/{gmem_total:.0f} MB{C['reset']}")
    print(f"  {C['dim']}{'─' * 66}{C['reset']}")

    total_frames = sum(s["frames"] for s in stats.values())
    total_dets   = sum(s["detections"] for s in stats.values())
    total_fps    = total_frames / elapsed if elapsed > 0 else 0

    for sid in sorted(stats):
        s        = stats[sid]
        sc       = STREAM_COLORS.get(sid, C["white"])
        fps      = s["frames"] / elapsed if elapsed > 0 else 0
        inf_avg  = (s["infer_ms_total"] / s["infer_calls"]) if s["infer_calls"] > 0 else 0
        det_rate = s["detections"] / s["frames"] if s["frames"] > 0 else 0
        fps_bar  = "█" * min(int(fps), 30) + "░" * max(0, 30 - int(fps))
        print(
            f"  {sc}{C['bold']}stream{sid + 1}{C['reset']}  "
            f"FPS {C['bold']}{fps:5.1f}{C['reset']} {C['gray']}{fps_bar}{C['reset']}  "
            f"infer {C['cyan']}{inf_avg:5.1f}ms{C['reset']}  "
            f"dets/frame {C['yellow']}{det_rate:.2f}{C['reset']}  "
            f"total dets {C['white']}{s['detections']}{C['reset']}"
        )

    print(f"  {C['dim']}{'─' * 66}{C['reset']}")
    print(
        f"  {C['bold']}TOTAL{C['reset']}   "
        f"FPS {C['bold']}{total_fps:5.1f}{C['reset']}  "
        f"frames {total_frames}  detections {C['yellow']}{total_dets}{C['reset']}"
    )
    print(f"{C['bold']}{C['blue']}{'─' * w}{C['reset']}\n")


class ConsoleDashboard:
    """Event dashboard — prints alerts immediately, summary via benchmark tick."""

    _ICONS = {
        "fall":  "🚨 FALL",
        "fire":  "🔥 FIRE",
        "smoke": "💨 SMOKE",
        "crowd": "👥 CROWD",
    }

    def __init__(self, num_streams: int = 4):
        self.num_streams = num_streams
        self._events     = defaultdict(int)
        self._total_dets = defaultdict(int)

    def update_stream(self, stream_id: int, detections: int,
                      fps: float, last_class: str = "", zone: str = None) -> None:
        self._total_dets[stream_id] += detections

    def push_event(self, event_type: str, stream_id: int,
                   cam_id: str, location: str, details: str) -> None:
        ts   = time.strftime("%H:%M:%S")
        icon = self._ICONS.get(event_type, "📌 EVENT")
        sc   = STREAM_COLORS.get(stream_id, C["white"])

        if event_type in ("fall", "fire"):
            ec = C["red"]
        elif event_type == "crowd" and any(z in details for z in ("CRITICAL", "HIGH")):
            ec = C["red"]
        elif event_type == "crowd" and "MEDIUM" in details:
            ec = C["yellow"]
        else:
            ec = sc

        self._events[stream_id] += 1
        print(
            f"{ec}{C['bold']}[{cam_id}]{C['reset']} "
            f"{ec}{location:<14}{C['reset']} "
            f"{C['bold']}{icon}{C['reset']}  "
            f"{C['gray']}{details}  ts={ts}{C['reset']}",
            flush=True,
        )

    def tick(self) -> None:
        pass  # summary handled by _benchmark_tick in pipeline
