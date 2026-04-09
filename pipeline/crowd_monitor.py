"""
Crowd density monitoring: person count → zone classification → OSD overlay.
"""
from collections import defaultdict, deque
from .constants import C


class CrowdMonitor:
    """
    Tracks rolling person count per stream and classifies crowd density zones.
    Emits zone-change events via the dashboard.
    """

    _ZONE_COLORS = {
        "CRITICAL": (1.0, 0.0, 0.0),
        "HIGH":     (1.0, 0.3, 0.0),
        "MEDIUM":   (1.0, 1.0, 0.0),
        "LOW":      (0.0, 1.0, 1.0),
        "CLEAR":    (0.0, 1.0, 0.0),
    }

    def __init__(self, window_size: int = 10):
        self._counts  = defaultdict(lambda: deque(maxlen=window_size))
        self._zones   = defaultdict(str)   # {stream_id: last_zone}

    def update(self, stream_id: int, person_count: int,
               thresholds: dict, dashboard, cam_id: str, location: str) -> str:
        """
        Update rolling count, classify zone, emit event on zone change.
        Returns current zone string.
        """
        self._counts[stream_id].append(person_count)
        avg = sum(self._counts[stream_id]) / len(self._counts[stream_id])

        if avg >= thresholds.get("critical", 999):
            zone = "CRITICAL"
        elif avg >= thresholds.get("high", 999):
            zone = "HIGH"
        elif avg >= thresholds.get("medium", 999):
            zone = "MEDIUM"
        elif avg >= thresholds.get("low", 999):
            zone = "LOW"
        else:
            zone = "CLEAR"

        if zone != self._zones[stream_id]:
            self._zones[stream_id] = zone
            dashboard.push_event(
                "crowd", stream_id, cam_id, location,
                f"zone={zone}  count={person_count}  avg={avg:.1f}"
            )

        return zone

    def current_zone(self, stream_id: int) -> str:
        return self._zones.get(stream_id, "CLEAR")

    def current_avg(self, stream_id: int) -> float:
        counts = self._counts[stream_id]
        return sum(counts) / len(counts) if counts else 0.0

    def draw_osd(self, frame_meta, batch_meta, stream_id: int) -> None:
        """Render crowd density text overlay on the stream tile."""
        import pyds
        zone  = self.current_zone(stream_id)
        avg   = self.current_avg(stream_id)
        last  = self._counts[stream_id][-1] if self._counts[stream_id] else 0
        r, g, b = self._ZONE_COLORS.get(zone, (0.0, 1.0, 0.0))
        try:
            dm = pyds.nvds_acquire_display_meta_from_pool(batch_meta)
            dm.num_labels = 1
            t  = dm.text_params[0]
            t.display_text = f"CROWD: {zone}  persons={last}  avg={avg:.1f}"
            t.x_offset = 10
            t.y_offset = 10
            t.font_params.font_name = "Serif Bold"
            t.font_params.font_size = 10
            t.font_params.font_color.set(r, g, b, 1.0)
            t.set_bg_clr = 1
            t.text_bg_clr.set(0.0, 0.0, 0.0, 0.7)
            pyds.nvds_add_display_meta_to_frame(frame_meta, dm)
        except Exception:
            pass
