"""
Frame Receiver — feeds raw numpy frames into the ClipWriter's per-stream buffers.

Two strategies, selected via CLIP_FRAME_SOURCE env var:

  queue   (default, PoC / WSL same-process):
      The pipeline's probe_handler pushes frames into a shared
      threading.Queue. FrameReceiver drains it and calls
      clip_writer.push_frame().

  appsink (production / Docker):
      A GStreamer appsink element emits new-sample signals.
      FrameReceiver connects to those signals and converts the
      GstSample to a numpy array before calling push_frame().

Usage (PoC):
    from services.frame_receiver import get_shared_queue, FrameReceiver
    from services.clip_writer import ClipWriter

    writer = ClipWriter()
    receiver = FrameReceiver(writer)
    receiver.start()          # starts drain thread
    # pipeline pushes: get_shared_queue().put_nowait((stream_id, frame, fps))
"""
import logging
import os
import queue
import threading
from typing import Optional, TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    from services.clip_writer import ClipWriter

logger = logging.getLogger(__name__)

CLIP_FRAME_SOURCE: str = os.getenv("CLIP_FRAME_SOURCE", "queue")

# ---------------------------------------------------------------------------
# Shared queue (PoC mode)
# Each item: (stream_id: int, frame: np.ndarray, fps: float)
# ---------------------------------------------------------------------------

_shared_queue: queue.Queue = queue.Queue(maxsize=64)


def get_shared_queue() -> queue.Queue:
    """Return the module-level shared frame queue used in PoC mode."""
    return _shared_queue


# ---------------------------------------------------------------------------
# FrameReceiver
# ---------------------------------------------------------------------------

class FrameReceiver:
    """
    Bridges the pipeline's frame output to the ClipWriter's frame buffers.

    Strategy is selected at construction time via the CLIP_FRAME_SOURCE env var
    (or the `source` constructor argument for testing).
    """

    def __init__(self, clip_writer: "ClipWriter", source: Optional[str] = None):
        self._writer = clip_writer
        self._source = source or CLIP_FRAME_SOURCE
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the receiver (idempotent)."""
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()

        if self._source == "queue":
            target = self._drain_queue
        elif self._source == "appsink":
            target = self._connect_appsink
        else:
            logger.warning("Unknown CLIP_FRAME_SOURCE=%r; defaulting to queue", self._source)
            target = self._drain_queue

        self._thread = threading.Thread(
            target=target,
            name="frame-receiver",
            daemon=True,
        )
        self._thread.start()
        logger.info("FrameReceiver started (source=%s)", self._source)

    def stop(self) -> None:
        """Signal the receiver to stop."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=3)

    # ------------------------------------------------------------------
    # Queue strategy (PoC)
    # ------------------------------------------------------------------

    def _drain_queue(self) -> None:
        """
        Continuously drain the shared queue and push frames to ClipWriter.
        Each queue item must be a tuple: (stream_id, frame, fps).
        """
        q = get_shared_queue()
        while not self._stop_event.is_set():
            try:
                item = q.get(timeout=0.5)
            except queue.Empty:
                continue

            try:
                stream_id, frame, fps = item
                if not isinstance(frame, np.ndarray):
                    logger.warning("FrameReceiver: non-ndarray frame dropped for stream %d", stream_id)
                    continue
                self._writer.push_frame(stream_id, frame, fps)
            except (TypeError, ValueError) as exc:
                logger.warning("FrameReceiver: malformed queue item: %s", exc)

    # ------------------------------------------------------------------
    # Appsink strategy (production)
    # ------------------------------------------------------------------

    def _connect_appsink(self) -> None:
        """
        Connect to GStreamer appsink new-sample signals.

        Expects the pipeline to have registered appsink elements under the
        name pattern "appsink_{stream_id}" in the default GStreamer registry.
        This method blocks until stop() is called.

        Requires: gi (PyGObject), GStreamer Python bindings.
        """
        try:
            import gi
            gi.require_version("Gst", "1.0")
            from gi.repository import Gst, GLib
        except (ImportError, ValueError) as exc:
            logger.error(
                "FrameReceiver: appsink mode requires GStreamer Python bindings: %s. "
                "Falling back to queue mode.",
                exc,
            )
            self._drain_queue()
            return

        Gst.init(None)

        # The pipeline is expected to expose appsink elements via a shared
        # registry dict populated by the pipeline process.
        # We poll for them until stop() is called.
        connected: set = set()
        main_loop = GLib.MainLoop()

        def _on_new_sample(appsink, stream_id: int):
            sample = appsink.emit("pull-sample")
            if sample is None:
                return Gst.FlowReturn.OK
            buf = sample.get_buffer()
            caps = sample.get_caps()
            structure = caps.get_structure(0)
            width = structure.get_value("width")
            height = structure.get_value("height")
            success, map_info = buf.map(Gst.MapFlags.READ)
            if not success:
                return Gst.FlowReturn.OK
            try:
                frame = np.frombuffer(map_info.data, dtype=np.uint8).reshape(height, width, 3)
                self._writer.push_frame(stream_id, frame.copy())
            finally:
                buf.unmap(map_info)
            return Gst.FlowReturn.OK

        # Run main loop in a thread; poll for new appsinks
        loop_thread = threading.Thread(target=main_loop.run, daemon=True)
        loop_thread.start()

        try:
            while not self._stop_event.is_set():
                # Appsink elements are registered externally; check every second
                from pipeline import _appsink_registry  # type: ignore[import]
                for stream_id, appsink in _appsink_registry.items():
                    if stream_id not in connected:
                        appsink.connect(
                            "new-sample",
                            lambda sink, sid=stream_id: _on_new_sample(sink, sid),
                        )
                        appsink.set_property("emit-signals", True)
                        connected.add(stream_id)
                        logger.info("FrameReceiver: connected appsink for stream %d", stream_id)
                self._stop_event.wait(timeout=1.0)
        except ImportError:
            logger.warning(
                "FrameReceiver: pipeline._appsink_registry not found; "
                "appsink mode unavailable. Falling back to queue drain."
            )
            main_loop.quit()
            self._drain_queue()
        finally:
            main_loop.quit()
