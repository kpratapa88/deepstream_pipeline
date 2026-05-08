"""
Pipeline launcher abstraction.

Selected via PIPELINE_LAUNCHER env var:
  - "subprocess" (default): spawns python main.py as a child process
  - "docker": sends SIGHUP/SIGTERM to the pipeline process inside a running container

Requirements: 9.4, 9.6
"""
import json
import logging
import os
import signal
import subprocess
import threading
import time
from typing import Optional

logger = logging.getLogger(__name__)

# Status constants
STATUS_STOPPED = "stopped"
STATUS_STARTING = "starting"
STATUS_RUNNING = "running"
STATUS_ERROR = "error"


class SubprocessLauncher:
    """
    Launches the DeepStream pipeline.

    If PIPELINE_CONTAINER env var is set, runs inside the named Docker container
    via `docker exec`. Otherwise spawns python main.py directly (requires gi/pyds).
    """

    def __init__(self):
        self._process: Optional[subprocess.Popen] = None
        self._status: str = STATUS_STOPPED
        self._last_error: str = ""
        self._lock = threading.Lock()
        from backend.config import settings
        self._container: str = settings.PIPELINE_CONTAINER

    def start(self, rules_path: str, args: dict) -> None:
        with self._lock:
            backend_url = os.environ.get("BACKEND_URL", "http://localhost:8000")
            kafka_broker = args.get("kafka_broker", "localhost:9092")
            interval = str(args.get("inference_interval", 4))

            if self._container:
                # Copy rules.json into container then launch
                container_rules = "/app/configs/rules_launch.json"
                try:
                    with open(rules_path) as f:
                        rules_content = f.read()
                    write_cmd = [
                        "docker", "exec", self._container,
                        "sh", "-c", f"cat > {container_rules}",
                    ]
                    subprocess.run(write_cmd, input=rules_content, text=True,
                                   capture_output=True, timeout=10, check=True)
                except Exception as exc:
                    self._status = STATUS_ERROR
                    self._last_error = str(exc)
                    raise

                cmd = [
                    "docker", "exec", "-d", self._container,
                    "sh", "-c",
                    f"BACKEND_URL={backend_url} python3 main.py "
                    f"--config {container_rules} "
                    f"--kafka-broker {kafka_broker} "
                    f"--interval {interval} "
                    f"> /tmp/pipeline.log 2>&1"
                ]
            else:
                cmd = [
                    "python", "main.py",
                    "--config", rules_path,
                    "--kafka-broker", kafka_broker,
                    "--interval", interval,
                ]

            logger.info("SubprocessLauncher: %s", cmd)
            self._status = STATUS_STARTING
            self._last_error = ""
            try:
                self._process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                threading.Thread(target=self._monitor, daemon=True).start()
                self._status = STATUS_RUNNING
            except Exception as exc:
                self._status = STATUS_ERROR
                self._last_error = str(exc)
                logger.error("SubprocessLauncher: failed to start: %s", exc)
                raise

    def stop(self) -> None:
        """Send SIGTERM and wait up to 10 seconds."""
        with self._lock:
            if self._process is None or self._process.poll() is not None:
                self._status = STATUS_STOPPED
                return
            logger.info("SubprocessLauncher: sending SIGTERM to pid %d", self._process.pid)
            try:
                self._process.terminate()
                try:
                    self._process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    logger.warning("SubprocessLauncher: SIGTERM timed out, sending SIGKILL")
                    self._process.kill()
                    self._process.wait()
            except Exception as exc:
                logger.error("SubprocessLauncher: error during stop: %s", exc)
            finally:
                self._process = None
                self._status = STATUS_STOPPED

    def status(self) -> str:
        with self._lock:
            if self._process is not None and self._process.poll() is not None:
                # Process exited unexpectedly
                self._status = STATUS_ERROR
                self._process = None
            return self._status

    @property
    def last_error(self) -> str:
        return self._last_error

    def _monitor(self):
        """Background thread: wait for process exit and update status."""
        proc = self._process
        if proc is None:
            return
        _, stderr_output = proc.communicate()
        with self._lock:
            if self._process is proc:  # still the same process
                if proc.returncode != 0:
                    self._status = STATUS_ERROR
                    lines = [l for l in stderr_output.splitlines() if l.strip()]
                    self._last_error = lines[-1] if lines else f"exit code {proc.returncode}"
                    logger.error("SubprocessLauncher: pipeline exited with error: %s", self._last_error)
                else:
                    self._status = STATUS_STOPPED
                self._process = None


class DockerLauncher:
    """
    Controls the pipeline process *inside* an already-running Docker container.
    The container itself is never stopped or restarted.

    PIPELINE_LAUNCHER=docker
    PIPELINE_CONTAINER env var: container name/id (default "deepstream")
    PIPELINE_CONFIG_VOLUME env var: path inside container for rules.json
                                    (default "/app/configs/rules.json")

    Start: writes new rules.json to the mounted config volume,
           then sends SIGHUP to the pipeline process inside the container.
    Stop:  sends SIGTERM to the pipeline process inside the container.
    """

    def __init__(self):
        self._container: str = os.environ.get("PIPELINE_CONTAINER", "deepstream")
        self._config_path: str = os.environ.get(
            "PIPELINE_CONFIG_VOLUME", "/app/configs/rules.json"
        )
        self._status: str = STATUS_STOPPED
        self._last_error: str = ""
        self._lock = threading.Lock()

    def start(self, rules_path: str, args: dict) -> None:
        """Write rules.json into the container volume and send SIGHUP."""
        with self._lock:
            # Read the generated rules.json and copy it into the container
            try:
                with open(rules_path, "r") as f:
                    rules_content = f.read()
            except OSError as exc:
                self._status = STATUS_ERROR
                self._last_error = str(exc)
                raise

            # Write to container via docker exec + tee
            write_cmd = [
                "docker", "exec", self._container,
                "sh", "-c", f"cat > {self._config_path}",
            ]
            logger.info("DockerLauncher: writing rules.json to container %s", self._container)
            self._status = STATUS_STARTING
            self._last_error = ""
            try:
                result = subprocess.run(
                    write_cmd,
                    input=rules_content,
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                if result.returncode != 0:
                    raise RuntimeError(result.stderr.strip())
            except Exception as exc:
                self._status = STATUS_ERROR
                self._last_error = str(exc)
                logger.error("DockerLauncher: failed to write config: %s", exc)
                raise

            # Send SIGHUP to the pipeline process inside the container
            hup_cmd = [
                "docker", "exec", self._container,
                "sh", "-c", "kill -HUP $(pgrep -f 'python main.py') 2>/dev/null || true",
            ]
            logger.info("DockerLauncher: sending SIGHUP to pipeline in container %s", self._container)
            try:
                result = subprocess.run(hup_cmd, capture_output=True, text=True, timeout=10)
                if result.returncode != 0:
                    raise RuntimeError(result.stderr.strip())
                self._status = STATUS_RUNNING
            except Exception as exc:
                self._status = STATUS_ERROR
                self._last_error = str(exc)
                logger.error("DockerLauncher: failed to send SIGHUP: %s", exc)
                raise

    def stop(self) -> None:
        """Send SIGTERM to the pipeline process inside the container."""
        with self._lock:
            term_cmd = [
                "docker", "exec", self._container,
                "sh", "-c", "kill -TERM $(pgrep -f 'python main.py') 2>/dev/null || true",
            ]
            logger.info("DockerLauncher: sending SIGTERM to pipeline in container %s", self._container)
            try:
                subprocess.run(term_cmd, capture_output=True, text=True, timeout=15)
            except Exception as exc:
                logger.error("DockerLauncher: error during stop: %s", exc)
            finally:
                self._status = STATUS_STOPPED

    def status(self) -> str:
        with self._lock:
            return self._status

    @property
    def last_error(self) -> str:
        return self._last_error


def get_launcher():
    """
    Factory: return the launcher selected by PIPELINE_LAUNCHER env var.
    Defaults to SubprocessLauncher.
    """
    launcher_type = os.environ.get("PIPELINE_LAUNCHER", "subprocess").lower()
    if launcher_type == "docker":
        return DockerLauncher()
    return SubprocessLauncher()


# Module-level singleton
pipeline_launcher = get_launcher()
