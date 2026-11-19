"""A real cluster for the integration tests: four node servers as separate OS
processes (python -m nodes.node_server), each with its own data folder and
sending real heartbeats, plus the real Flask API (app.py) served on a local
port from a background thread.

One cluster is started for the whole test session (starting a node takes a
second or two); each test gets it back in a clean state through reset().
"""
import importlib
import os
import shutil
import socket
import subprocess
import sys
import threading
import time

import requests
from werkzeug.serving import make_server

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NODE_IDS = ["worker1", "worker2", "worker3", "worker4"]
HEARTBEAT_INTERVAL = 0.5     # seconds between a node's heartbeats in the tests
HEARTBEAT_TIMEOUT = 2.0      # silent this long -> the controller treats the node as down


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_until(condition, seconds: float, interval: float = 0.1) -> bool:
    """Poll `condition` until it is true or the time is up."""
    end = time.time() + seconds
    while time.time() < end:
        if condition():
            return True
        time.sleep(interval)
    return bool(condition())


class Cluster:
    def __init__(self, base_dir: str, capacity: int = 50 * 1024 * 1024):
        self.base_dir = base_dir
        self.capacity = capacity
        self.node_ports = {n: free_port() for n in NODE_IDS}
        self.api_port = free_port()
        self.procs = {}
        self.extra = {}       # node_id -> port of workers started by a test, NOT in the WORKERS list
        self.server = None
        self.app_module = None

    # --- lifecycle ---------------------------------------------------------
    def start(self) -> None:
        # The API reads the worker list from WORKERS when config.py is first
        # imported, so set it before importing app.
        os.environ["WORKERS"] = ",".join(
            f"{n}=http://127.0.0.1:{p}" for n, p in self.node_ports.items())
        # the metadata is saved like in the real deployment (and cleanup needs it)
        os.environ["METADATA_PATH"] = self.metadata_path
        # Only app and config are imported afresh. The controller.* modules are
        # left alone, so every test module sees the same Distributor/RemoteNode
        # classes (and the ones fixtures patch).
        for name in ("app", "config"):
            sys.modules.pop(name, None)
        self.app_module = importlib.import_module("app")
        os.environ.pop("WORKERS", None)       # config has read it; do not leak it into other tests
        os.environ.pop("METADATA_PATH", None)
        self.distributor.heartbeat_timeout = HEARTBEAT_TIMEOUT

        self.server = make_server("127.0.0.1", self.api_port, self.app_module.app, threaded=True)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

        for n in NODE_IDS:
            self._spawn(n)
        self.wait_all_alive()

    def stop(self) -> None:
        for n in list(self.procs):
            self.kill(n)
        if self.server:
            self.server.shutdown()

    def _spawn(self, node_id: str, port: int = None, controller_url: str = None) -> None:
        port = port or self.node_ports[node_id]
        env = dict(
            os.environ, NODE_ID=node_id, PORT=str(port), HOST="127.0.0.1",
            CAPACITY=str(self.capacity), DATA_DIR=self.node_dir(node_id),
            CONTROLLER_URL=controller_url or f"http://127.0.0.1:{self.api_port}",
            HEARTBEAT_INTERVAL=str(HEARTBEAT_INTERVAL),
        )
        env.pop("ADVERTISE_URL", None)
        # Its output goes to a log file (appended across restarts), so a
        # worker that does not come up can say why (wait_all_alive).
        os.makedirs(self.log_dir, exist_ok=True)
        with open(self.log_path(node_id), "ab") as log:
            self.procs[node_id] = subprocess.Popen(
                [sys.executable, "-m", "nodes.node_server"], env=env, cwd=REPO_ROOT,
                stdout=log, stderr=subprocess.STDOUT)

    def start_extra(self, node_id: str, controller_url: str = None) -> int:
        """Start a worker the controller has never heard of (not in WORKERS):
        it has to register itself. Returns its port. reset() removes it."""
        port = free_port()
        self.extra[node_id] = port
        self._spawn(node_id, port, controller_url)
        return port

    def _remove_extras(self) -> None:
        for node_id in list(self.extra):
            self.kill(node_id)
            self.procs.pop(node_id, None)
            shutil.rmtree(self.node_dir(node_id), ignore_errors=True)
            del self.extra[node_id]
        d = self.distributor
        with d._nodes_lock:
            d.nodes = [n for n in d.nodes if n.node_id in NODE_IDS]
        with d._status_lock:
            for node_id in [k for k in d._status if k not in NODE_IDS]:
                del d._status[node_id]

    # --- convenient views --------------------------------------------------
    @property
    def api(self) -> str:
        return f"http://127.0.0.1:{self.api_port}/api/v1"

    @property
    def root(self) -> str:
        return f"http://127.0.0.1:{self.api_port}"

    @property
    def distributor(self):
        return self.app_module.distributor

    def node_url(self, node_id: str) -> str:
        port = self.extra.get(node_id) or self.node_ports[node_id]
        return f"http://127.0.0.1:{port}"

    @property
    def metadata_path(self) -> str:
        return os.path.join(self.base_dir, "controller", "metadata.db")

    def node_dir(self, node_id: str) -> str:
        return os.path.join(self.base_dir, node_id)

    @property
    def log_dir(self) -> str:
        return os.path.join(self.base_dir, "logs")

    def log_path(self, node_id: str) -> str:
        return os.path.join(self.log_dir, f"{node_id}.log")

    def is_running(self, node_id: str) -> bool:
        p = self.procs.get(node_id)
        return p is not None and p.poll() is None

    def is_alive(self, node_id: str) -> bool:
        """The controller's own view (from heartbeats). False for a worker that
        has not registered yet."""
        try:
            node = self.distributor._get_node(node_id)
        except KeyError:
            return False
        return self.distributor._cached_state(node)[0]

    # --- failure injection -------------------------------------------------
    def kill(self, node_id: str) -> None:
        """Stop a node's process (like `docker stop`)."""
        p = self.procs.get(node_id)
        if p is not None and p.poll() is None:
            p.terminate()
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()

    def restart(self, node_id: str) -> None:
        """Start a stopped node again with the same data folder (like `docker start`)."""
        if not self.is_running(node_id):
            self._spawn(node_id)

    def wait_alive(self, node_id: str, seconds: float = 15) -> bool:
        return wait_until(lambda: self.is_alive(node_id), seconds)

    def wait_down(self, node_id: str, seconds: float = HEARTBEAT_TIMEOUT + 5) -> bool:
        return wait_until(lambda: not self.is_alive(node_id), seconds)

    def wait_all_alive(self, seconds: float = 20) -> None:
        for n in NODE_IDS:
            if not self.wait_alive(n, seconds):
                raise RuntimeError(f"{n} did not come up\n{self.why_not_alive(n)}")

    def why_not_alive(self, node_id: str) -> str:
        """What is known about a worker that does not count as alive: is its
        process running, what the controller last heard from it, and the end
        of its log."""
        p = self.procs.get(node_id)
        process = "never started" if p is None else (
            "running" if p.poll() is None else f"exited with code {p.returncode}")
        try:
            age = self.distributor.heartbeat_age(node_id)
        except Exception as e:                  # what we want is the report, whatever happens
            age = f"? ({e})"
        try:
            with open(self.log_path(node_id), "rb") as f:
                tail = f.read()[-3000:].decode("utf-8", "replace")
        except OSError:
            tail = "(no log)"
        return (f"process: {process}; port {self.node_ports.get(node_id) or self.extra.get(node_id)}; "
                f"last heartbeat {age} s ago (timeout {self.distributor.heartbeat_timeout} s)\n"
                f"--- end of {self.log_path(node_id)} ---\n{tail}")

    # --- what is on disk ---------------------------------------------------
    def chunk_files(self, node_id: str) -> list:
        """Chunk ids stored on a node's disk (not the hidden checksum files)."""
        d = self.node_dir(node_id)
        if not os.path.isdir(d):
            return []
        return sorted(f for f in os.listdir(d)
                      if not f.startswith(".") and os.path.isfile(os.path.join(d, f)))

    def total_chunk_files(self) -> int:
        return sum(len(self.chunk_files(n)) for n in NODE_IDS)

    def chunk_path(self, node_id: str, chunk_id: str) -> str:
        return os.path.join(self.node_dir(node_id), chunk_id)

    def damage_chunk(self, node_id: str, chunk_id: str) -> None:
        """Flip a byte of the chunk file on disk (the checksum file is left alone)."""
        path = self.chunk_path(node_id, chunk_id)
        with open(path, "rb") as f:
            data = bytearray(f.read())
        data[len(data) // 2] ^= 0xFF
        with open(path, "wb") as f:
            f.write(bytes(data))

    # --- back to a clean state ---------------------------------------------
    def reset(self) -> None:
        """Every node running and alive, no objects, no chunk files, no extra workers."""
        self._remove_extras()
        for n in NODE_IDS:
            self.restart(n)
        self.wait_all_alive()
        self.distributor.heartbeat_timeout = HEARTBEAT_TIMEOUT
        for obj in requests.get(self.api + "/objects", timeout=30).json():
            requests.delete(f"{self.api}/objects/{obj['object_id']}", timeout=30)
        # chunks left behind (for example copies on a node that was down while
        # their object was deleted or repaired)
        for n in NODE_IDS:
            d = self.node_dir(n)
            for sub in (d, os.path.join(d, ".checksums")):
                if os.path.isdir(sub):
                    for f in os.listdir(sub):
                        p = os.path.join(sub, f)
                        if os.path.isfile(p):
                            os.remove(p)
        d = self.distributor
        with d._lock:
            d.metadata.clear()
            d.paths.clear()
            d._save()
        # placement and auto-repair start from scratch, so every test sees the same behaviour
        d._pair_use.clear()
        d.cache.clear()                  # no test sees chunks another one downloaded
        d._rotation = 0
        d._down_since.clear()
        d._repaired_for.clear()
        d.last_auto_repair = None
        # make sure every node's heartbeat table entry is fresh and alive
        self.wait_all_alive()

    def cleanup_dirs(self) -> None:
        shutil.rmtree(self.base_dir, ignore_errors=True)
