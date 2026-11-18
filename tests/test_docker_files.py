"""The Docker files, checked without Docker: that each image's allowlist
contains everything its program needs (and nothing it should not have), that
the settings the containers rely on really work, and that the Dockerfiles say
what the spec requires."""
import os
import re
import shutil
import subprocess
import sys

import pytest

from config import parse_workers
from tests.cluster import REPO_ROOT


def read(name):
    with open(os.path.join(REPO_ROOT, name), encoding="utf-8") as f:
        return f.read()


def allowlist(dockerignore_name):
    """The paths a `*` + `!path` .dockerignore lets into the image."""
    lines = [l.strip() for l in read(dockerignore_name).splitlines()]
    assert "*" in lines, "an allowlist starts by excluding everything"
    return [l[1:].rstrip("/") for l in lines if l.startswith("!")]


def build_context(tmp_path, dockerignore_name):
    """Copy only what the image would contain."""
    dest = tmp_path / "image"
    dest.mkdir()
    for entry in allowlist(dockerignore_name):
        src = os.path.join(REPO_ROOT, entry)
        assert os.path.exists(src), f"{dockerignore_name} allows '{entry}' which does not exist"
        if os.path.isdir(src):
            shutil.copytree(src, dest / entry, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        else:
            shutil.copy(src, dest / entry)
    return dest


SETTINGS = ("WORKERS", "HOST", "PORT", "DEBUG", "NODE_ID", "DATA_DIR", "CONTROLLER_URL", "CAPACITY",
            "HEARTBEAT_INTERVAL", "CLIENT_ID", "API_URL", "CHUNK_SIZE", "REPLICATION_FACTOR",
            "ADVERTISE_URL", "WORKER_NAME")


def run_python(code, cwd, **env):
    """Run python in a clean environment: only the settings the test passes are set."""
    clean = {k: v for k, v in os.environ.items() if k not in SETTINGS}
    # METADATA_PATH="": never read or write the real ./data/controller of this checkout
    return subprocess.run([sys.executable, "-c", code], cwd=cwd, capture_output=True, text=True,
                          env={**clean, "PYTHONDONTWRITEBYTECODE": "1", "PYTHONPATH": "", "METADATA_PATH": "",
                               **env}, timeout=60)


# ----------------------------------------------------------- the allowlists
class TestImageContents:
    def test_the_controller_image_has_what_app_py_needs_and_starts(self, tmp_path):
        ctx = build_context(tmp_path, "Dockerfile.controller.dockerignore")
        r = run_python("import app, config; print(sorted(config.NODE_ADDRESSES))", ctx,
                       WORKERS="worker1=http://worker1:5000,worker2=http://worker2:5000")
        assert r.returncode == 0, r.stderr
        assert "['worker1', 'worker2']" in r.stdout

    def test_the_worker_image_has_what_node_server_needs_and_starts(self, tmp_path):
        ctx = build_context(tmp_path, "Dockerfile.worker.dockerignore")
        r = run_python("import nodes.node_server as s; print(s.NODE_ID, s.DATA_DIR)", ctx,
                       NODE_ID="worker2", DATA_DIR=str(tmp_path / "data"))
        assert r.returncode == 0, r.stderr
        assert "worker2" in r.stdout

    def test_the_controller_image_does_not_contain_node_code_or_the_client(self):
        allowed = allowlist("Dockerfile.controller.dockerignore")
        assert "nodes" not in allowed and "client.py" not in allowed
        assert {"app.py", "config.py", "controller", "helpers", "requirements.txt"} <= set(allowed)

    def test_the_worker_image_does_not_contain_controller_code_or_the_client(self):
        allowed = allowlist("Dockerfile.worker.dockerignore")
        assert not {"app.py", "controller", "client.py"} & set(allowed)
        assert {"config.py", "nodes", "helpers", "requirements.txt"} <= set(allowed)

    def test_the_client_image_has_what_client_py_needs_and_starts(self, tmp_path):
        ctx = build_context(tmp_path, "Dockerfile.client.dockerignore")
        r = run_python("import client; print(client.BASE_URL, client.default_client_id())", ctx,
                       API_URL="http://controller:5000/api/v1", CLIENT_ID="client7")
        assert r.returncode == 0, r.stderr
        assert r.stdout.split() == ["http://controller:5000/api/v1", "client7"]

    def test_the_client_image_contains_only_the_client(self):
        allowed = set(allowlist("Dockerfile.client.dockerignore"))
        assert allowed == {"client.py", "helpers", "requirements.txt"}

    @pytest.mark.parametrize("name", ["Dockerfile.controller.dockerignore", "Dockerfile.worker.dockerignore",
                                      "Dockerfile.client.dockerignore"])
    def test_local_data_and_caches_never_get_into_an_image(self, name):
        allowed = allowlist(name)
        assert not {"data", ".git", ".venv", "tests", ".env"} & set(allowed)

    def test_the_controller_code_never_imports_node_code_and_the_other_way_round(self):
        def imports(folder):
            found = set()
            for root, _, names in os.walk(os.path.join(REPO_ROOT, folder)):
                for n in names:
                    if n.endswith(".py"):
                        found |= set(re.findall(r"^\s*(?:from|import)\s+([\w.]+)", open(os.path.join(root, n), encoding="utf-8").read(), re.M))
            return {i.split(".")[0] for i in found}
        assert "nodes" not in imports("controller") and "controller" not in imports("nodes")
        assert "app" not in imports("controller") | imports("nodes") | imports("helpers")


# ------------------------------------------------------------- the settings
class TestWorkersSetting:
    def test_parse(self):
        assert parse_workers("worker1=http://worker1:5000, worker2=https://w2:6000") == {
            "worker1": "http://worker1:5000", "worker2": "https://w2:6000"}

    @pytest.mark.parametrize("bad", ["", "   ", ",", "a", "a=", "=http://x", "a=ftp://x", "a=http://x,b", "a=x:5000"])
    def test_malformed_values_are_rejected_with_a_clear_message(self, bad):
        with pytest.raises(ValueError, match="WORKERS"):
            parse_workers(bad)

    def test_no_list_is_needed_workers_register_themselves(self):
        r = run_python("import config; print(config.NODE_ADDRESSES)", REPO_ROOT)
        assert r.returncode == 0 and r.stdout.strip() == "{}"

    def test_an_explicit_list_can_still_be_given(self):
        r = run_python("import config; print(config.NODE_ADDRESSES)", REPO_ROOT, WORKERS="x=http://x:1")
        assert r.stdout.strip() == "{'x': 'http://x:1'}"

    def test_a_bad_value_stops_the_controller_at_startup_instead_of_running_misconfigured(self):
        r = run_python("import config", REPO_ROOT, WORKERS="not-valid")
        assert r.returncode != 0 and "WORKERS" in r.stderr


class TestStorageSettings:
    """CHUNK_SIZE and REPLICATION_FACTOR (set in docker-compose.yml) reach the distributor."""

    def test_defaults_are_the_spec_values(self):
        r = run_python("import app; d = app.distributor; print(d.chunk_size, d.replication_factor)", REPO_ROOT)
        assert r.returncode == 0, r.stderr
        assert r.stdout.split() == ["1048576", "2"]

    def test_the_environment_changes_them(self):
        r = run_python("import app; d = app.distributor; print(d.chunk_size, d.replication_factor)", REPO_ROOT,
                       CHUNK_SIZE="524288", REPLICATION_FACTOR="3")
        assert r.returncode == 0, r.stderr
        assert r.stdout.split() == ["524288", "3"]

    @pytest.mark.parametrize("name,value", [("CHUNK_SIZE", "0"), ("CHUNK_SIZE", "-5"), ("CHUNK_SIZE", "1MB"),
                                            ("REPLICATION_FACTOR", "0"), ("REPLICATION_FACTOR", "two")])
    def test_bad_values_stop_the_controller_at_startup(self, name, value):
        r = run_python("import config", REPO_ROOT, **{name: value})
        assert r.returncode != 0 and name in r.stderr

    def test_an_empty_value_means_the_default(self):
        r = run_python("import config; print(config.CHUNK_SIZE)", REPO_ROOT, CHUNK_SIZE="  ")
        assert r.stdout.strip() == "1048576"


# ----------------------------------------------------------- the compose file
class TestComposeFile:
    """docker-compose.yml, checked as text (no Docker needed)."""

    @pytest.fixture
    def compose(self):
        return read("docker-compose.yml")

    def setting(self, compose, name):
        """The default of a setting in the x-settings block: '&name ${VAR:-default}'."""
        m = re.search(r"&%s\s+\$\{(\w+):-([^}]*)\}" % name, compose)
        assert m, f"setting '{name}' missing from x-settings"
        return m.group(1), m.group(2)

    def test_every_parameter_is_in_the_settings_block_with_the_spec_defaults(self, compose):
        assert self.setting(compose, "workers") == ("WORKERS", "4")
        assert self.setting(compose, "chunk_size") == ("CHUNK_SIZE", "1048576")
        assert self.setting(compose, "replication_factor") == ("REPLICATION_FACTOR", "2")
        assert self.setting(compose, "worker_capacity") == ("WORKER_CAPACITY", "5242880")
        assert self.setting(compose, "worker_name") == ("WORKER_NAME", "worker")
        assert self.setting(compose, "api_port") == ("API_PORT", "5000")
        assert self.setting(compose, "auto_repair_grace") == ("AUTO_REPAIR_GRACE", "30")

    def test_the_settings_are_actually_used(self, compose):
        for alias, where in [("workers", "replicas: *workers"), ("chunk_size", "CHUNK_SIZE: *chunk_size"),
                             ("replication_factor", "REPLICATION_FACTOR: *replication_factor"),
                             ("worker_capacity", "CAPACITY: *worker_capacity"),
                             ("worker_name", "WORKER_NAME: *worker_name"), ("api_port", "published: *api_port"),
                             ("auto_repair_grace", "AUTO_REPAIR_GRACE: *auto_repair_grace")]:
            assert where in compose, f"{alias} is defined but not used ({where})"

    def test_the_controller_service_is_called_controller(self, compose):
        # the workers send to http://controller:5000 (Dockerfile.worker)
        assert re.search(r"^  controller:\s*$", compose, re.M)

    def test_only_the_controller_publishes_a_port(self, compose):
        worker_part = compose[compose.index("\n  worker:"):compose.index("\nnetworks:")]
        assert "ports" not in worker_part and "published" not in worker_part
        assert compose.count("ports:") == 1

    def test_the_data_lives_in_named_volumes(self, compose):
        assert "- worker-data:/data" in compose and "- controller-data:/meta" in compose
        tail = compose[compose.index("\nvolumes:"):]
        assert "worker-data:" in tail and "controller-data:" in tail

    def test_all_images_are_built_from_this_folder(self, compose):
        for name in ("Dockerfile.controller", "Dockerfile.worker", "Dockerfile.client"):
            assert f"dockerfile: {name}" in compose
        assert compose.count("context: .") == 3

    def test_two_clients_with_their_own_stable_ids(self, compose):
        assert "CLIENT_ID: client1" in compose and "CLIENT_ID: client2" in compose
        assert compose.count("./files:/files:ro") == 2
        assert "./downloads/client1:/downloads" in compose and "./downloads/client2:/downloads" in compose
        assert "profiles: [clients]" in compose          # started on demand, not by `up`

    def test_the_worker_finds_its_own_id_and_folder(self):
        text = read("Dockerfile.worker")
        assert "NODE_ID=auto" in text and "DATA_DIR=/data/{node_id}" in text


class TestControllerEntrypoint:
    def test_host_port_and_debug_come_from_the_environment(self):
        source = read("app.py")
        assert 'os.environ.get("HOST"' in source and 'os.environ.get("PORT"' in source
        assert 'os.environ.get("DEBUG") == "1"' in source
        assert "debug=True" not in source          # the interactive debugger must not be on by default


# ----------------------------------------------------------- the Dockerfiles
class TestDockerfiles:
    @pytest.mark.parametrize("name", ["Dockerfile.controller", "Dockerfile.worker"])
    def test_common_requirements(self, name):
        text = read(name)
        assert re.search(r"^FROM python:3\.\d+-slim", text, re.M), "a pinned, small base image"
        assert "COPY requirements.txt" in text and "pip install --no-cache-dir -r requirements.txt" in text
        assert re.search(r"^USER \w+", text, re.M), "must not run as root"
        assert "HEALTHCHECK" in text and "HOST=0.0.0.0" in text
        # dependencies are installed before the code is copied, so the layer is cached
        assert text.index("pip install") < text.index("COPY . .")

    def test_the_worker_keeps_its_chunks_in_a_volume(self):
        text = read("Dockerfile.worker")
        assert 'VOLUME ["/data"]' in text and "DATA_DIR=/data/" in text
        assert text.index("chown app:app /data") < text.index("VOLUME"), "the volume must start out writable by the user"

    def test_start_commands(self):
        assert 'CMD ["python", "app.py"]' in read("Dockerfile.controller")
        assert 'CMD ["python", "-m", "nodes.node_server"]' in read("Dockerfile.worker")
        assert 'ENTRYPOINT ["python", "/app/client.py"]' in read("Dockerfile.client")

    def test_the_controller_keeps_its_metadata_in_a_volume(self):
        text = read("Dockerfile.controller")
        assert 'VOLUME ["/meta"]' in text and "METADATA_PATH=/meta/metadata.db" in text
        assert text.index("chown app:app /meta") < text.index("VOLUME")

    def test_the_client_image(self):
        text = read("Dockerfile.client")
        assert re.search(r"^USER \w+", text, re.M)
        assert "API_URL=http://controller:5000/api/v1" in text
        assert text.index("pip install") < text.index("COPY . .")

    def test_the_worker_points_at_the_controller_by_service_name(self):
        assert "CONTROLLER_URL=http://controller:5000" in read("Dockerfile.worker")

    def test_requirements_are_pinned(self):
        lines = [l.strip() for l in read("requirements.txt").splitlines() if l.strip()]
        assert lines and all(re.fullmatch(r"[A-Za-z0-9_.-]+==\d+(\.\d+)*", l) for l in lines), lines
        assert {"flask", "requests"} <= {l.split("==")[0].lower() for l in lines}
