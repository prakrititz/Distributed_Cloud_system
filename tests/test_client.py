"""client.py: the command-line client, driven against the real cluster."""
import builtins
import os

import pytest

import client as client_module
from client import Client, parse_pairs


class TestParsePairs:
    def test_pairs(self):
        assert parse_pairs("a=1, b = two ,c=null") == {"a": "1", "b": "two", "c": None}

    def test_empty_and_blank_pieces(self):
        assert parse_pairs("") == {} and parse_pairs(" , ,") == {}

    def test_a_value_may_contain_an_equals_sign(self):
        assert parse_pairs("url=http://x/?a=b") == {"url": "http://x/?a=b"}

    def test_errors(self):
        with pytest.raises(ValueError, match="not key=value"):
            parse_pairs("novalue")
        with pytest.raises(ValueError, match="no key"):
            parse_pairs("=value")


class TestClientConfiguration:
    def test_the_api_address_can_come_from_the_environment(self, monkeypatch):
        import importlib
        monkeypatch.setenv("API_URL", "http://controller:5000/api/v1")
        try:
            reloaded = importlib.reload(client_module)
            assert reloaded.BASE_URL == "http://controller:5000/api/v1"
            assert reloaded.Client().base_url == "http://controller:5000/api/v1"
        finally:
            monkeypatch.delenv("API_URL")
            importlib.reload(client_module)

    def test_the_default_address_is_the_local_controller(self, monkeypatch):
        import importlib
        monkeypatch.delenv("API_URL", raising=False)
        reloaded = importlib.reload(client_module)
        assert reloaded.BASE_URL == "http://127.0.0.1:5000/api/v1"


class TestClientAgainstTheCluster:
    def test_upload_download_round_trip_with_metadata(self, cluster, tmp_path):
        c = Client(base_url=cluster.api, client_id="t")
        src = tmp_path / "todo.txt"
        src.write_bytes(b"buy milk\r\nwrite report\r\n")                 # CRLF must survive
        m = c.upload(str(src), {"project": "demo"})
        assert m["custom"] == {"project": "demo"} and m["content_type"] == "text/plain" and m["size"] == src.stat().st_size
        out = tmp_path / "out"
        out.mkdir()
        os.chdir(out)
        saved = c.retrieve(m["object_id"])
        assert saved == "todo.txt" and (out / "todo.txt").read_bytes() == src.read_bytes()

    def test_download_never_overwrites_an_existing_file(self, cluster, tmp_path, monkeypatch):
        c = Client(base_url=cluster.api, client_id="t")
        src = tmp_path / "f.txt"
        src.write_bytes(b"x")
        m = c.upload(str(src))
        monkeypatch.chdir(tmp_path)
        (tmp_path / "f.txt").write_bytes(b"precious")
        with pytest.raises(ValueError, match="already exists"):
            c.retrieve(m["object_id"])
        assert (tmp_path / "f.txt").read_bytes() == b"precious"

    def test_errors_are_readable(self, cluster, tmp_path):
        c = Client(base_url=cluster.api, client_id="t")
        with pytest.raises(ValueError, match="404"):
            c.retrieve("no-such-id", str(tmp_path / "x"))
        with pytest.raises(ValueError, match="404"):
            c.get_metadata("no-such-id")
        with pytest.raises(ValueError, match="404"):
            c.delete("no-such-id")
        with pytest.raises(FileNotFoundError):
            c.upload(str(tmp_path / "does-not-exist"))

    def test_a_download_damaged_in_transit_is_detected(self, cluster, tmp_path, monkeypatch):
        c = Client(base_url=cluster.api, client_id="t")
        src = tmp_path / "f.bin"
        src.write_bytes(os.urandom(2000))
        m = c.upload(str(src))
        import requests
        real_get = requests.get

        class Damaged:
            def __init__(self, r):
                self._r = r
                self.headers = r.headers
                self.ok = r.ok
                self.status_code = r.status_code
                self.content = bytes([r.content[0] ^ 0xFF]) + r.content[1:]

        monkeypatch.setattr(client_module.requests, "get",
                            lambda url, *a, **k: Damaged(real_get(url, *a, **k)) if url.endswith(m["object_id"]) else real_get(url, *a, **k))
        with pytest.raises(ValueError, match="damaged in transit"):
            c.retrieve(m["object_id"], str(tmp_path / "out.bin"))
        assert not (tmp_path / "out.bin").exists()


class TestMenu:
    def test_a_scripted_session(self, cluster, tmp_path, monkeypatch, capsys):
        src = tmp_path / "src" / "todo.txt"
        src.parent.mkdir()
        src.write_bytes(b"buy milk\r\nwrite report\r\n")
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(client_module, "Client", lambda: Client(base_url=cluster.api, client_id="menu"))
        probe = Client(base_url=cluster.api)

        def latest():
            return probe.list_objects()[-1]["object_id"]

        script = [
            ("Choose", "1"), ("File path", str(src)), ("Custom metadata", "project=demo, owner=ansh"),
            ("Choose", "3"),
            ("Choose", "4"), ("Object ID", latest),
            ("Choose", "5"), ("Object ID", latest), ("Changes", "owner=null, stage=2"),
            ("Choose", "2"), ("Object ID", latest), ("Save as", ""),
            ("Choose", "2"), ("Object ID", latest), ("Save as", ""),          # the original name exists now
            ("Choose", "1"), ("File path", str(src)), ("Custom metadata", ""),   # the same path again
            ("Choose", "2"), ("Object ID", lambda: "no-such-id"), ("Save as", "x.txt"),
            ("Choose", "6"), ("Object ID", latest),
            ("Choose", "3"),
            ("Choose", "9"),
            ("Choose", "7"),
        ]
        it = iter(script)

        def fake_input(prompt=""):
            key, value = next(it)
            assert key in prompt, f"expected a '{key}' prompt, got '{prompt}'"
            return value() if callable(value) else value

        monkeypatch.setattr(builtins, "input", fake_input)
        client_module.main()
        out = capsys.readouterr().out
        assert "Uploaded 'todo.txt'" in out and "Object ID:" in out
        assert '"project": "demo"' in out and '"stage": "2"' in out and "owner" not in out.split("Saved to")[0].split('"stage"')[-1]
        assert "Saved to todo.txt" in out and "already exists" in out
        assert "Server said 409" in out and "Server said 404" in out
        assert "Deleted." in out and "Nothing stored yet." in out and "Please enter 1-7." in out
        assert (tmp_path / "todo.txt").read_bytes() == src.read_bytes()
        assert next(it, None) is None                                     # the whole script was used
