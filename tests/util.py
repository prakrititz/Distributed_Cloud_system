"""Small helpers shared by the integration tests."""
import requests

from helpers.checksum import compute

TIMEOUT = 60
MB = 1024 * 1024


def upload(cluster, name, data, content_type="application/octet-stream", **form):
    """POST /objects like curl would: multipart with 'file' plus any form fields."""
    return requests.post(cluster.api + "/objects", files={"file": (name, data, content_type)},
                         data=form, timeout=TIMEOUT)


def upload_ok(cluster, name, data, **kwargs) -> dict:
    r = upload(cluster, name, data, **kwargs)
    assert r.status_code == 201, r.text
    return r.json()


def download(cluster, object_id):
    return requests.get(f"{cluster.api}/objects/{object_id}", timeout=TIMEOUT)


def metadata(cluster, object_id) -> dict:
    r = requests.get(f"{cluster.api}/objects/{object_id}/metadata", timeout=TIMEOUT)
    assert r.status_code == 200, r.text
    return r.json()


def listing(cluster, **params) -> list:
    r = requests.get(cluster.api + "/objects", params=params, timeout=TIMEOUT)
    assert r.status_code == 200, r.text
    return r.json()


def replicas_of(cluster, object_id) -> list:
    """[(chunk_id, [node ids]), ...] in order."""
    return [(c["chunk_id"], c["replicas"]) for c in metadata(cluster, object_id)["chunks"]]


def expected_chunk_files(cluster) -> int:
    """How many chunk files the nodes should hold in total: 2 per chunk of every object."""
    return 2 * sum(metadata(cluster, o["object_id"])["chunk_count"] for o in listing(cluster))


def post_heartbeat(cluster, body):
    return requests.post(cluster.root + "/internal/workers/heartbeat", json=body, timeout=TIMEOUT)


__all__ = ["compute", "MB", "upload", "upload_ok", "download", "metadata", "listing", "replicas_of",
           "expected_chunk_files", "post_heartbeat"]
