from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path


def _load_cos_upload():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "cos_upload.py"
    spec = importlib.util.spec_from_file_location("cos_upload", module_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_upload_file_keeps_cos_object_private_by_default(monkeypatch, tmp_path):
    cos_upload = _load_cos_upload()
    archive = tmp_path / "data-cn-2026-06-26.tar.gz"
    archive.write_bytes(b"archive")
    calls = []

    def fake_cos_request(*args, **kwargs):
        calls.append({"args": args, "kwargs": kwargs})
        return 200, b""

    monkeypatch.setattr(cos_upload, "_upload_file_with_sdk", lambda *args: None)
    monkeypatch.setattr(cos_upload, "_cos_request", fake_cos_request)

    assert cos_upload.upload_file("bucket", "region", "key.tar.gz", archive, "sid", "skey") is True
    assert calls[0]["kwargs"]["public_read"] is False


def test_upload_file_uses_sdk_without_public_acl(monkeypatch, tmp_path):
    cos_upload = _load_cos_upload()
    archive = tmp_path / "data-cn-2026-06-26.tar.gz"
    archive.write_bytes(b"archive")
    calls = []

    class FakeClient:
        def __init__(self, config):
            self.config = config

        def upload_file(self, **kwargs):
            calls.append(kwargs)

    fake_module = types.SimpleNamespace(
        CosConfig=lambda **kwargs: kwargs,
        CosS3Client=FakeClient,
    )
    monkeypatch.setitem(sys.modules, "qcloud_cos", fake_module)

    assert cos_upload.upload_file("bucket", "region", "key.tar.gz", archive, "sid", "skey") is True
    assert calls[0]["Bucket"] == "bucket"
    assert calls[0]["Key"] == "key.tar.gz"
    assert "ACL" not in calls[0]
    assert "x-cos-acl" not in calls[0]


def test_releases_json_keeps_cos_object_private_by_default(monkeypatch):
    cos_upload = _load_cos_upload()
    calls = []

    def fake_cos_request(*args, **kwargs):
        calls.append({"args": args, "kwargs": kwargs})
        return 200, b""

    monkeypatch.setattr(cos_upload, "_cos_request", fake_cos_request)

    assert cos_upload._put_releases_json("bucket", "region", [], "sid", "skey") is True
    assert calls[0]["kwargs"]["public_read"] is False


def test_release_data_prefers_sdk_capable_python_for_cos_upload():
    release_script = Path(__file__).resolve().parents[1] / "scripts" / "release_data.sh"
    source = release_script.read_text()

    assert "COS_UPLOAD_PYTHON" in source
    assert "sys.version_info >= (3, 10)" in source
    assert 'python3 -c "import sys; import qcloud_cos;' in source
    assert 'python3 "$SCRIPT_DIR/cos_upload.py"' in source
