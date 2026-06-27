from __future__ import annotations

import importlib.util
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

    monkeypatch.setattr(cos_upload, "_cos_request", fake_cos_request)

    assert cos_upload.upload_file("bucket", "region", "key.tar.gz", archive, "sid", "skey") is True
    assert calls[0]["kwargs"]["public_read"] is False


def test_releases_json_keeps_cos_object_private_by_default(monkeypatch):
    cos_upload = _load_cos_upload()
    calls = []

    def fake_cos_request(*args, **kwargs):
        calls.append({"args": args, "kwargs": kwargs})
        return 200, b""

    monkeypatch.setattr(cos_upload, "_cos_request", fake_cos_request)

    assert cos_upload._put_releases_json("bucket", "region", [], "sid", "skey") is True
    assert calls[0]["kwargs"]["public_read"] is False
