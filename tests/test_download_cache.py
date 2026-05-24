"""Regression tests for content-fingerprint based TDX download/import caching."""
import hashlib
import json
import sys
from pathlib import Path
from urllib.error import HTTPError

import duckdb
import pytest

# download.py imports its siblings as top-level modules (it prepends scripts/
# to sys.path). Patch that same module object, not the scripts.* alias.
_SCRIPTS_DIR = str(Path(__file__).resolve().parents[1] / "scripts")
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

import download_tdx_day as tdx  # noqa: E402
import import_tdx_day  # noqa: E402
from scripts import download as download_entry  # noqa: E402

URL = "https://example.test/hsjday.zip"
ETAG = '"etag-v1"'
LAST_MODIFIED = "Thu, 26 Nov 2026 07:58:57 GMT"
SIZE = 5


class _HeadResponse:
    def __init__(self, headers):
        self.headers = headers

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class _BodyResponse(_HeadResponse):
    def __init__(self, headers, chunks):
        super().__init__(headers)
        self._chunks = list(chunks)

    def read(self, _size):
        return self._chunks.pop(0) if self._chunks else b""


def _install_urlopen(monkeypatch, *, not_modified_etag=None, chunks=None, headers=None):
    seen = []

    def fake(req, timeout=None):
        seen.append(req)
        sent = {k.lower(): v for k, v in req.header_items()}
        if not_modified_etag and sent.get("if-none-match") == not_modified_etag:
            raise HTTPError(req.full_url, 304, "Not Modified", None, None)
        default_headers = {
            "Content-Length": str(SIZE),
            "Last-Modified": LAST_MODIFIED,
            "ETag": ETAG,
        }
        response_headers = default_headers if headers is None else headers
        if chunks is not None:
            return _BodyResponse(response_headers, chunks)
        return _HeadResponse(response_headers)

    monkeypatch.setattr(tdx, "urlopen", fake)
    return seen


def _write_meta(dest, **overrides):
    meta = {
        "etag": ETAG,
        "last_modified": LAST_MODIFIED,
        "size": SIZE,
        "sha256": "abc123",
        **overrides,
    }
    tdx.write_package_meta(dest, meta)
    return meta


def test_get_remote_file_info_uses_conditional_head_and_handles_304(monkeypatch):
    seen = _install_urlopen(monkeypatch, not_modified_etag=ETAG)

    info = tdx.get_remote_file_info(URL, local_meta={"etag": ETAG, "size": SIZE,
                                                     "last_modified": LAST_MODIFIED})

    assert info["unchanged"] is True
    assert info["etag"] == ETAG
    assert {k.lower(): v for k, v in seen[0].header_items()}["if-none-match"] == ETAG


def test_get_remote_file_info_without_meta_returns_fingerprint(monkeypatch):
    seen = _install_urlopen(monkeypatch)

    info = tdx.get_remote_file_info(URL)

    assert info == {
        "size": SIZE,
        "last_modified": LAST_MODIFIED,
        "etag": ETAG,
        "unchanged": False,
    }
    assert "if-none-match" not in {k.lower() for k, _ in seen[0].header_items()}


@pytest.mark.parametrize(
    ("setup", "remote", "expected"),
    [
        ("missing", {"size": SIZE, "etag": ETAG}, True),
        ("cached", {"size": SIZE, "etag": ETAG, "unchanged": True}, False),
        ("cached", {"size": SIZE + 1, "etag": ETAG}, True),
        ("cached", {"size": SIZE, "etag": ETAG}, False),
        ("cached", {"size": SIZE, "etag": '"etag-v2"'}, True),
        ("legacy", {"size": SIZE, "etag": ETAG}, False),
    ],
)
def test_needs_update_matrix(tmp_path, setup, remote, expected):
    dest = tmp_path / "hsjday.zip"
    if setup != "missing":
        dest.write_bytes(b"abcde")
        if setup == "cached":
            _write_meta(dest)

    assert tdx.needs_update(dest, remote) is expected


def test_needs_update_falls_back_to_last_modified(tmp_path):
    dest = tmp_path / "hsjday.zip"
    dest.write_bytes(b"abcde")
    _write_meta(dest, etag=None)

    assert tdx.needs_update(dest, {"size": SIZE, "last_modified": LAST_MODIFIED}) is False
    assert tdx.needs_update(dest, {"size": SIZE, "last_modified": "Wed, 25 Nov 2026 00:00:00 GMT"}) is True


def test_download_file_records_sha256_while_streaming(tmp_path, monkeypatch):
    _install_urlopen(monkeypatch, chunks=[b"ab", b"cde", b""])
    dest = tmp_path / "hsjday.zip"

    assert tdx.download_file(URL, dest, show_progress=False) is True

    meta = json.loads((tmp_path / "hsjday.zip.meta.json").read_text(encoding="utf-8"))
    assert meta["sha256"] == hashlib.sha256(b"abcde").hexdigest()
    assert meta["etag"] == ETAG
    assert meta["last_modified"] == LAST_MODIFIED
    assert meta["size"] == SIZE
    assert dest.read_bytes() == b"abcde"


def _make_db(path, rows):
    con = duckdb.connect(str(path))
    try:
        con.execute("CREATE TABLE stocks (symbol VARCHAR, date DATE)")
        for i in range(rows):
            con.execute("INSERT INTO stocks VALUES (?, ?)", [f"{i:06d}.SZ", "2026-11-26"])
    finally:
        con.close()


def test_import_ledger_only_skips_when_package_and_db_match(tmp_path):
    db = tmp_path / "cn.duckdb"
    _make_db(db, 10)
    dest = tmp_path / "hsjday.zip"
    meta = _write_meta(dest, sha256="pkg-hash")
    meta["imported"] = {
        "sha256": "pkg-hash",
        "stocks_rows": 10,
        "stocks_max": "2026-11-26",
    }

    assert download_entry._tdx_import_is_current(dest, str(db), meta) is True

    mismatch = dict(meta, sha256="other-hash")
    assert download_entry._tdx_import_is_current(dest, str(db), mismatch) is False

    without_ledger = {k: v for k, v in meta.items() if k != "imported"}
    assert download_entry._tdx_import_is_current(dest, str(db), without_ledger) is False

    _make_db(tmp_path / "rewritten.duckdb", 2)
    assert download_entry._tdx_import_is_current(
        dest, str(tmp_path / "rewritten.duckdb"), meta
    ) is False


def test_run_tdx_phase_skips_import_when_package_is_unchanged(tmp_path, monkeypatch):
    """No TDX importer (and no 500MB re-parse) when bytes and DB are unchanged."""
    db = tmp_path / "cn.duckdb"
    _make_db(db, 10)
    dest = tmp_path / "downloads" / "hsjday.zip"
    dest.parent.mkdir(parents=True)
    dest.write_bytes(b"abcde")
    meta = _write_meta(dest, sha256="pkg-hash", size=5)
    meta["imported"] = {
        "sha256": "pkg-hash",
        "stocks_rows": 10,
        "stocks_max": "2026-11-26",
    }
    tdx.write_package_meta(dest, meta)

    monkeypatch.setattr(tdx, "DOWNLOAD_DIR", dest.parent)
    monkeypatch.setattr(tdx, "DOWNLOAD_URL", URL)
    # Conditional HEAD returns 304 -> needs_update() is False.
    _install_urlopen(monkeypatch, not_modified_etag=ETAG)

    calls = []

    class _MustNotImport:
        def __init__(self, *args, **kwargs):
            calls.append("importer")

        def close(self):
            pass

    monkeypatch.setattr(import_tdx_day, "TdxDayImporter", _MustNotImport)

    assert download_entry.run_tdx_download_and_import(str(db)) is True
    assert calls == []
