# -*- coding: utf-8 -*-
"""
Download and import TDX (通达信) daily data automatically.

Data source: https://www.tdx.com.cn/article/vipdata.html
Downloads the complete daily data package (hsjday.zip, ~500MB) and imports to DuckDB.

Features:
1. Auto-download from official TDX server
2. Incremental import (only new data)
3. Optional full reimport

Usage:
    # Download and import (incremental)
    uv run python scripts/download_tdx_day.py

    # Full reimport
    uv run python scripts/download_tdx_day.py --full

    # Download only (no import)
    uv run python scripts/download_tdx_day.py --download-only

    # Import from existing file
    uv run python scripts/download_tdx_day.py --file hsjday.zip
"""

import argparse
import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from tqdm import tqdm

from scripts.import_tdx_day import TdxDayImporter
from simtradedata.writers.duckdb_writer import DEFAULT_DB_PATH

# Configuration
DOWNLOAD_URL = "https://data.tdx.com.cn/vipdoc/hsjday.zip"
DOWNLOAD_DIR = Path("data/downloads")
LOG_FILE = "data/download_tdx_day.log"

# Ensure directories exist
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
Path(LOG_FILE).parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="w",
)
logger = logging.getLogger(__name__)


META_SUFFIX = ".meta.json"


def _meta_path(dest_path: Path) -> Path:
    return dest_path.with_name(dest_path.name + META_SUFFIX)


def read_package_meta(dest_path: Path) -> dict:
    """Load the content fingerprint recorded after the last successful download."""
    path = _meta_path(dest_path)
    if not path.is_file():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def write_package_meta(dest_path: Path, meta: dict) -> None:
    """Persist the fingerprint atomically so a crashed download leaves no stale meta."""
    path = _meta_path(dest_path)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def get_remote_file_info(url: str, local_meta: dict | None = None) -> dict:
    """HEAD the package with conditional headers when a local fingerprint exists.

    A 304 response means the server confirmed the cached bytes are unchanged,
    so the 500MB transfer can be skipped entirely. Returns an ``unchanged``
    flag alongside size / last_modified / etag.
    """
    req = Request(url, method="HEAD")
    req.add_header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
    meta = local_meta or {}
    if meta.get("etag"):
        req.add_header("If-None-Match", meta["etag"])
    elif meta.get("last_modified"):
        req.add_header("If-Modified-Since", meta["last_modified"])
    try:
        with urlopen(req, timeout=30) as response:
            size = response.headers.get("Content-Length")
            return {
                "size": int(size) if size else None,
                "last_modified": response.headers.get("Last-Modified"),
                "etag": response.headers.get("ETag"),
                "unchanged": False,
            }
    except HTTPError as exc:
        if exc.code == 304 and meta:
            return {
                "size": meta.get("size"),
                "last_modified": meta.get("last_modified"),
                "etag": meta.get("etag"),
                "unchanged": True,
            }
        raise
    except Exception as e:
        logger.warning(f"Failed to get remote file info: {e}")
        return {"size": None, "last_modified": None, "etag": None, "unchanged": False}


def download_file(url: str, dest_path: Path, show_progress: bool = True, progress_callback=None) -> bool:
    """
    Download file with progress bar.

    Args:
        url: Download URL
        dest_path: Destination path
        show_progress: Show progress bar

    Returns:
        True if download succeeded
    """
    temp_path = dest_path.with_suffix(".tmp")
    try:
        req = Request(url)
        req.add_header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")

        with urlopen(req, timeout=60) as response:
            total_size = response.headers.get("Content-Length")
            total_size = int(total_size) if total_size else None
            downloaded = 0
            if progress_callback:
                progress_callback(0, total_size)

            # Hash while streaming so the fingerprint costs no extra disk read.
            digest = hashlib.sha256()

            with open(temp_path, "wb") as f:
                if show_progress and total_size:
                    with tqdm(
                        total=total_size,
                        unit="B",
                        unit_scale=True,
                        desc="Downloading",
                        ncols=100,
                    ) as pbar:
                        while True:
                            chunk = response.read(8192)
                            if not chunk:
                                break
                            f.write(chunk)
                            downloaded += len(chunk)
                            digest.update(chunk)
                            if progress_callback:
                                progress_callback(downloaded, total_size)
                            pbar.update(len(chunk))
                else:
                    while True:
                        chunk = response.read(8192)
                        if not chunk:
                            break
                        f.write(chunk)
                        downloaded += len(chunk)
                        digest.update(chunk)
                        if progress_callback:
                            progress_callback(downloaded, total_size)

            # Verify download size matches Content-Length
            downloaded_size = temp_path.stat().st_size
            if total_size and downloaded_size != total_size:
                temp_path.unlink(missing_ok=True)
                logger.error(
                    "Download size mismatch for %s: expected %d, got %d",
                    url, total_size, downloaded_size,
                )
                return False

            # Atomically replace the existing package. Path.rename() fails on
            # Windows with WinError 183 when the destination already exists.
            os.replace(temp_path, dest_path)
            write_package_meta(dest_path, {
                "etag": response.headers.get("ETag"),
                "last_modified": response.headers.get("Last-Modified"),
                "size": downloaded_size,
                "sha256": digest.hexdigest(),
                "downloaded_at": datetime.now().isoformat(timespec="seconds"),
            })
            return True

    except Exception as e:
        logger.error(f"Download failed: {e}")
        print(f"Error: Download failed - {e}")
        temp_path.unlink(missing_ok=True)
        return False


def needs_update(local_path: Path, remote_info: dict) -> bool:
    """Decide whether the cached package differs from the remote one.

    Priority: server-confirmed 304, then size mismatch, then ETag, then
    Last-Modified. Falls back to the legacy size-only check when no local
    fingerprint exists yet (first run after upgrading).
    """
    if not local_path.exists():
        return True
    if remote_info.get("unchanged"):
        return False

    meta = read_package_meta(local_path)
    remote_size = remote_info.get("size")
    if remote_size and local_path.stat().st_size != remote_size:
        return True

    etag = remote_info.get("etag")
    if etag and meta.get("etag"):
        return meta["etag"] != etag
    last_modified = remote_info.get("last_modified")
    if last_modified and meta.get("last_modified"):
        return meta["last_modified"] != last_modified

    # No sidecar fingerprint yet (first run after upgrading): fall back to
    # Last-Modified vs the local file mtime instead of blindly trusting it.
    if last_modified:
        try:
            modified = parsedate_to_datetime(last_modified)
            if modified.tzinfo is None:
                modified = modified.replace(tzinfo=timezone.utc)
            if modified > datetime.fromtimestamp(local_path.stat().st_mtime, timezone.utc):
                return True
        except (TypeError, ValueError, OverflowError):
            return True
    return False


def main():
    parser = argparse.ArgumentParser(
        description="Download and import TDX daily data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Download and import (incremental)
    uv run python scripts/download_tdx_day.py

    # Full reimport
    uv run python scripts/download_tdx_day.py --full

    # Download only
    uv run python scripts/download_tdx_day.py --download-only

    # Use existing file
    uv run python scripts/download_tdx_day.py --file hsjday.zip
        """,
    )
    parser.add_argument(
        "--db",
        type=str,
        default=DEFAULT_DB_PATH,
        help=f"Database path (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Full import (ignore existing data, reimport all)",
    )
    parser.add_argument(
        "--download-only",
        action="store_true",
        help="Download only, do not import",
    )
    parser.add_argument(
        "--file",
        type=str,
        default=None,
        help="Use existing ZIP file instead of downloading",
    )
    parser.add_argument(
        "--force-download",
        action="store_true",
        help="Force download even if local file is up to date",
    )

    args = parser.parse_args()

    print("=" * 60)
    print("TDX Daily Data Download & Import")
    print("=" * 60)
    print(f"Source: {DOWNLOAD_URL}")
    print(f"Database: {args.db}")
    print()

    # Determine ZIP file path
    if args.file:
        zip_path = Path(args.file)
        if not zip_path.exists():
            print(f"Error: File not found: {zip_path}")
            return 1
        print(f"Using existing file: {zip_path}")
    else:
        # Download from TDX
        zip_path = DOWNLOAD_DIR / "hsjday.zip"

        print("Checking remote file...")
        remote_info = get_remote_file_info(DOWNLOAD_URL)

        if remote_info.get("size"):
            print(f"  Remote size: {remote_info['size'] / 1024 / 1024:.1f} MB")
        if remote_info.get("last_modified"):
            print(f"  Last modified: {remote_info['last_modified']}")

        if args.force_download or needs_update(zip_path, remote_info):
            print()
            print("Downloading hsjday.zip...")
            if not download_file(DOWNLOAD_URL, zip_path):
                return 1
            print(f"Downloaded to: {zip_path}")
        else:
            print()
            print("Local file is up to date, skipping download.")
            print("  (Use --force-download to re-download)")

    if args.download_only:
        print()
        print("Download complete (--download-only specified)")
        return 0

    # Import data
    print()
    print("Importing data...")
    print()

    importer = TdxDayImporter(db_path=args.db, full_import=args.full)

    try:
        stats = importer.import_from_source(zip_path)

        print()
        print("=" * 60)
        print("Import Complete")
        print("=" * 60)
        print(f"Files processed: {stats['files_processed']}")
        print(f"Files skipped: {stats['files_skipped']}")
        print(f"Records imported: {stats['records_imported']}")

        if stats["records_backfilled"] > 0:
            print(f"  - Backfilled (historical): {stats['records_backfilled']}")

        if stats["records_skipped"] > 0:
            print(f"Records skipped (up to date): {stats['records_skipped']}")

    finally:
        importer.close()

    return 0


if __name__ == "__main__":
    exit(main())
