#!/usr/bin/env bash
# Export data from DuckDB and release locally, to GitHub, or to Tencent COS.
# Usage: bash scripts/release_data.sh [options]
#
# This script:
# 1. Runs export_parquet.py → data/export/{market}/
# 2. Packages into a single tar.gz
# 3. Publishes locally, to GitHub Release, and/or Tencent COS
#
# Prerequisites:
#   Local:   no external dependencies
#   GitHub:  poetry install, gh auth login
#   COS:     COS_SECRET_ID / COS_SECRET_KEY env vars set

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ── Defaults ────────────────────────────────────────────────────────
MARKET="cn"
PUBLISH_TARGETS="github"          # local | github | cos | all
COS_BUCKET="${COS_BUCKET:-}"
COS_REGION="${COS_REGION:-}"
COS_KEY_PREFIX="${COS_KEY_PREFIX:-}"
LOCAL_RELEASE_DIR="${LOCAL_RELEASE_DIR:-$PROJECT_ROOT/data/releases}"

# ── Parse arguments ─────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    --market)          MARKET="$2"; shift 2 ;;
    --publish-targets) PUBLISH_TARGETS="$2"; shift 2 ;;
    --cos-bucket)      COS_BUCKET="$2"; shift 2 ;;
    --cos-region)      COS_REGION="$2"; shift 2 ;;
    --cos-key-prefix)  COS_KEY_PREFIX="$2"; shift 2 ;;
    --local-release-dir) LOCAL_RELEASE_DIR="$2"; shift 2 ;;
    *) echo "Unknown argument: $1"; exit 1 ;;
  esac
done

MARKET=$(echo "$MARKET" | tr '[:upper:]' '[:lower:]')
if [[ "$MARKET" != "cn" && "$MARKET" != "us" && "$MARKET" != "all" ]]; then
  echo "ERROR: --market must be cn, us, or all"
  exit 1
fi

if [[ "$PUBLISH_TARGETS" != "local" && "$PUBLISH_TARGETS" != "github" && "$PUBLISH_TARGETS" != "cos" && "$PUBLISH_TARGETS" != "all" ]]; then
  echo "ERROR: --publish-targets must be local, github, cos, or all"
  exit 1
fi

# ── Publish single market ───────────────────────────────────────────
release_market() {
  local market="$1"
  local export_dir="$PROJECT_ROOT/data/export/$market"

  # 1. Export
  echo "=== Exporting $market data ==="
  cd "$PROJECT_ROOT"
  poetry run python scripts/export_parquet.py --market "$market"

  # Data manifest produced by export_parquet.py
  local data_manifest="$export_dir/manifest.json"
  if [ ! -f "$data_manifest" ]; then
    echo "ERROR: Export did not produce manifest.json"
    return 1
  fi

  local version
  version=$(python3 -c "import json; print(json.load(open('$data_manifest'))['version'])")
  local tag="data-${market}-${version}"
  local archive_name="${tag}.tar.gz"
  local archive_dir
  archive_dir=$(mktemp -d "/tmp/${tag}.XXXXXX")
  local archive="$archive_dir/$archive_name"
  local local_archive="$LOCAL_RELEASE_DIR/$archive_name"

  # 2. Package
  echo ""
  echo "=== Packaging ${market} ${version} ==="
  tar -czf "$archive" -C "$export_dir" .

  local size
  size=$(du -h "$archive" | cut -f1)
  echo "  -> $archive ($size)"

  local local_ok=true
  local github_ok=true
  local cos_ok=true

  # 3a. Local artifact / authorized index mirror
  if [[ "$PUBLISH_TARGETS" == "local" || "$PUBLISH_TARGETS" == "cos" || "$PUBLISH_TARGETS" == "all" ]]; then
    echo ""
    echo "=== Publishing locally ==="
    mkdir -p "$LOCAL_RELEASE_DIR"
    cp "$archive" "$local_archive" || local_ok=false
    if $local_ok; then
      echo "  -> $local_archive"
    else
      echo "  ERROR: local publish failed"
    fi
  fi

  # 3b. GitHub Release
  if [[ "$PUBLISH_TARGETS" == "github" || "$PUBLISH_TARGETS" == "all" ]]; then
    echo ""
    echo "=== Publishing to GitHub ==="
    if gh release view "$tag" >/dev/null 2>&1; then
      echo "  Release $tag exists, updating..."
      gh release upload "$tag" "$archive" --clobber || github_ok=false
    else
      gh release create "$tag" \
        --title "SimTradeData ${market} ${version}" \
        --notes "Data date: ${version} (${market})" \
        "$archive" || github_ok=false
    fi
    if $github_ok; then
      echo "  -> $(gh release view "$tag" --json url -q .url 2>/dev/null || echo "uploaded")"
    else
      echo "  ERROR: GitHub release failed"
    fi
  fi

  # 3c. Tencent COS
  if [[ "$PUBLISH_TARGETS" == "cos" || "$PUBLISH_TARGETS" == "all" ]]; then
    echo ""
    echo "=== Publishing to COS ==="
    if [[ -z "$COS_BUCKET" ]]; then
      echo "  ERROR: --cos-bucket or COS_BUCKET env var required for COS publish"
      cos_ok=false
    elif [[ -z "$COS_REGION" ]]; then
      echo "  ERROR: --cos-region or COS_REGION env var required for COS publish"
      cos_ok=false
    else
      if [[ -n "${COS_UPLOAD_PYTHON:-}" ]]; then
        "$COS_UPLOAD_PYTHON" "$SCRIPT_DIR/cos_upload.py" \
          --file "$archive" \
          --data-manifest "$data_manifest" \
          --bucket "$COS_BUCKET" \
          --region "$COS_REGION" \
          --key-prefix "$COS_KEY_PREFIX" || cos_ok=false
      elif command -v python3 >/dev/null 2>&1 && python3 -c "import sys; import qcloud_cos; raise SystemExit(0 if sys.version_info >= (3, 10) else 1)" >/dev/null 2>&1; then
        python3 "$SCRIPT_DIR/cos_upload.py" \
          --file "$archive" \
          --data-manifest "$data_manifest" \
          --bucket "$COS_BUCKET" \
          --region "$COS_REGION" \
          --key-prefix "$COS_KEY_PREFIX" || cos_ok=false
      else
        poetry run python "$SCRIPT_DIR/cos_upload.py" \
          --file "$archive" \
          --data-manifest "$data_manifest" \
          --bucket "$COS_BUCKET" \
          --region "$COS_REGION" \
          --key-prefix "$COS_KEY_PREFIX" || cos_ok=false
      fi
    fi
  fi

  # 4. Cleanup
  rm -rf "$archive_dir"

  # 5. Summary
  echo ""
  echo "=== Release Summary for $market ==="
  if [[ "$PUBLISH_TARGETS" == "local" ]]; then
    echo "  Local:  $($local_ok && echo 'OK' || echo 'FAILED')"
  fi
  if [[ "$PUBLISH_TARGETS" == "github" || "$PUBLISH_TARGETS" == "all" ]]; then
    echo "  GitHub: $($github_ok && echo 'OK' || echo 'FAILED')"
  fi
  if [[ "$PUBLISH_TARGETS" == "cos" || "$PUBLISH_TARGETS" == "all" ]]; then
    echo "  COS:    $($cos_ok && echo 'OK' || echo 'FAILED')"
  fi

  # Fail if any requested target failed
  if ! $local_ok || ! $github_ok || ! $cos_ok; then
    return 1
  fi
  echo ""
}

# ── Main ─────────────────────────────────────────────────────────────
if [ "$MARKET" = "all" ]; then
  release_market "cn"
  release_market "us"
else
  release_market "$MARKET"
fi
