import json
from pathlib import Path

import duckdb

from scripts.check_integrity import check_integrity


def create_db(path, include_active_valuation=True, include_security_type=True):
    conn = duckdb.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE stocks (
                symbol VARCHAR NOT NULL,
                date DATE NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE valuation (
                symbol VARCHAR NOT NULL,
                date DATE NOT NULL
            )
            """
        )
        security_type_column = ", security_type VARCHAR" if include_security_type else ""
        conn.execute(
            f"""
            CREATE TABLE stock_metadata (
                symbol VARCHAR NOT NULL,
                de_listed_date DATE{security_type_column}
            )
            """
        )
        metadata_rows = [
            ("000001.SZ", "2900-01-01", None),
            ("302132.SZ", "2900-01-01", None),
            ("000003.SZ", "2002-04-26", None),
        ]
        if not include_security_type:
            metadata_rows = [row[:2] for row in metadata_rows]
        metadata_placeholders = ", ".join(["?"] * len(metadata_rows[0]))
        conn.executemany(
            f"INSERT INTO stock_metadata VALUES ({metadata_placeholders})",
            metadata_rows,
        )
        conn.executemany(
            "INSERT INTO stocks VALUES (?, ?)",
            [
                ("000001.SZ", "2026-06-24"),
                ("302132.SZ", "2026-06-24"),
                ("000003.SZ", "2002-04-26"),
            ],
        )
        valuation_rows = [("302132.SZ", "2026-06-24")]
        if include_active_valuation:
            valuation_rows.append(("000001.SZ", "2026-06-24"))
        conn.executemany("INSERT INTO valuation VALUES (?, ?)", valuation_rows)
    finally:
        conn.close()


def test_integrity_passes_when_active_cn_stocks_are_complete(tmp_path):
    db_path = tmp_path / "cn.duckdb"
    create_db(db_path)

    report = check_integrity(str(db_path), target_date="2026-06-24")

    assert report["status"] == "pass"
    assert report["active_symbols"] == 2
    assert report["stocks"]["latest_count"] == 2
    assert report["valuation"]["latest_count"] == 2
    assert report["delisted_missing_valuation"] == 1
    assert report["anomalies"]["non_standard_prefix_symbols"] == []


def test_integrity_handles_legacy_metadata_without_security_type(tmp_path):
    db_path = tmp_path / "cn.duckdb"
    create_db(db_path, include_security_type=False)

    report = check_integrity(str(db_path), target_date="2026-06-24")

    assert report["status"] == "pass"
    assert report["active_symbols"] == 2
    assert report["stocks"]["latest_count"] == 2
    assert report["valuation"]["latest_count"] == 2


def test_integrity_allows_halted_active_stock_to_be_stale(tmp_path):
    db_path = tmp_path / "cn.duckdb"
    create_db(db_path)
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute("DELETE FROM stocks WHERE symbol = '000001.SZ'")
        conn.execute("DELETE FROM valuation WHERE symbol = '000001.SZ'")
        conn.execute("INSERT INTO stocks VALUES ('000001.SZ', '2026-06-23')")
        conn.execute("INSERT INTO valuation VALUES ('000001.SZ', '2026-06-23')")
        conn.execute(
            """
            CREATE TABLE stock_status (
                date VARCHAR NOT NULL,
                status_type VARCHAR NOT NULL,
                symbols VARCHAR NOT NULL
            )
            """
        )
        conn.execute(
            "INSERT INTO stock_status VALUES (?, ?, ?)",
            ["20260624", "HALT", json.dumps(["000001.SZ"])],
        )
    finally:
        conn.close()

    report = check_integrity(str(db_path), target_date="2026-06-24")

    assert report["status"] == "pass"
    assert report["stocks"]["latest_count"] == 2
    assert report["valuation"]["latest_count"] == 2


def test_integrity_excludes_delisted_name_with_placeholder_delisted_date(tmp_path):
    db_path = tmp_path / "cn.duckdb"
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute("CREATE TABLE stocks (symbol VARCHAR NOT NULL, date DATE NOT NULL)")
        conn.execute(
            "CREATE TABLE valuation (symbol VARCHAR NOT NULL, date DATE NOT NULL)"
        )
        conn.execute(
            """
            CREATE TABLE stock_metadata (
                symbol VARCHAR NOT NULL,
                stock_name VARCHAR,
                de_listed_date DATE,
                security_type VARCHAR
            )
            """
        )
        conn.executemany(
            "INSERT INTO stock_metadata VALUES (?, ?, ?, ?)",
            [
                ("000001.SZ", "平安银行", "2900-01-01", "1"),
                ("688287.SS", "退市观典", "2900-01-01", "1"),
            ],
        )
        conn.execute("INSERT INTO stocks VALUES ('000001.SZ', '2026-06-24')")
        conn.execute("INSERT INTO valuation VALUES ('000001.SZ', '2026-06-24')")
    finally:
        conn.close()

    report = check_integrity(str(db_path), target_date="2026-06-24")

    assert report["status"] == "pass"
    assert report["active_symbols"] == 1


def test_integrity_excludes_metadata_not_seen_in_latest_stock_pool(tmp_path):
    db_path = tmp_path / "cn.duckdb"
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute("CREATE TABLE stocks (symbol VARCHAR NOT NULL, date DATE NOT NULL)")
        conn.execute(
            "CREATE TABLE valuation (symbol VARCHAR NOT NULL, date DATE NOT NULL)"
        )
        conn.execute(
            """
            CREATE TABLE stock_metadata (
                symbol VARCHAR NOT NULL,
                de_listed_date DATE,
                security_type VARCHAR
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE stock_pool (
                symbol VARCHAR NOT NULL,
                first_seen_date DATE,
                last_seen_date DATE
            )
            """
        )
        conn.executemany(
            "INSERT INTO stock_metadata VALUES (?, ?, ?)",
            [
                ("000001.SZ", "2900-01-01", "1"),
                ("000638.SZ", "2900-01-01", "1"),
            ],
        )
        conn.executemany(
            "INSERT INTO stock_pool VALUES (?, ?, ?)",
            [
                ("000001.SZ", "2026-01-01", "2026-06-24"),
                ("000638.SZ", "2026-01-01", "2026-06-01"),
            ],
        )
        conn.execute("INSERT INTO stocks VALUES ('000001.SZ', '2026-06-24')")
        conn.execute("INSERT INTO valuation VALUES ('000001.SZ', '2026-06-24')")
    finally:
        conn.close()

    report = check_integrity(str(db_path), target_date="2026-06-24")

    assert report["status"] == "pass"
    assert report["active_symbols"] == 1


def test_integrity_ignores_stale_stock_pool_when_metadata_is_current(tmp_path):
    db_path = tmp_path / "cn.duckdb"
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute("CREATE TABLE stocks (symbol VARCHAR NOT NULL, date DATE NOT NULL)")
        conn.execute(
            "CREATE TABLE valuation (symbol VARCHAR NOT NULL, date DATE NOT NULL)"
        )
        conn.execute(
            """
            CREATE TABLE stock_metadata (
                symbol VARCHAR NOT NULL,
                de_listed_date DATE,
                security_type VARCHAR
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE stock_pool (
                symbol VARCHAR NOT NULL,
                first_seen_date DATE,
                last_seen_date DATE
            )
            """
        )
        conn.executemany(
            "INSERT INTO stock_metadata VALUES (?, ?, ?)",
            [
                ("000001.SZ", "2900-01-01", "1"),
                ("000002.SZ", "2900-01-01", "1"),
            ],
        )
        conn.execute(
            "INSERT INTO stock_pool VALUES ('000001.SZ', '2020-01-01', '2020-04-01')"
        )
        conn.executemany(
            "INSERT INTO stocks VALUES (?, ?)",
            [
                ("000001.SZ", "2026-06-24"),
                ("000002.SZ", "2026-06-24"),
            ],
        )
        conn.executemany(
            "INSERT INTO valuation VALUES (?, ?)",
            [
                ("000001.SZ", "2026-06-24"),
                ("000002.SZ", "2026-06-24"),
            ],
        )
    finally:
        conn.close()

    report = check_integrity(str(db_path), target_date="2026-06-24")

    assert report["status"] == "pass"
    assert report["active_symbols"] == 2


def test_integrity_fails_when_active_cn_valuation_is_missing(tmp_path):
    db_path = tmp_path / "cn.duckdb"
    create_db(db_path, include_active_valuation=False)

    report = check_integrity(str(db_path), target_date="2026-06-24")

    assert report["status"] == "fail"
    valuation_check = next(
        check for check in report["checks"]
        if check["name"] == "active_valuation_latest"
    )
    assert valuation_check["missing"] == ["000001.SZ"]


def test_integrity_treats_quarterly_fundamentals_as_coverage(tmp_path):
    db_path = tmp_path / "cn.duckdb"
    create_db(db_path)
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE fundamentals (
                symbol VARCHAR NOT NULL,
                date DATE NOT NULL
            )
            """
        )
        conn.executemany(
            "INSERT INTO fundamentals VALUES (?, ?)",
            [
                ("000001.SZ", "2026-03-31"),
                ("302132.SZ", "2026-03-31"),
            ],
        )
    finally:
        conn.close()

    report = check_integrity(str(db_path), target_date="2026-06-24")

    fundamentals_check = next(
        check for check in report["checks"]
        if check["name"] == "active_fundamentals_coverage"
    )
    assert fundamentals_check["status"] == "pass"
    assert fundamentals_check["actual"] == 2
    assert report["fundamentals"]["stale"] == []


def test_integrity_fails_when_export_is_missing_active_valuation_file(tmp_path):
    db_path = tmp_path / "cn.duckdb"
    create_db(db_path)
    export_dir = tmp_path / "export" / "cn"
    (export_dir / "stocks").mkdir(parents=True)
    (export_dir / "valuation").mkdir()
    (export_dir / "metadata").mkdir()
    for symbol in ["000001.SZ", "302132.SZ"]:
        (export_dir / "stocks" / f"{symbol}.parquet").write_bytes(b"")
    (export_dir / "valuation" / "302132.SZ.parquet").write_bytes(b"")
    (export_dir / "metadata" / "stock_metadata.parquet").write_bytes(b"")
    (export_dir / "manifest.json").write_text(
        json.dumps(
            {
                "version": "2026-06-24",
                "market": "CN",
                "date_range": {"start": "2026-06-24", "end": "2026-06-24"},
            }
        ),
        encoding="utf-8",
    )

    report = check_integrity(
        str(db_path),
        target_date="2026-06-24",
        export_dir=str(export_dir),
    )

    assert report["status"] == "fail"
    export_check = next(
        check for check in report["checks"]
        if check["name"] == "active_valuation_files_present"
    )
    assert export_check["missing"] == ["000001.SZ"]


def test_daily_integrity_gates_use_market_duckdb_path():
    script = Path("scripts/run_daily.sh").read_text(encoding="utf-8")

    assert 'DUCKDB_FILE="$SIMTRADE_DATA_DIR/data/${MARKET}.duckdb"' in script
    assert script.count('--db-path "$DUCKDB_FILE"') == 2


def test_daily_pipeline_publishes_when_local_release_lags_db_version():
    script = Path("scripts/run_daily.sh").read_text(encoding="utf-8")

    assert "TRACK_LOCAL_RELEASE_VERSION" in script
    assert "get_released_version" in script
    assert 'OLD_RELEASE_VERSION=$(get_released_version "$MARKET")' in script
    assert '"$OLD_RELEASE_VERSION" != "$NEW_VERSION"' in script
    assert "continuing to integrity gate and release" in script
