# -*- coding: utf-8 -*-
"""
US stock data download program with DuckDB storage

Uses yfinance to fetch US stock data:
- Phase 1: Stock list from NASDAQ trader file
- Phase 2: Batch OHLCV + adjust factors + preclose (yf.download)
- Phase 3: Per-stock fundamentals + valuation
- Phase 4: Per-stock metadata + exrights
- Phase 5: Benchmark + trade_days + index_constituents

Output: DuckDB database (data/us.duckdb)
"""

import argparse
from filelock import FileLock, Timeout
import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from simtradedata.fetchers.yfinance_fetcher import YFinanceFetcher
from simtradedata.utils.paths import US_DUCKDB_PATH
from simtradedata.utils.process_lock import ProcessLock
from simtradedata.writers.duckdb_writer import DuckDBWriter

# Configuration
OUTPUT_DIR = "data"
LOG_FILE = "data/download_us.log"
LOCK_FILE = "data/.download_us.lock"

# Date range
START_DATE = "2015-01-01"
END_DATE = None  # None means current date

# Batch sizes
OHLCV_BATCH_SIZE = 50  # yf.download batch size
COMMIT_BATCH_SIZE = 20  # DB commit batch size

# Ensure log directory exists
Path(LOG_FILE).parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="w",
)
logger = logging.getLogger(__name__)


class ProcessLock:
    """Process lock to prevent multiple instances from running simultaneously."""

    def __init__(self, lock_file: str):
        self.lock_file = Path(lock_file)
        self.lock_file.parent.mkdir(parents=True, exist_ok=True)
        self.lock_fd = None
        self.file_lock = FileLock(str(self.lock_file), timeout=-1)

    def __enter__(self):
        try:
            self.file_lock.acquire()
            self.lock_fd = open(self.lock_file, "w")
            self.lock_fd.write(str(os.getpid()))
        except Timeout:
            print("\nError: Another US download process is running")
            print(f"Lock file: {self.lock_file}")
            print(f"\nIf no other process is running, delete the lock file:")
            print(f"  rm {self.lock_file}")
            sys.exit(1)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.lock_fd:
            try:
                self.lock_fd.close()
                self.file_lock.release()
            except Exception:
                pass
        try:
            self.lock_file.unlink(missing_ok=True)
        except Exception:
            pass


class USDownloader:
    """US stock data downloader with DuckDB storage."""

    def __init__(self, db_path: str, symbols: list[str] | None = None):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self.fetcher = YFinanceFetcher()
        self.writer = DuckDBWriter(db_path=str(self.db_path))
        self.custom_symbols = symbols
        self.failed_stocks = []

    def get_incremental_start_date(
        self, symbol: str, default_start: str
    ) -> str:
        """Get next date after MAX(date) for incremental updates."""
        max_date = self.writer.get_max_date("stocks", symbol)
        if max_date:
            next_day = datetime.strptime(max_date, "%Y-%m-%d") + timedelta(days=1)
            return next_day.strftime("%Y-%m-%d")
        return default_start

    # ========================================
    # Phase 1: Stock list
    # ========================================

    def get_stock_list(self) -> list[str]:
        """Get stock list, either custom or from NASDAQ trader."""
        if self.custom_symbols:
            return self.custom_symbols
        return self.fetcher.fetch_stock_list()

    # ========================================
    # Phase 2: Batch OHLCV + adjust factors
    # ========================================

    def download_ohlcv_batch(
        self,
        symbols: list[str],
        start_date: str,
        end_date: str,
        pbar=None,
    ) -> int:
        """
        Download OHLCV data for a batch of symbols.

        Uses yf.download for efficient batch retrieval,
        then writes per-symbol to DuckDB.

        Returns:
            Number of symbols successfully written.
        """
        # Determine per-symbol start dates for incremental update
        # Use the earliest needed start for the batch download
        symbol_starts = {}
        earliest_start = end_date
        for sym in symbols:
            sym_start = self.get_incremental_start_date(sym, start_date)
            symbol_starts[sym] = sym_start
            if sym_start < earliest_start:
                earliest_start = sym_start

        if earliest_start >= end_date:
            if pbar:
                pbar.update(len(symbols))
            return 0  # All symbols up to date

        # Batch download (OHLCV + adjust factors from single request)
        data, adj_data = self.fetcher.fetch_batch_ohlcv(symbols, earliest_start, end_date)
        if not data:
            if pbar:
                pbar.update(len(symbols))
            return 0

        # Write per-symbol
        success = 0
        self.writer.begin()
        try:
            for sym in symbols:
                try:
                    if sym not in data:
                        continue

                    df = data[sym]
                    sym_start = symbol_starts.get(sym, start_date)

                    # Filter to only new data
                    df = df[df.index >= pd.Timestamp(sym_start)]
                    if df.empty:
                        continue

                    self.writer.write_market_data(sym, df)

                    success += 1
                except Exception as e:
                    logger.warning(f"Failed to write OHLCV for {sym}: {e}")
                    self.failed_stocks.append(sym)
                finally:
                    if pbar:
                        pbar.update(1)

            self.writer.commit()
        except Exception:
            self.writer.rollback()
            raise

        return success

    # ========================================
    # Phase 3: Per-stock fundamentals + valuation
    # ========================================

    def download_fundamentals_and_valuation(
        self,
        symbols: list[str],
        pbar=None,
    ) -> int:
        """
        Download fundamentals and valuation data per-stock.

        Returns:
            Number of symbols successfully processed.
        """
        success = 0
        batch_count = 0

        self.writer.begin()
        try:
            for sym in symbols:
                try:
                    ohlcv = self._load_ohlcv_from_db(sym)
                    fund_df, val_df = self.fetcher.fetch_stock_detail(sym, ohlcv)

                    if not fund_df.empty:
                        self.writer.write_fundamentals(sym, fund_df)

                    if not val_df.empty:
                        self.writer.write_valuation(sym, val_df)

                    success += 1
                    self.fetcher._throttle()

                except Exception as e:
                    logger.warning(
                        f"Failed fundamentals/valuation for {sym}: {e}"
                    )
                    self.failed_stocks.append(sym)
                finally:
                    batch_count += 1
                    if pbar:
                        pbar.update(1)

                    # Commit every COMMIT_BATCH_SIZE stocks
                    if batch_count % COMMIT_BATCH_SIZE == 0:
                        self.writer.commit()
                        self.writer.begin()

            self.writer.commit()
        except Exception:
            self.writer.rollback()
            raise

        return success

    # ========================================
    # Phase 4: Per-stock metadata + exrights
    # ========================================

    def download_metadata_and_exrights(
        self,
        symbols: list[str],
        pbar=None,
    ) -> int:
        """
        Download metadata and exrights data per-stock.

        Returns:
            Number of symbols successfully processed.
        """
        success = 0
        batch_count = 0

        self.writer.begin()
        try:
            for sym in symbols:
                try:
                    # Metadata
                    meta = self.fetcher.fetch_metadata(sym)
                    if meta:
                        meta_df = pd.DataFrame([meta])
                        self.writer.write_stock_metadata(meta_df)

                    # Exrights
                    exr_df = self.fetcher.fetch_exrights(sym)
                    if not exr_df.empty:
                        self.writer.write_exrights(sym, exr_df)

                    success += 1
                    self.fetcher._throttle()

                except Exception as e:
                    logger.warning(f"Failed metadata/exrights for {sym}: {e}")
                    self.failed_stocks.append(sym)
                finally:
                    batch_count += 1
                    if pbar:
                        pbar.update(1)

                    if batch_count % COMMIT_BATCH_SIZE == 0:
                        self.writer.commit()
                        self.writer.begin()

            self.writer.commit()
        except Exception:
            self.writer.rollback()
            raise

        return success

    # ========================================
    # Phase 5: Benchmark + trade_days + index_constituents
    # ========================================

    def download_global_data(self, start_date: str, end_date: str) -> None:
        """Download benchmark, trade days, and index constituents."""
        # Benchmark (S&P 500) + Trade days (from same data)
        print("  Benchmark (S&P 500) + Trade days...")
        try:
            bench_df = self.fetcher.fetch_benchmark(start_date, end_date)
            if not bench_df.empty:
                self.writer.write_benchmark(bench_df)
                trade_days = pd.DataFrame({"date": bench_df.index})
                self.writer.write_trade_days(trade_days)
                print(f"    {len(bench_df)} days")
        except Exception as e:
            logger.error(f"Failed to download benchmark: {e}")

        # Index constituents
        print("  Index constituents (S&P 500)...")
        try:
            sp500 = self.fetcher.fetch_index_constituents_sp500()
            if sp500:
                today = datetime.now().strftime("%Y-%m-%d")
                self.writer.write_index_constituents(today, "SPX.US", sp500)
                print(f"    S&P 500: {len(sp500)} stocks")
        except Exception as e:
            logger.error(f"Failed to download S&P 500 constituents: {e}")

        print("  Index constituents (NASDAQ-100)...")
        try:
            ndx = self.fetcher.fetch_index_constituents_ndx100()
            if ndx:
                today = datetime.now().strftime("%Y-%m-%d")
                self.writer.write_index_constituents(today, "NDX.US", ndx)
                print(f"    NASDAQ-100: {len(ndx)} stocks")
        except Exception as e:
            logger.error(f"Failed to download NASDAQ-100 constituents: {e}")

    # ========================================
    # Helpers
    # ========================================

    def _load_ohlcv_from_db(self, symbol: str) -> pd.DataFrame:
        """Load existing OHLCV data from DuckDB for valuation calculation."""
        try:
            df = self.writer.conn.execute(
                "SELECT date, open, high, low, close, volume FROM stocks WHERE symbol = ? ORDER BY date",
                [symbol],
            ).fetchdf()
            if df.empty:
                return pd.DataFrame()
            df["date"] = pd.to_datetime(df["date"])
            df = df.set_index("date")
            return df
        except Exception:
            return pd.DataFrame()


def download_us_data(
    symbols: list[str] | None = None,
    skip_fundamentals: bool = False,
    skip_metadata: bool = False,
    start_date: str | None = None,
):
    """
    Main US stock download function.

    Args:
        symbols: Optional list of specific symbols (e.g., ['AAPL', 'MSFT'])
        skip_fundamentals: Skip Phase 3 (fundamentals + valuation)
        skip_metadata: Skip Phase 4 (metadata + exrights)
        start_date: Override default start date
    """
    with ProcessLock(LOCK_FILE):
        print("=" * 70)
        print("SimTradeData Download (US Stocks - yfinance)")
        print("=" * 70)

        end_date = (
            datetime.now().date()
            if END_DATE is None
            else datetime.strptime(END_DATE, "%Y-%m-%d").date()
        )
        use_start_date = start_date or START_DATE
        end_date_str = end_date.strftime("%Y-%m-%d")

        print(f"Date range: {use_start_date} ~ {end_date_str}")

        # Convert symbol args to PTrade format if provided as raw tickers
        ptrade_symbols = None
        if symbols:
            ptrade_symbols = []
            for s in symbols:
                if "." not in s:
                    ptrade_symbols.append(f"{s}.US")
                else:
                    ptrade_symbols.append(s)

        db_path = US_DUCKDB_PATH
        downloader = USDownloader(db_path=str(db_path), symbols=ptrade_symbols)

        try:
            # Phase 1: Get stock list
            print("\nFetching stock list...")
            stock_list = downloader.get_stock_list()
            if not stock_list:
                print("Error: No stocks found")
                return
            print(f"Total stocks: {len(stock_list)}")

            # Pre-fetch stocks with existing fundamentals/valuation (batch, not per-symbol)
            has_fund = {r[0] for r in downloader.writer.conn.execute(
                "SELECT DISTINCT symbol FROM fundamentals"
            ).fetchall()}
            has_meta = {r[0] for r in downloader.writer.conn.execute(
                "SELECT DISTINCT symbol FROM valuation"
            ).fetchall()}

            # Filter stocks needing OHLCV update
            needs_ohlcv = []
            already_current = []
            needs_fundamentals = []
            needs_metadata = []
            for sym in stock_list:
                sym_start = downloader.get_incremental_start_date(sym, use_start_date)
                if sym_start >= end_date_str:
                    already_current.append(sym)
                else:
                    needs_ohlcv.append(sym)
                if sym not in has_fund:
                    needs_fundamentals.append(sym)
                if sym not in has_meta:
                    needs_metadata.append(sym)

            if already_current:
                print(f"  Resume: {len(needs_ohlcv)} need download, "
                      f"{len(already_current)} already have latest data")

            # Phase 2: Batch OHLCV + adjust factors
            if needs_ohlcv:
                ohlcv_batches = [
                    needs_ohlcv[i : i + OHLCV_BATCH_SIZE]
                    for i in range(0, len(needs_ohlcv), OHLCV_BATCH_SIZE)
                ]
                total_ohlcv = 0

                print(f"\nProcessing {len(needs_ohlcv)} stocks in {len(ohlcv_batches)} batches...")
                print(f"Batch size: {OHLCV_BATCH_SIZE}")
                print("=" * 60)

                with tqdm(
                    total=len(needs_ohlcv),
                    desc="Downloading stocks",
                    unit="stock",
                    ncols=100,
                    bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
                ) as pbar:
                    for batch in ohlcv_batches:
                        try:
                            n = downloader.download_ohlcv_batch(
                                batch, use_start_date, end_date_str, pbar
                            )
                            total_ohlcv += n
                        except Exception as e:
                            logger.error(f"OHLCV batch failed: {e}")
                            pbar.update(len(batch))

                print("=" * 60)
                print(f"Download complete: {total_ohlcv} updated, "
                      f"{len(needs_ohlcv) - total_ohlcv} skipped/failed")
            else:
                print("\nAll stocks already have latest OHLCV data.")

            # Phase 3: Per-stock fundamentals + valuation
            # Use union of stocks needing OHLCV refresh + stocks never downloaded
            fund_targets = sorted(set(needs_ohlcv) | set(needs_fundamentals))
            if not skip_fundamentals and fund_targets:
                print(f"\nDownloading fundamentals for {len(fund_targets)} stocks...")
                print("=" * 60)
                with tqdm(
                    total=len(fund_targets),
                    desc="Fundamentals",
                    unit="stock",
                    ncols=100,
                    bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
                ) as pbar:
                    n = downloader.download_fundamentals_and_valuation(
                        fund_targets, pbar
                    )
                print("=" * 60)
                print(f"Fundamentals complete: {n} updated, "
                      f"{len(fund_targets) - n} skipped/failed")

            # Phase 4: Per-stock metadata + exrights
            # Use union of stocks needing OHLCV refresh + stocks never downloaded
            meta_targets = sorted(set(needs_ohlcv) | set(needs_metadata))
            if not skip_metadata and meta_targets:
                print(f"\nDownloading metadata for {len(meta_targets)} stocks...")
                print("=" * 60)
                with tqdm(
                    total=len(meta_targets),
                    desc="Metadata",
                    unit="stock",
                    ncols=100,
                    bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
                ) as pbar:
                    n = downloader.download_metadata_and_exrights(
                        meta_targets, pbar
                    )
                print("=" * 60)
                print(f"Metadata complete: {n} updated, "
                      f"{len(meta_targets) - n} skipped/failed")

            # Phase 5: Global data
            print("\nDownloading benchmark & index data...")
            downloader.download_global_data(use_start_date, end_date_str)

        finally:
            downloader.writer.close()

        # Summary
        print("\n" + "=" * 60)
        print("Download Complete!")
        print("=" * 60)

        if db_path.exists():
            db_size = db_path.stat().st_size / (1024 * 1024)
            print(f"\nDatabase: {db_path}")
            print(f"Size: {db_size:.1f} MB")

        if downloader.failed_stocks:
            unique_failed = list(set(downloader.failed_stocks))
            print(f"\nFailed stocks: {len(unique_failed)}")
            if len(unique_failed) <= 20:
                print(f"  {unique_failed}")
            logger.info(f"Failed stocks: {unique_failed}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download US stock data to DuckDB via yfinance"
    )
    parser.add_argument(
        "--symbols",
        type=str,
        default=None,
        help="Comma-separated list of symbols (e.g., AAPL,MSFT,GOOGL)",
    )
    parser.add_argument(
        "--skip-fundamentals",
        action="store_true",
        help="Skip fundamentals and valuation download",
    )
    parser.add_argument(
        "--skip-metadata",
        action="store_true",
        help="Skip metadata and exrights download",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Override default start date (YYYY-MM-DD)",
    )

    args = parser.parse_args()

    symbol_list = None
    if args.symbols:
        symbol_list = [s.strip() for s in args.symbols.split(",")]

    download_us_data(
        symbols=symbol_list,
        skip_fundamentals=args.skip_fundamentals,
        skip_metadata=args.skip_metadata,
        start_date=args.start_date,
    )
