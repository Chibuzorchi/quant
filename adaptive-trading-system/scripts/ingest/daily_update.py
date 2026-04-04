#!/usr/bin/env python3
"""
Daily Update Pipeline
Fetches new data since last ingestion, validates, appends to validated Parquet,
and refreshes the DuckDB research database.

Run manually or via cron:
  0 0 * * * cd /path/to/project && venv/bin/python scripts/ingest/daily_update.py
"""

import ccxt
import pandas as pd
import requests
import time
import sys
import os
import json
import shutil
from pathlib import Path
from datetime import datetime, timezone, timedelta
from loguru import logger
from dotenv import load_dotenv

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_RAW = PROJECT_ROOT / 'data' / 'raw'
DATA_VALIDATED = PROJECT_ROOT / 'data' / 'validated'
LOG_DIR = PROJECT_ROOT / 'logs'
LOG_DIR.mkdir(exist_ok=True)

# Load env
load_dotenv(PROJECT_ROOT / 'config' / '.env')

# Logging
logger.remove()
logger.add(sys.stdout, level='INFO')
logger.add(
    LOG_DIR / 'daily_update_{time:YYYY-MM-DD}.log',
    rotation='1 day',
    retention='30 days',
    level='INFO'
)

# Config
PAIRS = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'BNB/USDT', 'XRP/USDT', 'DOGE/USDT']
TIMEFRAMES = ['1d', '4h', '1h', '5m', '1m']
FUNDING_PAIRS = ['BTC/USDT:USDT', 'ETH/USDT:USDT']
EXCHANGE_ID = 'binance'


class DailyUpdater:
    """Handles daily incremental data updates."""

    def __init__(self):
        api_key = os.getenv(f'{EXCHANGE_ID.upper()}_API_KEY')
        secret = os.getenv(f'{EXCHANGE_ID.upper()}_SECRET')

        params = {}
        if api_key and secret:
            params = {'apiKey': api_key, 'secret': secret}

        self.exchange = getattr(ccxt, EXCHANGE_ID)(params)
        self.exchange.load_markets()
        self.stats = {'ohlcv_rows': 0, 'funding_rows': 0, 'sentiment_rows': 0, 'errors': []}

    def _get_last_timestamp(self, filepath: Path) -> int:
        """Get the last timestamp from an existing parquet file (ms)."""
        if not filepath.exists():
            return 0
        df = pd.read_parquet(filepath, columns=['timestamp'])
        if df.empty:
            return 0
        return int(df['timestamp'].max().timestamp() * 1000)

    def update_ohlcv(self):
        """Fetch new OHLCV candles since last ingestion."""
        logger.info("--- OHLCV UPDATE ---")
        ohlcv_dir = DATA_RAW / 'ohlcv' / EXCHANGE_ID
        ohlcv_dir.mkdir(parents=True, exist_ok=True)

        for symbol in PAIRS:
            for timeframe in TIMEFRAMES:
                safe_symbol = symbol.replace('/', '_')
                filepath = ohlcv_dir / f'{safe_symbol}_{timeframe}.parquet'

                last_ts = self._get_last_timestamp(filepath)
                if last_ts == 0:
                    logger.warning(f"  {symbol} {timeframe}: no existing data, skipping (run full ingestion first)")
                    continue

                since = last_ts + 1
                now_ts = int(datetime.now(timezone.utc).timestamp() * 1000)

                if since >= now_ts:
                    logger.info(f"  {symbol} {timeframe}: already up to date")
                    continue

                new_rows = []
                current = since

                while current < now_ts:
                    try:
                        ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe, since=current, limit=1000)
                        if not ohlcv:
                            break
                        new_rows.extend(ohlcv)
                        current = ohlcv[-1][0] + 1
                        time.sleep(self.exchange.rateLimit / 1000)
                    except ccxt.RateLimitExceeded:
                        time.sleep(60)
                    except Exception as e:
                        logger.error(f"  {symbol} {timeframe} error: {e}")
                        self.stats['errors'].append(f"ohlcv {symbol} {timeframe}: {e}")
                        break

                if not new_rows:
                    continue

                new_df = pd.DataFrame(new_rows, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                new_df['timestamp'] = pd.to_datetime(new_df['timestamp'], unit='ms', utc=True)
                new_df['symbol'] = symbol
                new_df['exchange'] = EXCHANGE_ID
                new_df['timeframe'] = timeframe

                # Append to existing
                existing = pd.read_parquet(filepath)
                combined = pd.concat([existing, new_df], ignore_index=True)
                combined = combined.drop_duplicates(subset=['timestamp', 'symbol'], keep='last')
                combined = combined.sort_values('timestamp').reset_index(drop=True)
                combined.to_parquet(filepath, index=False, compression='snappy')

                added = len(combined) - len(existing)
                self.stats['ohlcv_rows'] += added
                logger.info(f"  {symbol} {timeframe}: +{added} rows (total: {len(combined):,})")

    def update_funding_rates(self):
        """Fetch new funding rates."""
        logger.info("--- FUNDING RATES UPDATE ---")
        funding_dir = DATA_RAW / 'funding_rates' / EXCHANGE_ID
        funding_dir.mkdir(parents=True, exist_ok=True)

        for symbol in FUNDING_PAIRS:
            safe_symbol = symbol.replace('/', '_').replace(':', '_')
            filepath = funding_dir / f'{safe_symbol}_funding.parquet'

            last_ts = self._get_last_timestamp(filepath)
            if last_ts == 0:
                logger.warning(f"  {symbol}: no existing data, skipping")
                continue

            since = last_ts + 1
            new_rows = []

            try:
                while True:
                    data = self.exchange.fetch_funding_rate_history(symbol, since=since, limit=1000)
                    if not data:
                        break
                    new_rows.extend(data)
                    since = data[-1]['timestamp'] + 1
                    time.sleep(self.exchange.rateLimit / 1000)
            except Exception as e:
                logger.error(f"  {symbol} funding error: {e}")
                self.stats['errors'].append(f"funding {symbol}: {e}")

            if not new_rows:
                logger.info(f"  {symbol}: already up to date")
                continue

            new_df = pd.DataFrame(new_rows)
            new_df['timestamp'] = pd.to_datetime(new_df['timestamp'], unit='ms', utc=True)
            new_df['funding_rate'] = new_df['fundingRate']
            new_df['exchange'] = EXCHANGE_ID
            new_df = new_df[['timestamp', 'symbol', 'funding_rate', 'exchange']]

            existing = pd.read_parquet(filepath)
            combined = pd.concat([existing, new_df], ignore_index=True)
            combined = combined.drop_duplicates(subset=['timestamp', 'symbol'], keep='last')
            combined = combined.sort_values('timestamp').reset_index(drop=True)
            combined.to_parquet(filepath, index=False, compression='snappy')

            added = len(combined) - len(existing)
            self.stats['funding_rows'] += added
            logger.info(f"  {symbol}: +{added} rows")

    def update_sentiment(self):
        """Fetch latest Fear & Greed values."""
        logger.info("--- SENTIMENT UPDATE ---")
        filepath = DATA_RAW / 'sentiment' / 'fear_greed_index.parquet'

        if not filepath.exists():
            logger.warning("  No existing sentiment data, skipping")
            return

        try:
            response = requests.get(
                "https://api.alternative.me/fng/",
                params={'limit': 7, 'format': 'json'},
                timeout=30
            )
            response.raise_for_status()
            records = response.json().get('data', [])

            if not records:
                logger.info("  No new sentiment data")
                return

            new_df = pd.DataFrame(records)
            new_df['timestamp'] = pd.to_datetime(new_df['timestamp'].astype(int), unit='s', utc=True)
            new_df = new_df.rename(columns={'value_classification': 'classification'})
            new_df['value'] = new_df['value'].astype(int)
            new_df = new_df[['timestamp', 'value', 'classification']]

            existing = pd.read_parquet(filepath)
            combined = pd.concat([existing, new_df], ignore_index=True)
            combined = combined.drop_duplicates(subset=['timestamp'], keep='last')
            combined = combined.sort_values('timestamp').reset_index(drop=True)
            combined.to_parquet(filepath, index=False, compression='snappy')

            added = len(combined) - len(existing)
            self.stats['sentiment_rows'] += added
            logger.info(f"  Fear & Greed: +{added} rows (total: {len(combined):,})")

        except Exception as e:
            logger.error(f"  Sentiment error: {e}")
            self.stats['errors'].append(f"sentiment: {e}")

    def run_validation(self):
        """Run quick validation on raw data and copy to validated."""
        logger.info("--- VALIDATION ---")
        sys.path.insert(0, str(PROJECT_ROOT / 'scripts' / 'validate'))
        from validate_schema import run_schema_validation
        from validate_range import run_range_validation

        schema_result = run_schema_validation(DATA_RAW)
        range_result = run_range_validation(DATA_RAW)

        failures = schema_result['failed'] + range_result['failed']

        if failures == 0:
            logger.info("  Validation passed — syncing to validated/")
            # Sync raw to validated (rsync-style: only copy changed files)
            for src_file in DATA_RAW.rglob('*.parquet'):
                relative = src_file.relative_to(DATA_RAW)
                dest = DATA_VALIDATED / relative
                # Only copy if source is newer
                if not dest.exists() or src_file.stat().st_mtime > dest.stat().st_mtime:
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(src_file, dest)
        else:
            logger.warning(f"  {failures} validation failures — NOT updating validated/")

        return failures

    def refresh_duckdb(self):
        """Refresh the DuckDB research database views."""
        logger.info("--- DUCKDB REFRESH ---")
        try:
            sys.path.insert(0, str(PROJECT_ROOT / 'scripts' / 'utils'))
            from db_setup import create_research_db
            db_path = PROJECT_ROOT / 'data' / 'research.duckdb'
            con = create_research_db(str(DATA_VALIDATED), str(db_path))
            con.close()
            logger.info("  DuckDB refreshed")
        except Exception as e:
            logger.error(f"  DuckDB refresh error: {e}")
            self.stats['errors'].append(f"duckdb: {e}")

    def create_backup(self):
        """Create a daily backup snapshot."""
        logger.info("--- BACKUP ---")
        backup_dir = PROJECT_ROOT / 'data' / 'backups'
        backup_dir.mkdir(parents=True, exist_ok=True)

        today = datetime.now(timezone.utc).strftime('%Y%m%d')
        backup_path = backup_dir / f'validated_{today}'

        if backup_path.exists():
            logger.info(f"  Backup already exists for {today}")
            return

        # Only keep last 3 backups to save disk
        existing_backups = sorted(backup_dir.glob('validated_*'))
        while len(existing_backups) >= 3:
            oldest = existing_backups.pop(0)
            shutil.rmtree(oldest)
            logger.info(f"  Removed old backup: {oldest.name}")

        shutil.copytree(DATA_VALIDATED, backup_path)
        backup_size = sum(f.stat().st_size for f in backup_path.rglob('*') if f.is_file())
        logger.info(f"  Backup created: {backup_path.name} ({backup_size / 1024 / 1024:.1f} MB)")

    def print_summary(self, start_time: float, validation_failures: int):
        """Print daily update summary."""
        elapsed = time.time() - start_time

        logger.info("")
        logger.info("=" * 60)
        logger.info("DAILY UPDATE SUMMARY")
        logger.info("=" * 60)
        logger.info(f"  Time:          {elapsed:.0f}s ({elapsed/60:.1f} min)")
        logger.info(f"  OHLCV rows:    +{self.stats['ohlcv_rows']:,}")
        logger.info(f"  Funding rows:  +{self.stats['funding_rows']}")
        logger.info(f"  Sentiment rows:+{self.stats['sentiment_rows']}")
        logger.info(f"  Validation:    {'PASS' if validation_failures == 0 else f'FAIL ({validation_failures})'}")
        logger.info(f"  Errors:        {len(self.stats['errors'])}")

        if self.stats['errors']:
            logger.warning("  Error details:")
            for err in self.stats['errors']:
                logger.warning(f"    - {err}")

        logger.info("=" * 60)


def main():
    start_time = time.time()

    logger.info("#" * 60)
    logger.info(f"DAILY UPDATE — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    logger.info("#" * 60)

    updater = DailyUpdater()

    # 1. Fetch new data
    updater.update_ohlcv()
    updater.update_funding_rates()
    updater.update_sentiment()

    # 2. Validate
    validation_failures = updater.run_validation()

    # 3. Refresh DuckDB
    if validation_failures == 0:
        updater.refresh_duckdb()

    # 4. Backup (weekly — only on Sundays)
    if datetime.now(timezone.utc).weekday() == 6:
        updater.create_backup()

    # 5. Summary
    updater.print_summary(start_time, validation_failures)


if __name__ == "__main__":
    main()
