#!/usr/bin/env python3
"""
OHLCV Data Fetcher - The Workhorse
Fetches historical OHLCV data from exchanges via CCXT with:
- Rate limiting (respects exchange limits)
- Pagination (handles millions of rows)
- Resumability (checkpoint/resume on failure)
- Logging (every fetch logged with timestamp)
- Error handling (network timeouts, exchange maintenance)
"""

import ccxt
import pandas as pd
import time
import json
import sys
from pathlib import Path
from datetime import datetime, timezone
from loguru import logger
import os
from dotenv import load_dotenv

# Load environment variables
env_path = Path(__file__).parent.parent.parent / 'config' / '.env'
load_dotenv(env_path)

# Configure logging
logger.remove()  # Remove default stderr handler to avoid noise in orchestrator
log_dir = Path(__file__).parent.parent.parent / 'logs'
log_dir.mkdir(exist_ok=True)
logger.add(sys.stdout, level='INFO')  # Explicit stdout handler
logger.add(
    log_dir / 'ohlcv_ingestion_{time:YYYY-MM-DD}.log',
    rotation='1 day',
    retention='30 days',
    level='INFO'
)


class OHLCVFetcher:
    """Fetches and stores OHLCV data from exchanges."""

    def __init__(self, exchange_id: str, api_key: str = None, secret: str = None):
        """
        Initialize the fetcher.

        Args:
            exchange_id: Exchange name (e.g., 'binance', 'bybit')
            api_key: API key (optional for public endpoints)
            secret: API secret (optional for public endpoints)
        """
        self.exchange_id = exchange_id

        # Initialize exchange
        exchange_class = getattr(ccxt, exchange_id)
        params = {}
        if api_key and secret:
            params = {'apiKey': api_key, 'secret': secret}

        self.exchange = exchange_class(params)
        self.exchange.load_markets()

        logger.info(f"Initialized {exchange_id} - Rate limit: {self.exchange.rateLimit}ms")

    def _get_checkpoint_path(self, symbol: str, timeframe: str, output_dir: Path) -> Path:
        """Get checkpoint file path for resumability."""
        safe_symbol = symbol.replace('/', '_')
        return output_dir / f'.checkpoint_{safe_symbol}_{timeframe}.json'

    def _load_checkpoint(self, checkpoint_path: Path) -> int:
        """Load the last fetched timestamp from checkpoint."""
        if checkpoint_path.exists():
            with open(checkpoint_path, 'r') as f:
                data = json.load(f)
                return data.get('last_timestamp', 0)
        return 0

    def _save_checkpoint(self, checkpoint_path: Path, timestamp: int, rows_fetched: int):
        """Save checkpoint for resumability."""
        with open(checkpoint_path, 'w') as f:
            json.dump({
                'last_timestamp': timestamp,
                'rows_fetched': rows_fetched,
                'updated_at': datetime.now(timezone.utc).isoformat()
            }, f)

    def fetch_ohlcv_history(
        self,
        symbol: str,
        timeframe: str,
        since: str,
        until: str,
        output_dir: Path,
        resume: bool = True
    ) -> pd.DataFrame:
        """
        Fetch complete OHLCV history for a symbol.

        Args:
            symbol: Trading pair (e.g., 'BTC/USDT')
            timeframe: Timeframe (e.g., '1h', '1d')
            since: Start date (YYYY-MM-DD)
            until: End date (YYYY-MM-DD or 'present')
            output_dir: Directory to save raw data
            resume: Resume from checkpoint if exists

        Returns:
            DataFrame with OHLCV data
        """
        logger.info(f"Fetching {symbol} {timeframe} from {since} to {until}")

        # Convert dates to timestamps
        since_ts = int(pd.Timestamp(since, tz='UTC').timestamp() * 1000)

        if until == 'present':
            until_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
        else:
            until_ts = int(pd.Timestamp(until, tz='UTC').timestamp() * 1000)

        # Setup checkpoint
        checkpoint_path = self._get_checkpoint_path(symbol, timeframe, output_dir)

        if resume:
            checkpoint_ts = self._load_checkpoint(checkpoint_path)
            if checkpoint_ts > since_ts:
                logger.info(f"Resuming from checkpoint: {pd.Timestamp(checkpoint_ts, unit='ms', tz='UTC')}")
                current_since = checkpoint_ts + 1  # Next millisecond
            else:
                current_since = since_ts
        else:
            current_since = since_ts

        all_data = []
        total_rows = 0
        batch_rows = 0
        consecutive_empty = 0
        max_consecutive_empty = 3
        flush_threshold = 50000  # Write to disk every 50k rows to avoid memory issues

        # Prepare output path for incremental writes
        safe_symbol = symbol.replace('/', '_')
        temp_parts_dir = output_dir / f'.parts_{safe_symbol}_{timeframe}'
        temp_parts_dir.mkdir(parents=True, exist_ok=True)
        part_index = 0

        while current_since < until_ts:
            try:
                # Fetch batch
                ohlcv = self.exchange.fetch_ohlcv(
                    symbol,
                    timeframe,
                    since=current_since,
                    limit=1000  # Most exchanges support 1000-1500
                )

                if not ohlcv or len(ohlcv) == 0:
                    consecutive_empty += 1
                    logger.warning(f"Empty response for {symbol} {timeframe} at {pd.Timestamp(current_since, unit='ms', tz='UTC')}")

                    if consecutive_empty >= max_consecutive_empty:
                        logger.warning(f"Reached max consecutive empty responses ({max_consecutive_empty}), stopping")
                        break

                    # Move forward by one timeframe interval
                    current_since = self._increment_timestamp(current_since, timeframe)
                    time.sleep(self.exchange.rateLimit / 1000)
                    continue

                consecutive_empty = 0
                all_data.extend(ohlcv)
                total_rows += len(ohlcv)
                batch_rows += len(ohlcv)

                # Update timestamp for next iteration
                last_timestamp = ohlcv[-1][0]
                current_since = last_timestamp + 1

                # Flush to disk when batch gets large (prevents memory issues on 1m data)
                if batch_rows >= flush_threshold:
                    part_df = pd.DataFrame(all_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    part_df.to_parquet(temp_parts_dir / f'part_{part_index:04d}.parquet', index=False)
                    all_data = []
                    batch_rows = 0
                    part_index += 1
                    logger.info(f"Flushed part {part_index} to disk ({total_rows} total rows)")

                # Save checkpoint every 1000 rows
                if total_rows % 1000 == 0:
                    self._save_checkpoint(checkpoint_path, last_timestamp, total_rows)
                    logger.info(f"Fetched {total_rows} rows for {symbol} {timeframe} - Last: {pd.Timestamp(last_timestamp, unit='ms', tz='UTC')}")

                # Rate limiting
                time.sleep(self.exchange.rateLimit / 1000)

            except ccxt.RateLimitExceeded:
                logger.warning(f"Rate limit exceeded, sleeping 60s")
                time.sleep(60)
                continue

            except ccxt.NetworkError as e:
                logger.error(f"Network error: {e}, retrying in 10s")
                time.sleep(10)
                continue

            except ccxt.ExchangeError as e:
                logger.error(f"Exchange error: {e}, retrying in 10s")
                time.sleep(10)
                continue

            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                break

        # Combine any remaining in-memory data with flushed parts
        if all_data:
            part_df = pd.DataFrame(all_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            if part_index > 0:
                part_df.to_parquet(temp_parts_dir / f'part_{part_index:04d}.parquet', index=False)
                part_index += 1
                all_data = []

        # Reassemble from parts if we flushed, otherwise use in-memory data
        if part_index > 0:
            part_files = sorted(temp_parts_dir.glob('part_*.parquet'))
            df = pd.concat([pd.read_parquet(f) for f in part_files], ignore_index=True)
            # Clean up temp parts
            for f in part_files:
                f.unlink()
            temp_parts_dir.rmdir()
        elif all_data:
            df = pd.DataFrame(all_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        else:
            logger.warning(f"No data fetched for {symbol} {timeframe}")
            # Clean up empty temp dir
            if temp_parts_dir.exists() and not any(temp_parts_dir.iterdir()):
                temp_parts_dir.rmdir()
            return pd.DataFrame()

        # Clean up empty temp dir if it still exists
        if temp_parts_dir.exists() and not any(temp_parts_dir.iterdir()):
            temp_parts_dir.rmdir()

        # Convert timestamp to datetime (UTC)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)

        # Add metadata
        df['symbol'] = symbol
        df['exchange'] = self.exchange_id
        df['timeframe'] = timeframe

        # Remove duplicates (can happen with pagination overlap)
        df = df.drop_duplicates(subset=['timestamp', 'symbol'], keep='last')

        # Sort by timestamp
        df = df.sort_values('timestamp').reset_index(drop=True)

        logger.info(f"✓ Fetched {len(df)} total rows for {symbol} {timeframe}")

        return df

    def _increment_timestamp(self, timestamp: int, timeframe: str) -> int:
        """Increment timestamp by one timeframe interval."""
        intervals = {
            '1m': 60 * 1000,
            '5m': 5 * 60 * 1000,
            '15m': 15 * 60 * 1000,
            '1h': 60 * 60 * 1000,
            '4h': 4 * 60 * 60 * 1000,
            '1d': 24 * 60 * 60 * 1000,
        }
        return timestamp + intervals.get(timeframe, 60 * 60 * 1000)

    def save_to_parquet(self, df: pd.DataFrame, output_dir: Path):
        """
        Save DataFrame to Parquet with proper partitioning.

        Args:
            df: DataFrame with OHLCV data
            output_dir: Output directory
        """
        if df.empty:
            logger.warning("Empty DataFrame, skipping save")
            return

        symbol = df['symbol'].iloc[0]
        timeframe = df['timeframe'].iloc[0]
        exchange = df['exchange'].iloc[0]

        # Create directory structure: data/raw/ohlcv/{exchange}/
        exchange_dir = output_dir / exchange
        exchange_dir.mkdir(parents=True, exist_ok=True)

        # Filename: {symbol}_{timeframe}.parquet
        safe_symbol = symbol.replace('/', '_')
        filename = f"{safe_symbol}_{timeframe}.parquet"
        filepath = exchange_dir / filename

        # Merge with existing file if present (e.g., from historical download)
        if filepath.exists():
            existing_df = pd.read_parquet(filepath)
            df = pd.concat([existing_df, df], ignore_index=True)
            df = df.drop_duplicates(subset=['timestamp', 'symbol'], keep='last')
            df = df.sort_values('timestamp').reset_index(drop=True)
            logger.info(f"Merged with existing data: {len(existing_df)} + new → {len(df)} total rows")

        # Save as Parquet
        df.to_parquet(filepath, index=False, compression='snappy')

        file_size_mb = filepath.stat().st_size / (1024 * 1024)
        logger.info(f"✓ Saved to {filepath} ({file_size_mb:.2f} MB)")


def main():
    """Main function to fetch OHLCV data for MVP dataset."""

    # Load configuration
    import yaml
    config_path = Path(__file__).parent.parent.parent / 'config' / 'data_sources.yaml'
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    # Get OHLCV configuration
    ohlcv_config = config['ohlcv']
    exchange_id = ohlcv_config['primary_exchange']
    pairs = ohlcv_config['pairs']
    timeframes = list(ohlcv_config['timeframes'].keys())
    start_date = ohlcv_config['history']['start_date']
    end_date = ohlcv_config['history']['end_date']

    # Output directory
    output_dir = Path(__file__).parent.parent.parent / 'data' / 'raw' / 'ohlcv'
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load API credentials
    api_key = os.getenv(f'{exchange_id.upper()}_API_KEY')
    secret = os.getenv(f'{exchange_id.upper()}_SECRET')

    # Initialize fetcher
    fetcher = OHLCVFetcher(exchange_id, api_key, secret)

    # Fetch data for all pairs and timeframes
    logger.info(f"Starting OHLCV ingestion for {len(pairs)} pairs × {len(timeframes)} timeframes")
    logger.info(f"Date range: {start_date} to {end_date}")

    total_fetched = 0

    # Sort timeframes: critical first (1d, 4h, 1h), then high (5m), then medium (1m)
    tf_priority = {'1d': 0, '4h': 1, '1h': 2, '5m': 3, '1m': 4}
    timeframes_sorted = sorted(timeframes, key=lambda tf: tf_priority.get(tf, 99))
    logger.info(f"Timeframe order: {timeframes_sorted}")

    for symbol in pairs:
        for timeframe in timeframes_sorted:
            try:
                logger.info(f"\n{'='*60}")
                logger.info(f"Fetching {symbol} {timeframe}")
                logger.info(f"{'='*60}")

                df = fetcher.fetch_ohlcv_history(
                    symbol=symbol,
                    timeframe=timeframe,
                    since=start_date,
                    until=end_date,
                    output_dir=output_dir,
                    resume=True
                )

                if not df.empty:
                    fetcher.save_to_parquet(df, output_dir)
                    total_fetched += len(df)
                    # Clean up checkpoint only after successful save
                    checkpoint = fetcher._get_checkpoint_path(symbol, timeframe, output_dir)
                    if checkpoint.exists():
                        checkpoint.unlink()

            except Exception as e:
                logger.error(f"Failed to fetch {symbol} {timeframe}: {e}")
                continue

    logger.info(f"\n{'='*60}")
    logger.info(f"OHLCV ingestion complete!")
    logger.info(f"Total rows fetched: {total_fetched:,}")
    logger.info(f"{'='*60}")


if __name__ == "__main__":
    main()
