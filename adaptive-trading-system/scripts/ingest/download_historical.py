#!/usr/bin/env python3
"""
Bulk Historical Data Downloader
Downloads pre-aggregated historical CSVs from free sources to bootstrap the dataset.
This is much faster than API pagination for initial backfill.

Sources:
1. CryptoDataDownload - Free CSV downloads (Binance spot data)
2. Kraken - Official historical data downloads
"""

import requests
import pandas as pd
from pathlib import Path
from loguru import logger
import time
from io import StringIO

# Configure logging
log_dir = Path(__file__).parent.parent.parent / 'logs'
log_dir.mkdir(exist_ok=True)
logger.add(
    log_dir / 'download_historical_{time:YYYY-MM-DD}.log',
    rotation='1 day',
    retention='30 days',
    level='INFO'
)


class HistoricalDownloader:
    """Downloads bulk historical data from free sources."""

    def __init__(self):
        """Initialize the downloader."""
        logger.info("Initialized Historical Data Downloader")

    def download_cryptodatadownload(
        self,
        exchange: str,
        symbol: str,
        timeframe: str
    ) -> pd.DataFrame:
        """
        Download from CryptoDataDownload.

        URL format: https://www.cryptodatadownload.com/cdd/{exchange}_{symbol}_{timeframe}.csv

        Args:
            exchange: Exchange name (e.g., 'Binance')
            symbol: Symbol (e.g., 'BTCUSDT')
            timeframe: Timeframe (e.g., '1h', 'd')

        Returns:
            DataFrame with OHLCV data
        """
        # CryptoDataDownload URL pattern
        # Example: https://www.cryptodatadownload.com/cdd/Binance_BTCUSDT_1h.csv

        base_url = "https://www.cryptodatadownload.com/cdd"
        filename = f"{exchange}_{symbol}_{timeframe}.csv"
        url = f"{base_url}/{filename}"

        logger.info(f"Attempting download from CryptoDataDownload: {filename}")

        try:
            response = requests.get(url, timeout=60)
            response.raise_for_status()

            # CryptoDataDownload CSVs have a header row - skip it
            csv_data = response.text
            lines = csv_data.split('\n')

            # Find where actual data starts (after header comments)
            data_start = 0
            for i, line in enumerate(lines):
                if line.startswith('unix,'):
                    data_start = i
                    break

            # Read CSV from data start
            df = pd.read_csv(StringIO('\n'.join(lines[data_start:])))

            # Normalize column names
            df.columns = df.columns.str.lower()

            # Convert timestamp
            if 'unix' in df.columns:
                df['timestamp'] = pd.to_datetime(df['unix'], unit='s', utc=True)
            elif 'date' in df.columns:
                df['timestamp'] = pd.to_datetime(df['date'], utc=True)

            # Standardize column names
            column_mapping = {
                'unix': 'timestamp',
                'open': 'open',
                'high': 'high',
                'low': 'low',
                'close': 'close',
                'volume': 'volume'
            }

            df = df.rename(columns=column_mapping)

            # Select relevant columns
            df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]

            # Add metadata
            df['symbol'] = f"{symbol[:-4]}/{symbol[-4:]}"  # BTCUSDT -> BTC/USDT
            df['exchange'] = exchange.lower()
            df['timeframe'] = timeframe

            # Sort by timestamp
            df = df.sort_values('timestamp').reset_index(drop=True)

            logger.info(f"✓ Downloaded {len(df)} rows from CryptoDataDownload")

            return df

        except requests.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(f"File not found on CryptoDataDownload: {filename}")
            else:
                logger.error(f"HTTP error downloading {filename}: {e}")
            return pd.DataFrame()

        except Exception as e:
            logger.error(f"Error downloading {filename}: {e}")
            return pd.DataFrame()

    def download_from_url(self, url: str, format: str = 'csv') -> pd.DataFrame:
        """
        Generic downloader for direct CSV/ZIP URLs.

        Args:
            url: Direct download URL
            format: File format ('csv' or 'zip')

        Returns:
            DataFrame with data
        """
        logger.info(f"Downloading from: {url}")

        try:
            response = requests.get(url, timeout=120)
            response.raise_for_status()

            if format == 'csv':
                df = pd.read_csv(StringIO(response.text))
            else:
                logger.error(f"Format {format} not supported yet")
                return pd.DataFrame()

            logger.info(f"✓ Downloaded {len(df)} rows")

            return df

        except Exception as e:
            logger.error(f"Error downloading from {url}: {e}")
            return pd.DataFrame()

    def save_to_parquet(self, df: pd.DataFrame, output_dir: Path, symbol: str, timeframe: str, exchange: str):
        """Save downloaded data to Parquet."""
        if df.empty:
            logger.warning("Empty DataFrame, skipping save")
            return

        # Create directory: data/raw/ohlcv/{exchange}/
        exchange_dir = output_dir / exchange
        exchange_dir.mkdir(parents=True, exist_ok=True)

        # Filename: {symbol}_{timeframe}.parquet (same as API fetcher for merging)
        safe_symbol = symbol.replace('/', '_')
        filename = f"{safe_symbol}_{timeframe}.parquet"
        filepath = exchange_dir / filename

        # Save as Parquet
        df.to_parquet(filepath, index=False, compression='snappy')

        file_size_mb = filepath.stat().st_size / (1024 * 1024)
        logger.info(f"✓ Saved to {filepath} ({file_size_mb:.2f} MB)")


def main():
    """
    Main function to download bulk historical data.

    NOTE: CryptoDataDownload and other free sources may not have all pairs/timeframes.
    This script is best-effort - it will download what's available and skip what's not.

    For complete coverage, use the API-based fetcher (fetch_ohlcv.py).
    """

    # Output directory
    output_dir = Path(__file__).parent.parent.parent / 'data' / 'raw' / 'ohlcv'

    # Initialize downloader
    downloader = HistoricalDownloader()

    logger.info(f"\n{'='*60}")
    logger.info("Starting Bulk Historical Download")
    logger.info(f"{'='*60}")

    # Define what to download from CryptoDataDownload
    # Format: (exchange, symbol_no_slash, timeframe)
    downloads = [
        ('Binance', 'BTCUSDT', 'd'),   # BTC daily
        ('Binance', 'BTCUSDT', '1h'),  # BTC hourly
        ('Binance', 'ETHUSDT', 'd'),   # ETH daily
        ('Binance', 'ETHUSDT', '1h'),  # ETH hourly
    ]

    total_downloaded = 0

    for exchange, symbol, timeframe in downloads:
        try:
            logger.info(f"\nDownloading {exchange} {symbol} {timeframe}...")

            df = downloader.download_cryptodatadownload(exchange, symbol, timeframe)

            if not df.empty:
                # Convert symbol format: BTCUSDT -> BTC/USDT
                formatted_symbol = f"{symbol[:3]}/{symbol[3:]}"

                downloader.save_to_parquet(
                    df,
                    output_dir,
                    formatted_symbol,
                    timeframe,
                    exchange.lower()
                )

                total_downloaded += len(df)

            # Rate limiting (be nice to the server)
            time.sleep(2)

        except Exception as e:
            logger.error(f"Failed to download {exchange} {symbol} {timeframe}: {e}")
            continue

    logger.info(f"\n{'='*60}")
    logger.info(f"Bulk Historical Download Complete!")
    logger.info(f"Total rows downloaded: {total_downloaded:,}")
    logger.info(f"\n⚠️  NOTE: Free sources may not have all pairs/timeframes.")
    logger.info("Use fetch_ohlcv.py to fill gaps and get complete coverage.")
    logger.info(f"{'='*60}")


if __name__ == "__main__":
    main()
