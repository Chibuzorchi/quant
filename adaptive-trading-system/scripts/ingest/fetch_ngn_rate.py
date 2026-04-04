#!/usr/bin/env python3
"""
NGN/USD Exchange Rate Fetcher
Fetches daily NGN/USD rates for tracking real purchasing power.
Uses exchangerate-api.com (free tier: 1500 requests/month)
"""

import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone, timedelta
from loguru import logger
import time

# Configure logging
log_dir = Path(__file__).parent.parent.parent / 'logs'
log_dir.mkdir(exist_ok=True)
logger.add(
    log_dir / 'ngn_rate_{time:YYYY-MM-DD}.log',
    rotation='1 day',
    retention='30 days',
    level='INFO'
)


class NGNRateFetcher:
    """Fetches NGN/USD exchange rates."""

    def __init__(self):
        """Initialize the fetcher."""
        # Using free endpoint (no API key required for basic access)
        self.endpoint = "https://api.exchangerate-api.com/v4/latest/USD"
        logger.info("Initialized NGN/USD rate fetcher")

    def fetch_current_rate(self) -> dict:
        """Fetch current USD exchange rates."""
        try:
            response = requests.get(self.endpoint, timeout=30)
            response.raise_for_status()
            data = response.json()
            return data
        except Exception as e:
            logger.error(f"Error fetching current rate: {e}")
            return None

    def fetch_historical_rates(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Fetch historical NGN/USD rates day by day.

        Uses frankfurter.app (free, no key required, ECB data) as primary source.
        Falls back to fetching current rate only if historical source fails.

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD or 'present')

        Returns:
            DataFrame with NGN/USD rates
        """
        logger.info(f"Fetching NGN/USD rates from {start_date} to {end_date}")

        if end_date == 'present':
            end_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')

        # Try frankfurter.app for historical data (free, no API key, ECB source)
        # Note: ECB doesn't track NGN directly. Fall back to exchangerate-api history endpoint.
        # Alternative: Use the free exchangerate.host timeseries endpoint
        try:
            url = f"https://api.frankfurter.app/{start_date}..{end_date}"
            params = {'from': 'USD', 'to': 'NGN'}
            response = requests.get(url, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()

            if 'rates' in data and data['rates']:
                records = []
                for date_str, rates in data['rates'].items():
                    if 'NGN' in rates:
                        records.append({
                            'timestamp': pd.Timestamp(date_str, tz='UTC'),
                            'usd_ngn_rate': rates['NGN'],
                            'source': 'frankfurter.app',
                            'rate_type': 'official'
                        })

                if records:
                    df = pd.DataFrame(records)
                    df = df.sort_values('timestamp').reset_index(drop=True)
                    logger.info(f"✓ Fetched {len(df)} historical NGN/USD rates from frankfurter.app")
                    return df

            logger.warning("frankfurter.app did not return NGN rates (ECB may not track NGN)")
        except Exception as e:
            logger.warning(f"frankfurter.app failed: {e}")

        # Fallback: fetch current rate only (historical will build over daily runs)
        logger.warning("Historical NGN/USD data unavailable from free APIs. Fetching current rate only.")
        logger.warning("ACTION REQUIRED: Download historical NGN/USD CSV from investing.com or CBN website")

        data = self.fetch_current_rate()
        if not data:
            return pd.DataFrame()

        ngn_rate = data.get('rates', {}).get('NGN')
        if not ngn_rate:
            logger.error("NGN rate not found in response")
            return pd.DataFrame()

        df = pd.DataFrame([{
            'timestamp': pd.Timestamp.now(tz='UTC').normalize(),
            'usd_ngn_rate': ngn_rate,
            'source': 'exchangerate-api.com',
            'rate_type': 'official'
        }])

        logger.info(f"✓ Current NGN/USD rate: {ngn_rate:.2f}")
        return df

    def append_daily_rate(self, output_file: Path):
        """
        Append today's rate to existing historical data.

        Args:
            output_file: Path to existing Parquet file
        """
        # Fetch current rate
        data = self.fetch_current_rate()

        if not data:
            logger.error("Failed to fetch current rate")
            return

        ngn_rate = data.get('rates', {}).get('NGN')

        if not ngn_rate:
            logger.error("NGN rate not found in response")
            return

        # Create new record
        new_record = pd.DataFrame([{
            'timestamp': pd.Timestamp.now(tz='UTC').normalize(),  # Daily granularity
            'usd_ngn_rate': ngn_rate,
            'source': 'exchangerate-api.com',
            'rate_type': 'official'
        }])

        # Load existing data if exists
        if output_file.exists():
            existing_df = pd.read_parquet(output_file)

            # Check if today's rate already exists
            today = pd.Timestamp.now(tz='UTC').normalize()
            if today in existing_df['timestamp'].values:
                logger.info("Today's rate already exists, updating...")
                existing_df = existing_df[existing_df['timestamp'] != today]

            # Append new record
            df = pd.concat([existing_df, new_record], ignore_index=True)
        else:
            df = new_record

        # Sort by timestamp
        df = df.sort_values('timestamp').reset_index(drop=True)

        # Save
        df.to_parquet(output_file, index=False, compression='snappy')

        logger.info(f"✓ Appended NGN/USD rate: {ngn_rate:.2f} ({output_file})")

    def save_to_parquet(self, df: pd.DataFrame, output_dir: Path):
        """Save NGN/USD rates to Parquet."""
        if df.empty:
            logger.warning("Empty DataFrame, skipping save")
            return

        # Create directory: data/raw/currency_rates/
        output_dir.mkdir(parents=True, exist_ok=True)

        # Filename: ngn_usd_rates.parquet
        filepath = output_dir / 'ngn_usd_rates.parquet'

        # Save as Parquet
        df.to_parquet(filepath, index=False, compression='snappy')

        file_size_mb = filepath.stat().st_size / (1024 * 1024)
        logger.info(f"✓ Saved to {filepath} ({file_size_mb:.4f} MB)")


def main():
    """Main function to fetch NGN/USD rates."""

    # Output directory
    output_dir = Path(__file__).parent.parent.parent / 'data' / 'raw' / 'currency_rates'
    output_file = output_dir / 'ngn_usd_rates.parquet'

    # Initialize fetcher
    fetcher = NGNRateFetcher()

    logger.info(f"\n{'='*60}")
    logger.info("Fetching NGN/USD Exchange Rate")
    logger.info(f"{'='*60}")

    # Check if historical data exists
    if output_file.exists():
        logger.info("Historical data exists, appending today's rate")
        fetcher.append_daily_rate(output_file)
    else:
        logger.info("No historical data, creating new file with current rate")
        df = fetcher.fetch_historical_rates('2023-01-01', 'present')

        if not df.empty:
            fetcher.save_to_parquet(df, output_dir)

            logger.info("\n⚠️  NOTE: Only current rate fetched.")
            logger.info("For full historical NGN/USD data:")
            logger.info("1. Use https://www.exchangerate-api.com (paid tier for history)")
            logger.info("2. Or manually download CSV from https://www.investing.com/currencies/usd-ngn-historical-data")
            logger.info("3. Or use CBN official rates: https://www.cbn.gov.ng/rates/")
            logger.info("\nFor now, daily updates will build historical data going forward.")

    logger.info(f"\n{'='*60}")
    logger.info("NGN/USD rate ingestion complete!")
    logger.info(f"{'='*60}")


if __name__ == "__main__":
    main()
