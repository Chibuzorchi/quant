#!/usr/bin/env python3
"""
Sentiment Data Fetcher
Fetches Fear & Greed Index from alternative.me
No API key required - free public endpoint
"""

import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
from loguru import logger
import time

# Configure logging
log_dir = Path(__file__).parent.parent.parent / 'logs'
log_dir.mkdir(exist_ok=True)
logger.add(
    log_dir / 'sentiment_{time:YYYY-MM-DD}.log',
    rotation='1 day',
    retention='30 days',
    level='INFO'
)


class SentimentFetcher:
    """Fetches Fear & Greed Index from alternative.me"""

    def __init__(self):
        """Initialize the fetcher."""
        self.endpoint = "https://api.alternative.me/fng/"
        logger.info("Initialized Fear & Greed Index fetcher")

    def fetch_fear_greed_index(self, limit: int = 0) -> pd.DataFrame:
        """
        Fetch Fear & Greed Index.

        Args:
            limit: Number of results (0 = all historical data)

        Returns:
            DataFrame with Fear & Greed Index history
        """
        logger.info(f"Fetching Fear & Greed Index (limit={limit}, 0=all)")

        try:
            # Make request
            params = {
                'limit': limit,
                'format': 'json'
            }

            response = requests.get(self.endpoint, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()

            if 'data' not in data:
                logger.error("No data field in response")
                return pd.DataFrame()

            records = data['data']

            if not records:
                logger.warning("No records returned")
                return pd.DataFrame()

            # Convert to DataFrame
            df = pd.DataFrame(records)

            # Convert timestamp to datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'].astype(int), unit='s', utc=True)

            # Rename columns
            df = df.rename(columns={
                'value': 'value',
                'value_classification': 'classification'
            })

            # Convert value to integer
            df['value'] = df['value'].astype(int)

            # Select relevant columns
            df = df[['timestamp', 'value', 'classification']]

            # Sort by timestamp
            df = df.sort_values('timestamp').reset_index(drop=True)

            logger.info(f"✓ Fetched {len(df)} Fear & Greed Index records")
            logger.info(f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")

            return df

        except requests.RequestException as e:
            logger.error(f"Request error: {e}")
            return pd.DataFrame()

        except Exception as e:
            logger.error(f"Error fetching Fear & Greed Index: {e}")
            return pd.DataFrame()

    def save_to_parquet(self, df: pd.DataFrame, output_dir: Path):
        """Save Fear & Greed Index to Parquet."""
        if df.empty:
            logger.warning("Empty DataFrame, skipping save")
            return

        # Create directory: data/raw/sentiment/
        output_dir.mkdir(parents=True, exist_ok=True)

        # Filename: fear_greed_index.parquet
        filepath = output_dir / 'fear_greed_index.parquet'

        # Save as Parquet
        df.to_parquet(filepath, index=False, compression='snappy')

        file_size_mb = filepath.stat().st_size / (1024 * 1024)
        logger.info(f"✓ Saved to {filepath} ({file_size_mb:.2f} MB)")


def main():
    """Main function to fetch Fear & Greed Index."""

    # Output directory
    output_dir = Path(__file__).parent.parent.parent / 'data' / 'raw' / 'sentiment'

    # Initialize fetcher
    fetcher = SentimentFetcher()

    # Fetch all historical data (limit=0)
    logger.info(f"\n{'='*60}")
    logger.info("Fetching Fear & Greed Index (full history)")
    logger.info(f"{'='*60}")

    df = fetcher.fetch_fear_greed_index(limit=0)

    if not df.empty:
        fetcher.save_to_parquet(df, output_dir)

        # Display summary statistics
        logger.info("\nFear & Greed Index Summary:")
        logger.info(f"Total records: {len(df)}")
        logger.info(f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
        logger.info(f"\nValue distribution:")
        logger.info(f"  Mean: {df['value'].mean():.1f}")
        logger.info(f"  Median: {df['value'].median():.1f}")
        logger.info(f"  Min: {df['value'].min()}")
        logger.info(f"  Max: {df['value'].max()}")
        logger.info(f"\nClassification counts:")
        logger.info(df['classification'].value_counts().to_string())

    logger.info(f"\n{'='*60}")
    logger.info("Fear & Greed Index ingestion complete!")
    logger.info(f"{'='*60}")


if __name__ == "__main__":
    main()
