#!/usr/bin/env python3
"""
Validator 4: Freshness Validator
- Check that the most recent data point is within expected recency
- Alert if data appears stale
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, timezone, timedelta
from loguru import logger
import sys

logger.remove()
logger.add(sys.stdout, level='INFO')

# Maximum staleness thresholds per timeframe
FRESHNESS_THRESHOLDS = {
    '1m': timedelta(hours=1),
    '5m': timedelta(hours=2),
    '1h': timedelta(hours=4),
    '4h': timedelta(hours=12),
    '1d': timedelta(days=2),
}

# For non-OHLCV sources
OTHER_THRESHOLDS = {
    'funding_rates': timedelta(hours=24),
    'sentiment': timedelta(days=2),
}


def check_freshness(df: pd.DataFrame, source_name: str, max_staleness: timedelta) -> dict:
    """Check if the most recent data point is fresh enough."""
    now = datetime.now(timezone.utc)
    latest = df['timestamp'].max()

    # Handle both timezone-aware and naive timestamps
    if latest.tzinfo is None:
        latest = latest.tz_localize('UTC')

    age = now - latest
    is_fresh = age <= max_staleness

    return {
        'source': source_name,
        'latest_timestamp': str(latest),
        'age_hours': round(age.total_seconds() / 3600, 1),
        'threshold_hours': round(max_staleness.total_seconds() / 3600, 1),
        'is_fresh': is_fresh,
    }


def run_freshness_validation(data_dir: Path) -> dict:
    """Run freshness validation on all data files."""
    logger.info("=" * 60)
    logger.info("FRESHNESS VALIDATION")
    logger.info("=" * 60)

    all_results = []

    # Check OHLCV files
    ohlcv_dir = data_dir / 'ohlcv'
    if ohlcv_dir.exists():
        for parquet_file in sorted(ohlcv_dir.rglob('*.parquet')):
            df = pd.read_parquet(parquet_file)
            source_name = f"ohlcv/{parquet_file.parent.name}/{parquet_file.name}"

            parts = parquet_file.stem.split('_')
            timeframe = parts[-1]
            threshold = FRESHNESS_THRESHOLDS.get(timeframe, timedelta(days=2))

            result = check_freshness(df, source_name, threshold)
            all_results.append(result)

            status = "FRESH" if result['is_fresh'] else "STALE"
            logger.info(f"  {status} {source_name}: {result['age_hours']}h old (threshold: {result['threshold_hours']}h)")

    # Check funding rates
    funding_dir = data_dir / 'funding_rates'
    if funding_dir.exists():
        for parquet_file in sorted(funding_dir.rglob('*.parquet')):
            df = pd.read_parquet(parquet_file)
            source_name = f"funding_rates/{parquet_file.parent.name}/{parquet_file.name}"
            result = check_freshness(df, source_name, OTHER_THRESHOLDS['funding_rates'])
            all_results.append(result)

            status = "FRESH" if result['is_fresh'] else "STALE"
            logger.info(f"  {status} {source_name}: {result['age_hours']}h old")

    # Check sentiment
    sentiment_dir = data_dir / 'sentiment'
    if sentiment_dir.exists():
        for parquet_file in sorted(sentiment_dir.rglob('*.parquet')):
            df = pd.read_parquet(parquet_file)
            source_name = f"sentiment/{parquet_file.name}"
            result = check_freshness(df, source_name, OTHER_THRESHOLDS['sentiment'])
            all_results.append(result)

            status = "FRESH" if result['is_fresh'] else "STALE"
            logger.info(f"  {status} {source_name}: {result['age_hours']}h old")

    fresh_count = sum(1 for r in all_results if r['is_fresh'])
    stale_count = len(all_results) - fresh_count

    summary = {
        'validator': 'freshness',
        'total_sources': len(all_results),
        'fresh': fresh_count,
        'stale': stale_count,
        'details': all_results,
    }

    logger.info(f"\nFRESHNESS: {fresh_count} / {len(all_results)} sources fresh")
    if stale_count > 0:
        logger.warning(f"  {stale_count} sources are stale — may need re-ingestion")

    return summary


if __name__ == "__main__":
    data_dir = Path(__file__).parent.parent.parent / 'data' / 'raw'
    run_freshness_validation(data_dir)
