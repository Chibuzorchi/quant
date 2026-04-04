#!/usr/bin/env python3
"""
Validator 3: Completeness Validator
- Count expected vs actual candles for each pair/timeframe
- Report completeness percentage
- Flag gaps > 3 consecutive missing candles
- Fill small gaps (1-2 candles) with previous close
"""

import pandas as pd
import numpy as np
from pathlib import Path
from loguru import logger
import sys

logger.remove()
logger.add(sys.stdout, level='INFO')

# Timeframe to pandas frequency mapping
TIMEFRAME_FREQ = {
    '1m': '1min',
    '5m': '5min',
    '1h': '1h',
    '4h': '4h',
    '1d': '1D',
}


def analyze_gaps(df: pd.DataFrame, timeframe: str) -> dict:
    """
    Analyze gaps in time series data.

    Returns gap analysis with counts and details.
    """
    freq = TIMEFRAME_FREQ.get(timeframe)
    if not freq:
        return {'error': f'Unknown timeframe: {timeframe}'}

    df_sorted = df.sort_values('timestamp')
    ts = df_sorted['timestamp']

    # Calculate expected number of candles
    start = ts.min()
    end = ts.max()
    expected_index = pd.date_range(start=start, end=end, freq=freq)
    expected_count = len(expected_index)
    actual_count = len(df_sorted)

    # Find missing timestamps
    actual_set = set(ts.dt.floor(freq))
    expected_set = set(expected_index)
    missing = sorted(expected_set - actual_set)

    # Group consecutive missing candles into gaps
    gaps = []
    if missing:
        gap_start = missing[0]
        gap_length = 1
        for i in range(1, len(missing)):
            delta = missing[i] - missing[i - 1]
            expected_delta = pd.Timedelta(freq)
            if delta <= expected_delta * 1.5:  # Allow small jitter
                gap_length += 1
            else:
                gaps.append({
                    'start': str(gap_start),
                    'length': gap_length,
                    'severity': 'small' if gap_length <= 2 else ('medium' if gap_length <= 10 else 'large'),
                })
                gap_start = missing[i]
                gap_length = 1
        # Last gap
        gaps.append({
            'start': str(gap_start),
            'length': gap_length,
            'severity': 'small' if gap_length <= 2 else ('medium' if gap_length <= 10 else 'large'),
        })

    completeness = actual_count / expected_count * 100 if expected_count > 0 else 0

    return {
        'expected': expected_count,
        'actual': actual_count,
        'missing': len(missing),
        'completeness_pct': round(completeness, 2),
        'total_gaps': len(gaps),
        'small_gaps': sum(1 for g in gaps if g['severity'] == 'small'),
        'medium_gaps': sum(1 for g in gaps if g['severity'] == 'medium'),
        'large_gaps': sum(1 for g in gaps if g['severity'] == 'large'),
        'gaps': gaps[:20],  # Cap detail output
    }


def fill_small_gaps(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    """
    Fill gaps of 1-2 candles using forward-fill.
    Gaps of 1-2: fill with previous close as OHLC, volume=0.
    Adds 'interpolated' column to flag filled rows.
    """
    freq = TIMEFRAME_FREQ.get(timeframe)
    if not freq:
        return df

    df_sorted = df.sort_values('timestamp').copy()
    df_sorted['interpolated'] = False

    ts = df_sorted['timestamp']
    start = ts.min()
    end = ts.max()
    expected_index = pd.date_range(start=start, end=end, freq=freq)

    actual_set = set(ts)
    missing = sorted(set(expected_index) - actual_set)

    if not missing:
        return df_sorted

    # Group consecutive missing into gaps
    fill_timestamps = []
    gap_start_idx = 0
    i = 0
    while i < len(missing):
        gap_length = 1
        while i + gap_length < len(missing):
            delta = missing[i + gap_length] - missing[i + gap_length - 1]
            expected_delta = pd.Timedelta(freq)
            if delta <= expected_delta * 1.5:
                gap_length += 1
            else:
                break
        # Only fill gaps of 1-2 candles
        if gap_length <= 2:
            fill_timestamps.extend(missing[i:i + gap_length])
        i += gap_length

    if not fill_timestamps:
        return df_sorted

    # Get metadata from existing rows
    symbol = df_sorted['symbol'].iloc[0]
    exchange = df_sorted['exchange'].iloc[0]
    tf = df_sorted['timeframe'].iloc[0]

    # For each fill timestamp, find the previous candle's close
    fill_rows = []
    for fill_ts in fill_timestamps:
        prev_rows = df_sorted[df_sorted['timestamp'] < fill_ts]
        if prev_rows.empty:
            continue
        prev_close = prev_rows.iloc[-1]['close']
        fill_rows.append({
            'timestamp': fill_ts,
            'open': prev_close,
            'high': prev_close,
            'low': prev_close,
            'close': prev_close,
            'volume': 0.0,
            'symbol': symbol,
            'exchange': exchange,
            'timeframe': tf,
            'interpolated': True,
        })

    if fill_rows:
        fill_df = pd.DataFrame(fill_rows)
        df_sorted = pd.concat([df_sorted, fill_df], ignore_index=True)
        df_sorted = df_sorted.sort_values('timestamp').reset_index(drop=True)

    return df_sorted


def run_completeness_validation(data_dir: Path) -> dict:
    """Run completeness validation on all OHLCV files."""
    logger.info("=" * 60)
    logger.info("COMPLETENESS VALIDATION")
    logger.info("=" * 60)

    ohlcv_dir = data_dir / 'ohlcv'
    all_results = []

    if not ohlcv_dir.exists():
        logger.error(f"OHLCV directory not found: {ohlcv_dir}")
        return {'validator': 'completeness', 'details': []}

    for parquet_file in sorted(ohlcv_dir.rglob('*.parquet')):
        df = pd.read_parquet(parquet_file)
        source_name = f"{parquet_file.parent.name}/{parquet_file.name}"

        # Extract timeframe from filename (e.g., BTC_USDT_1h.parquet -> 1h)
        parts = parquet_file.stem.split('_')
        timeframe = parts[-1]  # Last part is timeframe

        if timeframe not in TIMEFRAME_FREQ:
            logger.warning(f"  SKIP {source_name}: unknown timeframe '{timeframe}'")
            continue

        gap_analysis = analyze_gaps(df, timeframe)

        result = {
            'source': source_name,
            'symbol': df['symbol'].iloc[0] if len(df) > 0 else 'unknown',
            'timeframe': timeframe,
            **gap_analysis,
        }
        all_results.append(result)

        status = f"{result['symbol']} {timeframe}: {result['actual']:,} / {result['expected']:,} ({result['completeness_pct']}%)"
        if result['missing'] > 0:
            status += f" — {result['missing']} missing"
            if result['large_gaps'] > 0:
                status += f" ({result['large_gaps']} large gaps)"
        logger.info(f"  {status}")

    summary = {
        'validator': 'completeness',
        'details': all_results,
    }

    return summary


if __name__ == "__main__":
    data_dir = Path(__file__).parent.parent.parent / 'data' / 'raw'
    run_completeness_validation(data_dir)
