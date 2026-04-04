#!/usr/bin/env python3
"""
Validator 2: Range Validator
- high >= low (always)
- high >= open and high >= close (always)
- low <= open and low <= close (always)
- volume >= 0 (always)
- Price within +/-50% of previous close (flag, don't reject)
- No zero prices
"""

import pandas as pd
import numpy as np
from pathlib import Path
from loguru import logger
import sys

logger.remove()
logger.add(sys.stdout, level='INFO')


def validate_range(df: pd.DataFrame, source_name: str) -> dict:
    """
    Validate OHLCV price/volume ranges.

    Returns dict with pass/fail counts, flagged rows, and details.
    """
    total = len(df)
    violations = []
    flagged_moves = []

    # high >= low
    mask_hl = df['high'] < df['low']
    if mask_hl.any():
        count = int(mask_hl.sum())
        violations.append(f"high < low: {count} rows")

    # high >= open
    mask_ho = df['high'] < df['open']
    if mask_ho.any():
        count = int(mask_ho.sum())
        violations.append(f"high < open: {count} rows")

    # high >= close
    mask_hc = df['high'] < df['close']
    if mask_hc.any():
        count = int(mask_hc.sum())
        violations.append(f"high < close: {count} rows")

    # low <= open
    mask_lo = df['low'] > df['open']
    if mask_lo.any():
        count = int(mask_lo.sum())
        violations.append(f"low > open: {count} rows")

    # low <= close
    mask_lc = df['low'] > df['close']
    if mask_lc.any():
        count = int(mask_lc.sum())
        violations.append(f"low > close: {count} rows")

    # volume >= 0
    mask_vol = df['volume'] < 0
    if mask_vol.any():
        count = int(mask_vol.sum())
        violations.append(f"negative volume: {count} rows")

    # No zero prices
    price_cols = ['open', 'high', 'low', 'close']
    mask_zero = (df[price_cols] == 0).any(axis=1)
    if mask_zero.any():
        count = int(mask_zero.sum())
        violations.append(f"zero prices: {count} rows")

    # All hard violations combined
    all_violation_masks = mask_hl | mask_ho | mask_hc | mask_lo | mask_lc | mask_vol | mask_zero
    hard_failures = int(all_violation_masks.sum())

    # Price move > 50% from previous close (flag only, not a failure)
    df_sorted = df.sort_values('timestamp')
    prev_close = df_sorted['close'].shift(1)
    pct_change = ((df_sorted['close'] - prev_close) / prev_close).abs()
    large_moves = pct_change > 0.5
    large_move_count = int(large_moves.sum())

    if large_move_count > 0:
        # Capture details of large moves for the report
        large_move_rows = df_sorted[large_moves]
        for _, row in large_move_rows.iterrows():
            idx = large_move_rows.index.get_loc(_) if hasattr(large_move_rows.index, 'get_loc') else 0
            prev_idx = df_sorted.index.get_loc(_) - 1
            if prev_idx >= 0:
                prev = df_sorted.iloc[prev_idx]['close']
                move_pct = abs((row['close'] - prev) / prev) * 100
                flagged_moves.append({
                    'timestamp': str(row['timestamp']),
                    'symbol': row.get('symbol', 'unknown'),
                    'prev_close': round(prev, 4),
                    'close': round(row['close'], 4),
                    'move_pct': round(move_pct, 2),
                })

    result = {
        'source': source_name,
        'total_rows': total,
        'passed': total - hard_failures,
        'failed': hard_failures,
        'violations': violations,
        'large_move_flags': large_move_count,
        'flagged_moves_sample': flagged_moves[:10],  # Cap at 10 for readability
    }

    return result


def run_range_validation(data_dir: Path) -> dict:
    """Run range validation on all OHLCV files."""
    logger.info("=" * 60)
    logger.info("RANGE VALIDATION")
    logger.info("=" * 60)

    ohlcv_dir = data_dir / 'ohlcv'
    all_results = []

    if not ohlcv_dir.exists():
        logger.error(f"OHLCV directory not found: {ohlcv_dir}")
        return {'validator': 'range', 'total_rows': 0, 'passed': 0, 'failed': 0, 'pass_rate': 0}

    for parquet_file in sorted(ohlcv_dir.rglob('*.parquet')):
        df = pd.read_parquet(parquet_file)
        source_name = f"{parquet_file.parent.name}/{parquet_file.name}"
        result = validate_range(df, source_name)
        all_results.append(result)

        if result['failed'] > 0:
            logger.warning(f"  FAIL {source_name}: {result['failed']} violations — {result['violations']}")
        else:
            status = f"PASS {source_name}: {result['total_rows']} rows OK"
            if result['large_move_flags'] > 0:
                status += f" ({result['large_move_flags']} large moves flagged)"
            logger.info(f"  {status}")

    total_rows = sum(r['total_rows'] for r in all_results)
    total_passed = sum(r['passed'] for r in all_results)
    total_failed = sum(r['failed'] for r in all_results)
    total_flagged = sum(r['large_move_flags'] for r in all_results)

    summary = {
        'validator': 'range',
        'total_rows': total_rows,
        'passed': total_passed,
        'failed': total_failed,
        'pass_rate': total_passed / total_rows * 100 if total_rows > 0 else 0,
        'large_moves_flagged': total_flagged,
        'details': all_results,
    }

    logger.info(f"\nRANGE: {total_passed:,} / {total_rows:,} PASS ({summary['pass_rate']:.4f}%)")
    if total_failed > 0:
        logger.warning(f"  {total_failed:,} rows with range violations")
    if total_flagged > 0:
        logger.info(f"  {total_flagged:,} rows flagged for >50% price move (review, not failures)")

    return summary


if __name__ == "__main__":
    data_dir = Path(__file__).parent.parent.parent / 'data' / 'raw'
    run_range_validation(data_dir)
