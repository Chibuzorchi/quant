#!/usr/bin/env python3
"""
Validator 5: Cross-Source Validator
- Compare BTC/USDT and ETH/USDT daily close prices across exchanges
- Daily close should be within 0.5% (arbitrage keeps them close)
- Flag deviation > 1% as likely data corruption
- Note: Requires data from 2+ exchanges. If only 1 exchange available,
  performs internal consistency checks instead.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from loguru import logger
import sys

logger.remove()
logger.add(sys.stdout, level='INFO')


def compare_exchanges(df_primary: pd.DataFrame, df_secondary: pd.DataFrame, symbol: str) -> dict:
    """
    Compare daily close prices between two exchange sources.

    Returns deviation analysis.
    """
    # Resample to daily close if not already daily
    p = df_primary.set_index('timestamp')['close'].resample('D').last().dropna()
    s = df_secondary.set_index('timestamp')['close'].resample('D').last().dropna()

    # Align on common dates
    common = p.index.intersection(s.index)
    if len(common) == 0:
        return {
            'symbol': symbol,
            'status': 'no_overlap',
            'common_days': 0,
        }

    p_aligned = p.loc[common]
    s_aligned = s.loc[common]

    # Calculate percentage deviation
    deviation = ((p_aligned - s_aligned) / p_aligned * 100).abs()

    result = {
        'symbol': symbol,
        'status': 'compared',
        'common_days': len(common),
        'mean_deviation_pct': round(float(deviation.mean()), 4),
        'max_deviation_pct': round(float(deviation.max()), 4),
        'median_deviation_pct': round(float(deviation.median()), 4),
        'days_over_0_5_pct': int((deviation > 0.5).sum()),
        'days_over_1_pct': int((deviation > 1.0).sum()),
        'pass': float(deviation.max()) < 1.0,
    }

    # Details on worst deviations
    worst = deviation.nlargest(5)
    result['worst_days'] = [
        {'date': str(d.date()), 'deviation_pct': round(float(v), 4)}
        for d, v in worst.items()
    ]

    return result


def internal_consistency_check(df: pd.DataFrame, source_name: str) -> dict:
    """
    When only one exchange is available, check internal consistency:
    - Close of candle N should equal open of candle N+1 (within tolerance for daily)
    - No duplicate timestamps
    """
    df_sorted = df.sort_values('timestamp')

    # Check for duplicate timestamps
    dupes = df_sorted['timestamp'].duplicated().sum()

    # Check close-to-open continuity (for 1d candles)
    prev_close = df_sorted['close'].shift(1)
    open_diff = ((df_sorted['open'] - prev_close) / prev_close * 100).abs().dropna()

    return {
        'source': source_name,
        'duplicate_timestamps': int(dupes),
        'close_open_mean_diff_pct': round(float(open_diff.mean()), 4),
        'close_open_max_diff_pct': round(float(open_diff.max()), 4),
        'rows_checked': len(df_sorted),
    }


def run_cross_source_validation(data_dir: Path) -> dict:
    """Run cross-source validation."""
    logger.info("=" * 60)
    logger.info("CROSS-SOURCE VALIDATION")
    logger.info("=" * 60)

    ohlcv_dir = data_dir / 'ohlcv'
    all_results = []

    if not ohlcv_dir.exists():
        logger.error(f"OHLCV directory not found: {ohlcv_dir}")
        return {'validator': 'cross_source', 'details': []}

    # Find all available exchanges (skip hidden/temp dirs)
    exchanges = [d.name for d in ohlcv_dir.iterdir() if d.is_dir() and not d.name.startswith('.')]
    logger.info(f"Available exchanges: {exchanges}")

    cross_check_symbols = ['BTC_USDT', 'ETH_USDT']

    if len(exchanges) >= 2:
        # Cross-exchange comparison
        primary = exchanges[0]
        secondary = exchanges[1]
        logger.info(f"Comparing {primary} vs {secondary}")

        for symbol_safe in cross_check_symbols:
            primary_file = ohlcv_dir / primary / f"{symbol_safe}_1d.parquet"
            secondary_file = ohlcv_dir / secondary / f"{symbol_safe}_1d.parquet"

            if not primary_file.exists() or not secondary_file.exists():
                logger.warning(f"  SKIP {symbol_safe}: missing file on one exchange")
                continue

            df_p = pd.read_parquet(primary_file)
            df_s = pd.read_parquet(secondary_file)
            symbol = symbol_safe.replace('_', '/')

            result = compare_exchanges(df_p, df_s, symbol)
            all_results.append(result)

            if result['status'] == 'compared':
                status = "PASS" if result['pass'] else "FAIL"
                logger.info(f"  {status} {symbol}: max deviation {result['max_deviation_pct']}%, "
                           f"mean {result['mean_deviation_pct']}% over {result['common_days']} days")
                if result['days_over_1_pct'] > 0:
                    logger.warning(f"    {result['days_over_1_pct']} days with >1% deviation — possible data issue")
    else:
        # Single exchange — internal consistency only
        logger.info(f"Only 1 exchange ({exchanges[0]}), running internal consistency checks")

        for symbol_safe in cross_check_symbols:
            daily_file = ohlcv_dir / exchanges[0] / f"{symbol_safe}_1d.parquet"
            if not daily_file.exists():
                continue

            df = pd.read_parquet(daily_file)
            source_name = f"{exchanges[0]}/{symbol_safe}_1d"
            result = internal_consistency_check(df, source_name)
            all_results.append(result)

            logger.info(f"  {source_name}: {result['duplicate_timestamps']} dupes, "
                       f"close-open gap mean={result['close_open_mean_diff_pct']}%, "
                       f"max={result['close_open_max_diff_pct']}%")

    summary = {
        'validator': 'cross_source',
        'exchanges_available': len(exchanges),
        'details': all_results,
    }

    return summary


if __name__ == "__main__":
    data_dir = Path(__file__).parent.parent.parent / 'data' / 'raw'
    run_cross_source_validation(data_dir)
