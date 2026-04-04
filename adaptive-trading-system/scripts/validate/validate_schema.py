#!/usr/bin/env python3
"""
Validator 1: Schema Validator
- Every OHLCV row must have all required fields non-null
- Types must match expected types
- Rejects and logs any row that fails
"""

import pandas as pd
from pathlib import Path
from loguru import logger
import sys

logger.remove()
logger.add(sys.stdout, level='INFO')

# Expected schemas
OHLCV_SCHEMA = {
    'timestamp': 'datetime64[ms, UTC]',
    'open': 'float64',
    'high': 'float64',
    'low': 'float64',
    'close': 'float64',
    'volume': 'float64',
    'symbol': 'str',
    'exchange': 'str',
    'timeframe': 'str',
}

OHLCV_REQUIRED = ['timestamp', 'open', 'high', 'low', 'close', 'volume']

FUNDING_SCHEMA = {
    'timestamp': 'datetime64[ms, UTC]',
    'symbol': 'str',
    'funding_rate': 'float64',
    'exchange': 'str',
}

FUNDING_REQUIRED = ['timestamp', 'symbol', 'funding_rate']

SENTIMENT_SCHEMA = {
    'timestamp': 'datetime64[ms, UTC]',
    'value': 'int64',
    'classification': 'str',
}

SENTIMENT_REQUIRED = ['timestamp', 'value', 'classification']


def validate_schema(df: pd.DataFrame, expected_columns: dict, required_fields: list, source_name: str) -> dict:
    """
    Validate DataFrame schema against expected columns and check for nulls.

    Returns dict with pass/fail counts and details.
    """
    result = {
        'source': source_name,
        'total_rows': len(df),
        'passed': 0,
        'failed': 0,
        'missing_columns': [],
        'type_mismatches': [],
        'null_rows': 0,
        'null_details': {},
    }

    # Check for missing columns
    for col in expected_columns:
        if col not in df.columns:
            result['missing_columns'].append(col)

    if result['missing_columns']:
        result['failed'] = len(df)
        logger.error(f"{source_name}: Missing columns: {result['missing_columns']}")
        return result

    # Check types (loose check — allow compatible types)
    for col, expected_type in expected_columns.items():
        actual_type = str(df[col].dtype)
        if expected_type == 'str' and actual_type not in ('object', 'str', 'string'):
            result['type_mismatches'].append(f"{col}: expected {expected_type}, got {actual_type}")
        elif expected_type == 'float64' and actual_type != 'float64':
            result['type_mismatches'].append(f"{col}: expected {expected_type}, got {actual_type}")
        elif expected_type == 'int64' and actual_type not in ('int64', 'int32'):
            result['type_mismatches'].append(f"{col}: expected {expected_type}, got {actual_type}")
        # datetime check — accept any datetime64 variant with UTC
        elif 'datetime64' in expected_type and 'datetime64' not in actual_type:
            result['type_mismatches'].append(f"{col}: expected {expected_type}, got {actual_type}")

    if result['type_mismatches']:
        logger.warning(f"{source_name}: Type mismatches: {result['type_mismatches']}")

    # Check nulls in required fields
    null_mask = df[required_fields].isnull().any(axis=1)
    result['null_rows'] = int(null_mask.sum())

    for col in required_fields:
        null_count = int(df[col].isnull().sum())
        if null_count > 0:
            result['null_details'][col] = null_count

    result['failed'] = result['null_rows']
    result['passed'] = result['total_rows'] - result['failed']

    return result


def validate_ohlcv_files(data_dir: Path) -> list:
    """Validate all OHLCV parquet files."""
    results = []
    ohlcv_dir = data_dir / 'ohlcv'

    if not ohlcv_dir.exists():
        logger.error(f"OHLCV directory not found: {ohlcv_dir}")
        return results

    for parquet_file in sorted(ohlcv_dir.rglob('*.parquet')):
        df = pd.read_parquet(parquet_file)
        source_name = f"ohlcv/{parquet_file.parent.name}/{parquet_file.name}"
        result = validate_schema(df, OHLCV_SCHEMA, OHLCV_REQUIRED, source_name)
        results.append(result)

        if result['failed'] > 0:
            logger.warning(f"  FAIL {source_name}: {result['failed']} rows with nulls")
        else:
            logger.info(f"  PASS {source_name}: {result['total_rows']} rows OK")

    return results


def validate_funding_files(data_dir: Path) -> list:
    """Validate all funding rate parquet files."""
    results = []
    funding_dir = data_dir / 'funding_rates'

    if not funding_dir.exists():
        logger.warning(f"Funding rates directory not found: {funding_dir}")
        return results

    for parquet_file in sorted(funding_dir.rglob('*.parquet')):
        df = pd.read_parquet(parquet_file)
        source_name = f"funding_rates/{parquet_file.parent.name}/{parquet_file.name}"
        result = validate_schema(df, FUNDING_SCHEMA, FUNDING_REQUIRED, source_name)
        results.append(result)

        if result['failed'] > 0:
            logger.warning(f"  FAIL {source_name}: {result['failed']} rows with nulls")
        else:
            logger.info(f"  PASS {source_name}: {result['total_rows']} rows OK")

    return results


def validate_sentiment_files(data_dir: Path) -> list:
    """Validate sentiment parquet files."""
    results = []
    sentiment_dir = data_dir / 'sentiment'

    if not sentiment_dir.exists():
        logger.warning(f"Sentiment directory not found: {sentiment_dir}")
        return results

    for parquet_file in sorted(sentiment_dir.rglob('*.parquet')):
        df = pd.read_parquet(parquet_file)
        source_name = f"sentiment/{parquet_file.name}"
        result = validate_schema(df, SENTIMENT_SCHEMA, SENTIMENT_REQUIRED, source_name)
        results.append(result)

        if result['failed'] > 0:
            logger.warning(f"  FAIL {source_name}: {result['failed']} rows with nulls")
        else:
            logger.info(f"  PASS {source_name}: {result['total_rows']} rows OK")

    return results


def run_schema_validation(data_dir: Path) -> dict:
    """Run full schema validation. Returns summary dict."""
    logger.info("=" * 60)
    logger.info("SCHEMA VALIDATION")
    logger.info("=" * 60)

    all_results = []
    all_results.extend(validate_ohlcv_files(data_dir))
    all_results.extend(validate_funding_files(data_dir))
    all_results.extend(validate_sentiment_files(data_dir))

    total_rows = sum(r['total_rows'] for r in all_results)
    total_passed = sum(r['passed'] for r in all_results)
    total_failed = sum(r['failed'] for r in all_results)

    summary = {
        'validator': 'schema',
        'total_rows': total_rows,
        'passed': total_passed,
        'failed': total_failed,
        'pass_rate': total_passed / total_rows * 100 if total_rows > 0 else 0,
        'details': all_results,
    }

    logger.info(f"\nSCHEMA: {total_passed:,} / {total_rows:,} PASS ({summary['pass_rate']:.4f}%)")
    if total_failed > 0:
        logger.warning(f"  {total_failed:,} rows failed schema validation")

    return summary


if __name__ == "__main__":
    data_dir = Path(__file__).parent.parent.parent / 'data' / 'raw'
    run_schema_validation(data_dir)
