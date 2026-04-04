#!/usr/bin/env python3
"""
Master Validation Pipeline
Runs all 5 validators, generates the validation report,
fills small gaps, and copies passing data to data/validated/.
"""

import sys
import shutil
from pathlib import Path
from datetime import datetime, timezone
from loguru import logger

# Setup logging
logger.remove()
log_dir = Path(__file__).parent.parent.parent / 'logs'
log_dir.mkdir(exist_ok=True)
logger.add(sys.stdout, level='INFO')

# Import validators
sys.path.insert(0, str(Path(__file__).parent))
from validate_schema import run_schema_validation
from validate_range import run_range_validation
from validate_completeness import run_completeness_validation, fill_small_gaps, TIMEFRAME_FREQ
from validate_freshness import run_freshness_validation
from validate_cross_source import run_cross_source_validation

import pandas as pd


def generate_report(results: dict, report_path: Path):
    """Generate human-readable validation report."""
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')

    lines = []
    lines.append("=" * 70)
    lines.append("DATA VALIDATION REPORT")
    lines.append(f"Date: {now}")
    lines.append("=" * 70)
    lines.append("")

    # Schema
    schema = results['schema']
    lines.append(f"SCHEMA: {schema['passed']:,} / {schema['total_rows']:,} PASS ({schema['pass_rate']:.4f}%)")
    if schema['failed'] > 0:
        for d in schema['details']:
            if d['failed'] > 0:
                lines.append(f"  - {d['source']}: {d['failed']} rows failed — nulls: {d['null_details']}")
    lines.append("")

    # Range
    rng = results['range']
    lines.append(f"RANGE: {rng['passed']:,} / {rng['total_rows']:,} PASS ({rng['pass_rate']:.4f}%)")
    if rng['failed'] > 0:
        for d in rng['details']:
            if d['failed'] > 0:
                lines.append(f"  - {d['source']}: {d['violations']}")
    if rng.get('large_moves_flagged', 0) > 0:
        lines.append(f"  - {rng['large_moves_flagged']} rows flagged for >50% price move (review, not failures)")
        for d in rng['details']:
            for move in d.get('flagged_moves_sample', []):
                lines.append(f"    {move['symbol']} {move['timestamp']}: {move['prev_close']} → {move['close']} ({move['move_pct']}%)")
    lines.append("")

    # Completeness
    comp = results['completeness']
    lines.append("COMPLETENESS:")
    for d in comp['details']:
        status = f"  {d['symbol']} {d['timeframe']}: {d['actual']:,} / {d['expected']:,} ({d['completeness_pct']}%)"
        if d['missing'] > 0:
            status += f" — {d['missing']} missing"
            if d['small_gaps'] > 0:
                status += f", {d['small_gaps']} small gaps filled"
            if d['large_gaps'] > 0:
                status += f", {d['large_gaps']} LARGE gaps (not filled)"
        lines.append(status)
    lines.append("")

    # Freshness
    fresh = results['freshness']
    lines.append(f"FRESHNESS: {fresh['fresh']} / {fresh['total_sources']} sources fresh")
    stale = [d for d in fresh['details'] if not d['is_fresh']]
    if stale:
        for d in stale:
            lines.append(f"  - STALE: {d['source']} — {d['age_hours']}h old (threshold: {d['threshold_hours']}h)")
    lines.append("")

    # Cross-source
    cross = results['cross_source']
    lines.append(f"CROSS-SOURCE: {cross['exchanges_available']} exchange(s) available")
    for d in cross['details']:
        if 'max_deviation_pct' in d:
            status = "PASS" if d.get('pass', True) else "FAIL"
            lines.append(f"  {status} {d['symbol']}: max deviation {d['max_deviation_pct']}%")
        elif 'duplicate_timestamps' in d:
            lines.append(f"  {d['source']}: {d['duplicate_timestamps']} dupes, close-open gap max={d['close_open_max_diff_pct']}%")
    lines.append("")

    # Verdict
    lines.append("=" * 70)
    critical_failures = schema['failed'] + rng['failed']
    if critical_failures == 0:
        lines.append("VERDICT: DATA APPROVED FOR RESEARCH USE")
    else:
        lines.append(f"VERDICT: {critical_failures:,} CRITICAL FAILURES — REVIEW REQUIRED")
    lines.append("=" * 70)

    report_text = "\n".join(lines)

    # Write to file
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, 'w') as f:
        f.write(report_text)

    # Also print to stdout
    print("\n" + report_text)

    return report_text


def copy_to_validated(raw_dir: Path, validated_dir: Path):
    """
    Copy validated data to data/validated/, applying gap fills.
    """
    logger.info("\nCopying validated data to data/validated/...")
    validated_dir.mkdir(parents=True, exist_ok=True)

    # Copy and gap-fill OHLCV
    ohlcv_raw = raw_dir / 'ohlcv'
    if ohlcv_raw.exists():
        for parquet_file in sorted(ohlcv_raw.rglob('*.parquet')):
            relative = parquet_file.relative_to(raw_dir)
            dest = validated_dir / relative
            dest.parent.mkdir(parents=True, exist_ok=True)

            # Read, fill small gaps, save
            df = pd.read_parquet(parquet_file)
            parts = parquet_file.stem.split('_')
            timeframe = parts[-1]

            if timeframe in TIMEFRAME_FREQ:
                df = fill_small_gaps(df, timeframe)

            df.to_parquet(dest, index=False, compression='snappy')
            logger.info(f"  Validated: {relative} ({len(df):,} rows)")

    # Copy funding rates (no gap fill needed)
    funding_raw = raw_dir / 'funding_rates'
    if funding_raw.exists():
        for parquet_file in sorted(funding_raw.rglob('*.parquet')):
            relative = parquet_file.relative_to(raw_dir)
            dest = validated_dir / relative
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(parquet_file, dest)
            logger.info(f"  Validated: {relative}")

    # Copy sentiment (no gap fill needed)
    sentiment_raw = raw_dir / 'sentiment'
    if sentiment_raw.exists():
        for parquet_file in sorted(sentiment_raw.rglob('*.parquet')):
            relative = parquet_file.relative_to(raw_dir)
            dest = validated_dir / relative
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(parquet_file, dest)
            logger.info(f"  Validated: {relative}")

    logger.info("Validated data copy complete.")


def main():
    raw_dir = Path(__file__).parent.parent.parent / 'data' / 'raw'
    validated_dir = Path(__file__).parent.parent.parent / 'data' / 'validated'
    report_date = datetime.now(timezone.utc).strftime('%Y%m%d')
    report_path = log_dir / f'validation_report_{report_date}.txt'

    logger.info("#" * 70)
    logger.info("STARTING FULL VALIDATION PIPELINE")
    logger.info(f"Raw data: {raw_dir}")
    logger.info("#" * 70)

    # Run all 5 validators
    results = {}
    results['schema'] = run_schema_validation(raw_dir)
    results['range'] = run_range_validation(raw_dir)
    results['completeness'] = run_completeness_validation(raw_dir)
    results['freshness'] = run_freshness_validation(raw_dir)
    results['cross_source'] = run_cross_source_validation(raw_dir)

    # Generate report
    generate_report(results, report_path)
    logger.info(f"\nReport saved to: {report_path}")

    # Copy validated data (with gap fills)
    critical_failures = results['schema']['failed'] + results['range']['failed']
    if critical_failures == 0:
        copy_to_validated(raw_dir, validated_dir)
    else:
        logger.warning(f"\n{critical_failures:,} critical failures found. Fix before copying to validated/")
        logger.warning("Run individual validators to investigate.")

    logger.info("\nValidation pipeline complete.")


if __name__ == "__main__":
    main()
