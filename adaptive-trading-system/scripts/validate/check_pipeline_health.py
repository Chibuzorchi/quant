#!/usr/bin/env python3
"""
Pipeline Health Checker
Monitors that the daily update pipeline is running correctly.

Checks:
1. Did the daily update run in the last 26 hours?
2. Is the data fresh (most recent candle within expected recency)?
3. Are there any validation failures?
4. Is disk usage reasonable?
"""

import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta
from loguru import logger

logger.remove()
logger.add(sys.stdout, level='INFO')

PROJECT_ROOT = Path(__file__).parent.parent.parent


def check_daily_update_ran() -> dict:
    """Check if daily_update.py ran in the last 26 hours."""
    log_dir = PROJECT_ROOT / 'logs'
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')

    today_log = log_dir / f'daily_update_{today}.log'
    yesterday_log = log_dir / f'daily_update_{yesterday}.log'

    ran_recently = today_log.exists() or yesterday_log.exists()

    if ran_recently:
        # Check the most recent log's modification time
        log_file = today_log if today_log.exists() else yesterday_log
        mod_time = datetime.fromtimestamp(log_file.stat().st_mtime, tz=timezone.utc)
        age_hours = (datetime.now(timezone.utc) - mod_time).total_seconds() / 3600
    else:
        age_hours = None

    return {
        'check': 'daily_update_ran',
        'pass': ran_recently and (age_hours is not None and age_hours < 26),
        'detail': f"Last log: {age_hours:.1f}h ago" if age_hours else "No recent daily update log found",
    }


def check_data_freshness() -> dict:
    """Check that key data files are fresh."""
    import pandas as pd

    validated_dir = PROJECT_ROOT / 'data' / 'validated' / 'ohlcv'
    if not validated_dir.exists():
        return {'check': 'data_freshness', 'pass': False, 'detail': 'No validated data directory'}

    # Check BTC/USDT 1h as representative
    btc_file = validated_dir / 'binance' / 'BTC_USDT_1h.parquet'
    if not btc_file.exists():
        return {'check': 'data_freshness', 'pass': False, 'detail': 'BTC_USDT_1h.parquet not found'}

    df = pd.read_parquet(btc_file, columns=['timestamp'])
    latest = df['timestamp'].max()
    if latest.tzinfo is None:
        latest = latest.tz_localize('UTC')

    age = datetime.now(timezone.utc) - latest
    age_hours = age.total_seconds() / 3600
    is_fresh = age_hours < 48  # Allow 48h for daily pipeline

    return {
        'check': 'data_freshness',
        'pass': is_fresh,
        'detail': f"BTC/USDT 1h latest: {latest}, {age_hours:.1f}h ago",
    }


def check_validation_status() -> dict:
    """Check the most recent validation report for failures."""
    log_dir = PROJECT_ROOT / 'logs'
    reports = sorted(log_dir.glob('validation_report_*.txt'), reverse=True)

    if not reports:
        return {'check': 'validation_status', 'pass': False, 'detail': 'No validation reports found'}

    latest_report = reports[0]
    content = latest_report.read_text()

    approved = 'DATA APPROVED FOR RESEARCH USE' in content

    return {
        'check': 'validation_status',
        'pass': approved,
        'detail': f"Latest report: {latest_report.name} — {'APPROVED' if approved else 'FAILURES FOUND'}",
    }


def check_disk_usage() -> dict:
    """Check total data size is reasonable."""
    data_dir = PROJECT_ROOT / 'data'
    if not data_dir.exists():
        return {'check': 'disk_usage', 'pass': False, 'detail': 'No data directory'}

    total_bytes = sum(f.stat().st_size for f in data_dir.rglob('*') if f.is_file())
    total_gb = total_bytes / (1024 ** 3)

    # Alert if over 20GB (something may be wrong)
    ok = total_gb < 20

    return {
        'check': 'disk_usage',
        'pass': ok,
        'detail': f"Total data: {total_gb:.2f} GB" + ("" if ok else " — OVER 20GB LIMIT"),
    }


def main():
    logger.info("=" * 60)
    logger.info("PIPELINE HEALTH CHECK")
    logger.info(f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    logger.info("=" * 60)

    checks = [
        check_daily_update_ran(),
        check_data_freshness(),
        check_validation_status(),
        check_disk_usage(),
    ]

    all_pass = True
    for check in checks:
        status = "PASS" if check['pass'] else "FAIL"
        icon = "OK" if check['pass'] else "!!"
        logger.info(f"  [{icon}] {check['check']}: {status} — {check['detail']}")
        if not check['pass']:
            all_pass = False

    logger.info("")
    if all_pass:
        logger.info("HEALTH: ALL CHECKS PASSED")
    else:
        logger.warning("HEALTH: ISSUES DETECTED — investigate above failures")

    logger.info("=" * 60)

    sys.exit(0 if all_pass else 1)


if __name__ == "__main__":
    main()
