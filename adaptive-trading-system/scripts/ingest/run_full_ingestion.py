#!/usr/bin/env python3
"""
Master Ingestion Orchestrator
Runs all data ingestion scripts in the optimal sequence.

Execution order:
1. Download bulk historical CSVs (fast bootstrap)
2. Fetch OHLCV via API (fill gaps and get complete coverage)
3. Fetch funding rates
4. Fetch sentiment data (Fear & Greed Index)
5. Fetch NGN/USD rates
6. Generate summary report
"""

import subprocess
import sys
from pathlib import Path
from datetime import datetime, timezone
from loguru import logger
import time

# Configure logging
log_dir = Path(__file__).parent.parent.parent / 'logs'
log_dir.mkdir(exist_ok=True)

log_file = log_dir / f'full_ingestion_{datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")}.log'

logger.remove()  # Remove default stderr handler to prevent duplicate output
logger.add(sys.stdout, level='INFO')
logger.add(
    log_file,
    rotation='100 MB',
    retention='30 days',
    level='INFO'
)


class IngestionOrchestrator:
    """Orchestrates the full data ingestion pipeline."""

    def __init__(self):
        """Initialize the orchestrator."""
        self.scripts_dir = Path(__file__).parent
        self.start_time = None
        self.results = {}

    def run_script(self, script_name: str, description: str) -> bool:
        """
        Run a Python script and capture results.

        Args:
            script_name: Name of script file (e.g., 'fetch_ohlcv.py')
            description: Human-readable description

        Returns:
            True if successful, False otherwise
        """
        script_path = self.scripts_dir / script_name

        if not script_path.exists():
            logger.error(f"Script not found: {script_path}")
            return False

        logger.info(f"\n{'='*70}")
        logger.info(f"STEP: {description}")
        logger.info(f"Script: {script_name}")
        logger.info(f"{'='*70}\n")

        start_time = time.time()

        try:
            # Get Python interpreter from virtual environment
            python_path = sys.executable

            # Run script (timeout: 6 hours for OHLCV, 1 hour for others)
            timeout_seconds = 21600 if 'ohlcv' in script_name else 3600
            result = subprocess.run(
                [python_path, str(script_path)],
                capture_output=True,
                text=True,
                cwd=script_path.parent,
                timeout=timeout_seconds
            )

            elapsed = time.time() - start_time

            # Log output
            if result.stdout:
                logger.info(f"Output:\n{result.stdout}")

            if result.stderr:
                logger.warning(f"Errors:\n{result.stderr}")

            # Check return code
            if result.returncode == 0:
                logger.info(f"✓ {description} completed in {elapsed:.1f}s")
                self.results[script_name] = {'status': 'success', 'elapsed': elapsed}
                return True
            else:
                logger.error(f"✗ {description} failed with return code {result.returncode}")
                self.results[script_name] = {'status': 'failed', 'elapsed': elapsed, 'code': result.returncode}
                return False

        except subprocess.TimeoutExpired:
            elapsed = time.time() - start_time
            logger.error(f"✗ {description} timed out after {timeout_seconds}s")
            self.results[script_name] = {'status': 'timeout', 'elapsed': elapsed}
            return False

        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"✗ {description} failed with exception: {e}")
            self.results[script_name] = {'status': 'error', 'elapsed': elapsed, 'error': str(e)}
            return False

    def generate_summary(self):
        """Generate summary report of ingestion."""
        logger.info(f"\n{'='*70}")
        logger.info("INGESTION SUMMARY")
        logger.info(f"{'='*70}\n")

        total_elapsed = time.time() - self.start_time
        successful = sum(1 for r in self.results.values() if r['status'] == 'success')
        failed = len(self.results) - successful

        logger.info(f"Total time: {total_elapsed / 60:.1f} minutes")
        logger.info(f"Successful: {successful}/{len(self.results)}")
        logger.info(f"Failed: {failed}/{len(self.results)}")

        logger.info(f"\nDetailed Results:")
        for script, result in self.results.items():
            status_symbol = "✓" if result['status'] == 'success' else "✗"
            logger.info(f"  {status_symbol} {script}: {result['status']} ({result['elapsed']:.1f}s)")

        # Check data directory sizes
        data_dir = Path(__file__).parent.parent.parent / 'data' / 'raw'

        if data_dir.exists():
            logger.info(f"\nData Directory Sizes:")

            for subdir in ['ohlcv', 'funding_rates', 'sentiment']:
                subdir_path = data_dir / subdir
                if subdir_path.exists():
                    size_bytes = sum(f.stat().st_size for f in subdir_path.rglob('*') if f.is_file())
                    size_mb = size_bytes / (1024 * 1024)
                    file_count = len(list(subdir_path.rglob('*.parquet')))
                    logger.info(f"  {subdir}: {size_mb:.2f} MB ({file_count} files)")

        logger.info(f"\n{'='*70}")

        if failed == 0:
            logger.info("✓✓✓ FULL INGESTION COMPLETE - ALL STEPS SUCCESSFUL ✓✓✓")
        else:
            logger.warning(f"⚠️  INGESTION COMPLETE WITH {failed} FAILURE(S)")

        logger.info(f"{'='*70}\n")

        logger.info(f"Full log saved to: {log_file}")

    def run_full_pipeline(self):
        """Run the complete ingestion pipeline."""
        self.start_time = time.time()

        logger.info(f"\n{'#'*70}")
        logger.info("STARTING FULL DATA INGESTION PIPELINE")
        logger.info(f"Started at: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
        logger.info(f"{'#'*70}\n")

        # Define pipeline steps
        steps = [
            # Step 1: Download bulk historical (fast bootstrap)
            {
                'script': 'download_historical.py',
                'description': 'Download bulk historical CSVs',
                'critical': False  # Non-critical - if it fails, API fetch will cover it
            },

            # Step 2: Fetch OHLCV via API (complete coverage)
            {
                'script': 'fetch_ohlcv.py',
                'description': 'Fetch OHLCV data via exchange APIs',
                'critical': True  # Critical - core dataset
            },

            # Step 3: Fetch funding rates
            {
                'script': 'fetch_funding_rates.py',
                'description': 'Fetch perpetual futures funding rates',
                'critical': True  # Critical for sentiment analysis
            },

            # Step 4: Fetch sentiment data
            {
                'script': 'fetch_sentiment.py',
                'description': 'Fetch Fear & Greed Index',
                'critical': True  # Critical for sentiment analysis
            },

            # Step 5: Fetch NGN/USD rates (DISABLED — operating in USD)
            # {
            #     'script': 'fetch_ngn_rate.py',
            #     'description': 'Fetch NGN/USD exchange rates',
            #     'critical': True
            # },
        ]

        # Execute pipeline
        critical_failure = False

        for step in steps:
            success = self.run_script(step['script'], step['description'])

            if not success and step['critical']:
                logger.error(f"Critical step failed: {step['description']}")
                critical_failure = True

            # Small pause between steps
            time.sleep(2)

        # Generate summary
        self.generate_summary()

        # Exit code
        if critical_failure:
            logger.error("Exiting with error due to critical failure(s)")
            sys.exit(1)
        else:
            logger.info("Exiting successfully")
            sys.exit(0)


def main():
    """Main entry point."""
    orchestrator = IngestionOrchestrator()
    orchestrator.run_full_pipeline()


if __name__ == "__main__":
    main()
