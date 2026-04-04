#!/usr/bin/env python3
"""
Funding Rates Fetcher
Fetches perpetual futures funding rates (sentiment proxy).
Positive funding = longs paying shorts (bullish positioning)
Negative funding = shorts paying longs (bearish positioning)
"""

import ccxt
import pandas as pd
import time
from pathlib import Path
from datetime import datetime, timezone
from loguru import logger
import os
from dotenv import load_dotenv

# Load environment variables
env_path = Path(__file__).parent.parent.parent / 'config' / '.env'
load_dotenv(env_path)

# Configure logging
log_dir = Path(__file__).parent.parent.parent / 'logs'
log_dir.mkdir(exist_ok=True)
logger.add(
    log_dir / 'funding_rates_{time:YYYY-MM-DD}.log',
    rotation='1 day',
    retention='30 days',
    level='INFO'
)


class FundingRateFetcher:
    """Fetches funding rate history from perpetual futures."""

    def __init__(self, exchange_id: str, api_key: str = None, secret: str = None):
        """Initialize the fetcher."""
        self.exchange_id = exchange_id

        exchange_class = getattr(ccxt, exchange_id)
        params = {}
        if api_key and secret:
            params = {'apiKey': api_key, 'secret': secret}

        self.exchange = exchange_class(params)
        self.exchange.load_markets()

        logger.info(f"Initialized {exchange_id} for funding rates")

    def fetch_funding_rate_history(
        self,
        symbol: str,
        since: str,
        until: str
    ) -> pd.DataFrame:
        """
        Fetch funding rate history.

        Args:
            symbol: Perpetual futures symbol (e.g., 'BTC/USDT:USDT')
            since: Start date (YYYY-MM-DD)
            until: End date (YYYY-MM-DD or 'present')

        Returns:
            DataFrame with funding rate history
        """
        logger.info(f"Fetching funding rates for {symbol} from {since} to {until}")

        # Convert dates to timestamps
        since_ts = int(pd.Timestamp(since, tz='UTC').timestamp() * 1000)

        if until == 'present':
            until_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
        else:
            until_ts = int(pd.Timestamp(until, tz='UTC').timestamp() * 1000)

        all_data = []
        current_since = since_ts

        while current_since < until_ts:
            try:
                # Fetch batch
                funding_history = self.exchange.fetch_funding_rate_history(
                    symbol,
                    since=current_since,
                    limit=1000
                )

                if not funding_history or len(funding_history) == 0:
                    logger.warning(f"No more funding data available for {symbol}")
                    break

                all_data.extend(funding_history)

                # Update timestamp
                last_timestamp = funding_history[-1]['timestamp']
                current_since = last_timestamp + 1

                logger.info(f"Fetched {len(funding_history)} funding rate records - Last: {pd.Timestamp(last_timestamp, unit='ms', tz='UTC')}")

                # Rate limiting
                time.sleep(self.exchange.rateLimit / 1000)

            except ccxt.RateLimitExceeded:
                logger.warning("Rate limit exceeded, sleeping 60s")
                time.sleep(60)
                continue

            except ccxt.NetworkError as e:
                logger.error(f"Network error: {e}, retrying in 10s")
                time.sleep(10)
                continue

            except Exception as e:
                logger.error(f"Error fetching funding rates: {e}")
                break

        if not all_data:
            logger.warning(f"No funding data fetched for {symbol}")
            return pd.DataFrame()

        # Convert to DataFrame
        df = pd.DataFrame(all_data)

        # Normalize timestamp
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)

        # Extract funding rate
        df['funding_rate'] = df['fundingRate']

        # Add metadata
        df['exchange'] = self.exchange_id

        # Select relevant columns
        df = df[['timestamp', 'symbol', 'funding_rate', 'exchange']]

        # Remove duplicates
        df = df.drop_duplicates(subset=['timestamp', 'symbol'], keep='last')

        # Sort by timestamp
        df = df.sort_values('timestamp').reset_index(drop=True)

        logger.info(f"✓ Fetched {len(df)} funding rate records for {symbol}")

        return df

    def save_to_parquet(self, df: pd.DataFrame, output_dir: Path):
        """Save funding rates to Parquet."""
        if df.empty:
            logger.warning("Empty DataFrame, skipping save")
            return

        symbol = df['symbol'].iloc[0]
        exchange = df['exchange'].iloc[0]

        # Create directory: data/raw/funding_rates/{exchange}/
        exchange_dir = output_dir / exchange
        exchange_dir.mkdir(parents=True, exist_ok=True)

        # Filename: {symbol}_funding.parquet
        safe_symbol = symbol.replace('/', '_').replace(':', '_')
        filename = f"{safe_symbol}_funding.parquet"
        filepath = exchange_dir / filename

        # Save as Parquet
        df.to_parquet(filepath, index=False, compression='snappy')

        file_size_mb = filepath.stat().st_size / (1024 * 1024)
        logger.info(f"✓ Saved to {filepath} ({file_size_mb:.2f} MB)")


def main():
    """Main function to fetch funding rates for MVP dataset."""

    # Load configuration
    import yaml
    config_path = Path(__file__).parent.parent.parent / 'config' / 'data_sources.yaml'
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    # Get funding rates configuration
    funding_config = config['funding_rates']
    exchange_id = funding_config['exchange']
    pairs = funding_config['pairs']
    start_date = funding_config['history']['start_date']
    end_date = funding_config['history']['end_date']

    # Output directory
    output_dir = Path(__file__).parent.parent.parent / 'data' / 'raw' / 'funding_rates'
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load API credentials
    api_key = os.getenv(f'{exchange_id.upper()}_API_KEY')
    secret = os.getenv(f'{exchange_id.upper()}_SECRET')

    # Initialize fetcher
    fetcher = FundingRateFetcher(exchange_id, api_key, secret)

    # Fetch funding rates
    logger.info(f"Starting funding rates ingestion for {len(pairs)} pairs")
    logger.info(f"Date range: {start_date} to {end_date}")

    total_fetched = 0

    for symbol in pairs:
        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"Fetching funding rates for {symbol}")
            logger.info(f"{'='*60}")

            df = fetcher.fetch_funding_rate_history(
                symbol=symbol,
                since=start_date,
                until=end_date
            )

            if not df.empty:
                fetcher.save_to_parquet(df, output_dir)
                total_fetched += len(df)

        except Exception as e:
            logger.error(f"Failed to fetch funding rates for {symbol}: {e}")
            continue

    logger.info(f"\n{'='*60}")
    logger.info(f"Funding rates ingestion complete!")
    logger.info(f"Total records fetched: {total_fetched:,}")
    logger.info(f"{'='*60}")


if __name__ == "__main__":
    main()
