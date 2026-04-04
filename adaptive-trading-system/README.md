# Adaptive Trading System - Module 1: Data Foundation

## Overview

This is Module 1 of a 12-module institutional-grade adaptive trading system. This module provides the complete data foundation that all other modules depend on.

**Location**: Lagos, Nigeria (WAT, UTC+1)  
**Purpose**: Personal/proprietary cryptocurrency trading  
**Markets**: Cryptocurrency (primary), Forex (secondary/later)  
**Python Version**: 3.13.11

## Project Structure

```
adaptive-trading-system/
├── config/                   # Configuration files
│   ├── .env                  # API keys (NEVER commit this)
│   ├── exchanges.yaml        # Exchange configs (pairs, timeframes, rate limits)
│   └── data_sources.yaml     # All data source definitions
├── data/
│   ├── raw/                  # Untouched data exactly as received
│   │   ├── ohlcv/           # OHLC + Volume data
│   │   ├── funding_rates/   # Perpetual futures funding rates
│   │   ├── on_chain/        # On-chain metrics (future)
│   │   └── sentiment/       # Sentiment indicators
│   ├── validated/            # Data that passed quality checks
│   ├── processed/            # Features calculated, ready for analysis
│   └── backups/              # Automated daily backups
├── scripts/
│   ├── ingest/              # Data collection scripts
│   ├── validate/            # Data quality checks
│   ├── transform/           # Feature engineering
│   └── utils/               # Shared utilities
├── notebooks/               # Jupyter research notebooks
├── logs/                    # Pipeline logs
├── tests/                   # Unit and integration tests
└── README.md                # This file
```

## Directory Descriptions

### `/config`
Configuration files for the system:
- **`.env`**: API keys and secrets (gitignored)
- **`exchanges.yaml`**: Exchange-specific settings (rate limits, available pairs, etc.)
- **`data_sources.yaml`**: Definitions of all data sources (APIs, endpoints, schemas)

### `/data/raw`
Raw data exactly as received from sources. No transformations applied.
- **`ohlcv/`**: Open, High, Low, Close, Volume data by exchange and symbol
- **`funding_rates/`**: Perpetual futures funding rates (8-hour intervals)
- **`on_chain/`**: Blockchain metrics (wallet flows, network activity) - future
- **`sentiment/`**: Fear & Greed Index, social sentiment metrics

### `/data/validated`
Data that has passed all quality checks:
- Schema validation (correct types, no nulls)
- Range validation (high >= low, volume >= 0, etc.)
- Completeness validation (gap detection and filling)
- Cross-source validation (price agreement between exchanges)

### `/data/processed`
Engineered features ready for backtesting and live trading:
- Technical indicators
- Regime classifications
- Volatility estimates
- Correlation matrices

### `/data/backups`
Automated daily backups of validated and processed data.

### `/scripts/ingest`
Data collection scripts:
- OHLCV fetchers (via CCXT)
- Funding rate collectors
- Sentiment data downloaders
- NGN/USD rate fetchers
- Bulk historical downloaders

### `/scripts/validate`
Data quality validators:
- Schema validator
- Range validator
- Completeness validator
- Freshness validator
- Cross-source validator

### `/scripts/transform`
Feature engineering pipelines (Module 1 Phase 1D and beyond).

### `/scripts/utils`
Shared utilities:
- Database setup (DuckDB)
- Logging configuration
- Rate limiting helpers
- Checkpoint management

### `/notebooks`
Jupyter notebooks for research and exploration:
- Data exploration
- Strategy prototyping
- Performance analysis

### `/logs`
Pipeline execution logs with timestamps.

### `/tests`
Unit and integration tests for all scripts.

## Setup Instructions

### 1. Environment Setup

```bash
# Create virtual environment (Python 3.13 required for pandas-ta)
python3.13 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install ccxt pandas numpy polars duckdb pyarrow fastparquet requests
pip install jupyter jupyterlab matplotlib plotly seaborn
pip install pandas-ta python-dotenv loguru schedule
```

### 2. Configure API Keys

Edit `config/.env` with your exchange API keys:
- Use **READ-ONLY** permissions only
- Enable **IP whitelisting** for security
- Never commit this file to git

### 3. Verify Setup

```bash
# Test core imports
python -c "import ccxt, pandas, duckdb, polars, pyarrow"

# Verify git ignores .env
git check-ignore config/.env  # Should output: config/.env
```

## Phase 1A Completion Checklist

- [x] Python 3.13 virtual environment created
- [x] All core dependencies installed
- [x] pandas-ta installed successfully
- [x] Project directory structure created
- [x] Git repository initialized
- [x] `.gitignore` configured
- [x] `.env` template created and gitignored
- [x] README.md documentation complete

## Next Steps

**Phase 1B**: Exchange Access Verification (Nigeria-specific)
- Create accounts on Tier 1 exchanges (Binance, Bybit, OKX)
- Complete KYC verification
- Generate READ-ONLY API keys with IP whitelisting
- Verify CCXT connectivity
- Document results in `config/exchanges.yaml`

## Currency Tracking

All performance metrics tracked in:
- **USD** (primary trading currency)
- **NGN** (local purchasing power)

NGN/USD parallel market rates are ingested daily alongside trading data to measure real purchasing-power performance.

## Data Pipeline Architecture

```
[Exchange APIs] → [Rate-Limited Fetcher] → [Raw Parquet] → [Validation] → [Validated Parquet] → [DuckDB Views] → [Analysis]
```

Every stage has verification. Never skip validation.

## Support

For issues related to:
- Nigerian exchange access or KYC
- Network/power resilience
- Currency conversion tracking

Refer to `references/strategic_answers.md` in the module documentation.

---

**Module 1 Status**: Phase 1A Complete ✓  
**Last Updated**: 2026-04-04  
**Python Version**: 3.13.11  
**Location**: Lagos, Nigeria (UTC+1)
