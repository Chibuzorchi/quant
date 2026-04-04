# Data Catalog

## Last updated: 2026-04-04

### OHLCV Data

| Symbol | Exchange | Timeframes | Start Date | End Date | Total Rows | Completeness |
|--------|----------|------------|------------|----------|------------|-------------|
| BTC/USDT | Binance | 1m,5m,1h,4h,1d | 2023-01-01 | 2026-04-04 | 2,092,479 | 99.99% |
| ETH/USDT | Binance | 1m,5m,1h,4h,1d | 2023-01-01 | 2026-04-04 | 2,092,479 | 99.99% |
| SOL/USDT | Binance | 1m,5m,1h,4h,1d | 2023-01-01 | 2026-04-04 | 2,092,479 | 99.99% |
| BNB/USDT | Binance | 1m,5m,1h,4h,1d | 2023-01-01 | 2026-04-04 | 2,092,479 | 99.99% |
| XRP/USDT | Binance | 1m,5m,1h,4h,1d | 2023-01-01 | 2026-04-04 | 2,092,479 | 99.99% |
| DOGE/USDT | Binance | 1m,5m,1h,4h,1d | 2023-01-01 | 2026-04-04 | 2,092,479 | 99.99% |

**Total OHLCV: ~12,554,874 rows across 30 Parquet files (~285 MB compressed)**

### Auxiliary Data

| Dataset | Source | Frequency | Start Date | End Date | Rows |
|---------|--------|-----------|------------|----------|------|
| Funding rates (BTC) | Binance | 8-hourly | 2023-01-01 | 2026-04-04 | 3,569 |
| Funding rates (ETH) | Binance | 8-hourly | 2023-01-01 | 2026-04-04 | 3,569 |
| Fear & Greed Index | alternative.me | Daily | 2018-02-01 | 2026-04-04 | 2,981 |

### Storage

- Format: Apache Parquet (Snappy compression)
- Raw data: `data/raw/` (~285 MB)
- Validated data: `data/validated/` (~285 MB)
- Research DB: `data/research.duckdb` (views over validated Parquet)
- Backups: `data/backups/` (latest snapshot)
- Total disk: ~0.6 GB

### Known Issues

- All pairs have ~80 missing 1-minute candles (single gap, likely exchange maintenance)
- All pairs have ~16 missing 5-minute candles (same gap period)
- 1-hour data has 1 missing candle per pair (filled via forward-fill)
- 4-hour and daily data: 100% complete, zero gaps
- XRP/USDT shows 73% price jump on 2023-07-13 (legitimate — Ripple SEC ruling)

### Data Licensing

| Source | License | Commercial Use |
|--------|---------|---------------|
| Binance API | Free, public market data | Yes for personal use |
| alternative.me | Free public API | Yes for personal use |
| CryptoDataDownload | CC BY-NC-SA 4.0 | NO — non-commercial only |

### API Keys & Subscriptions

| Service | Cost | Key Location | Permissions |
|---------|------|-------------|-------------|
| Binance | Free | config/.env | Read-only, IP whitelisted |
| Bybit | Free | config/.env | Read-only, IP whitelisted |

### Pipeline

- **Full ingestion**: `scripts/ingest/run_full_ingestion.py` (~2 hours)
- **Daily update**: `scripts/ingest/daily_update.py` (~5-10 minutes)
- **Validation**: `scripts/validate/run_validation.py` (~1 minute)
- **Health check**: `scripts/validate/check_pipeline_health.py`
- **Schedule**: Daily at 00:00 UTC (manual or cron)
