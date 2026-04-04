#!/usr/bin/env python3
"""
DuckDB Research Database Setup
Creates views over validated Parquet files for fast SQL-based analysis.
No data duplication — DuckDB reads Parquet natively.
"""

import duckdb
from pathlib import Path
from datetime import datetime, timezone


def create_research_db(parquet_dir: str, db_path: str = None) -> duckdb.DuckDBPyConnection:
    """
    Create a DuckDB database with views over validated Parquet files.

    Args:
        parquet_dir: Path to validated data directory
        db_path: Path for DuckDB file (None = in-memory)

    Returns:
        DuckDB connection
    """
    if db_path:
        con = duckdb.connect(db_path)
    else:
        con = duckdb.connect()

    parquet_dir = str(Path(parquet_dir).resolve())

    # Create views over parquet files
    con.execute(f"""
        CREATE OR REPLACE VIEW ohlcv AS
        SELECT * FROM read_parquet('{parquet_dir}/ohlcv/**/*.parquet')
    """)

    con.execute(f"""
        CREATE OR REPLACE VIEW funding_rates AS
        SELECT * FROM read_parquet('{parquet_dir}/funding_rates/**/*.parquet')
    """)

    con.execute(f"""
        CREATE OR REPLACE VIEW sentiment AS
        SELECT * FROM read_parquet('{parquet_dir}/sentiment/**/*.parquet')
    """)

    # Verification
    row_count = con.execute("SELECT COUNT(*) FROM ohlcv").fetchone()[0]
    date_range = con.execute("SELECT MIN(timestamp), MAX(timestamp) FROM ohlcv").fetchone()
    pairs = con.execute("SELECT DISTINCT symbol FROM ohlcv ORDER BY symbol").fetchall()
    timeframes = con.execute("SELECT DISTINCT timeframe FROM ohlcv ORDER BY timeframe").fetchall()

    funding_count = con.execute("SELECT COUNT(*) FROM funding_rates").fetchone()[0]
    sentiment_count = con.execute("SELECT COUNT(*) FROM sentiment").fetchone()[0]

    print("=" * 60)
    print("RESEARCH DATABASE READY")
    print("=" * 60)
    print(f"OHLCV:         {row_count:,} rows")
    print(f"Funding rates: {funding_count:,} rows")
    print(f"Sentiment:     {sentiment_count:,} rows")
    print(f"Date range:    {date_range[0]} to {date_range[1]}")
    print(f"Pairs:         {[p[0] for p in pairs]}")
    print(f"Timeframes:    {[t[0] for t in timeframes]}")
    print("=" * 60)

    return con


def main():
    project_root = Path(__file__).parent.parent.parent
    parquet_dir = project_root / 'data' / 'validated'
    db_path = project_root / 'data' / 'research.duckdb'

    if not parquet_dir.exists():
        print(f"ERROR: Validated data directory not found: {parquet_dir}")
        print("Run the validation pipeline first: python scripts/validate/run_validation.py")
        return

    con = create_research_db(str(parquet_dir), str(db_path))

    # Quick data inventory
    print("\nDATA INVENTORY:")
    inventory = con.execute("""
        SELECT symbol, timeframe, COUNT(*) as rows,
               MIN(timestamp) as start, MAX(timestamp) as end
        FROM ohlcv
        GROUP BY symbol, timeframe
        ORDER BY symbol, timeframe
    """).fetchdf()
    print(inventory.to_string(index=False))

    con.close()
    print(f"\nDatabase saved to: {db_path}")


if __name__ == "__main__":
    main()
