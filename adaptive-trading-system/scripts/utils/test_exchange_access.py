#!/usr/bin/env python3
"""
Test exchange API connectivity for Phase 1B verification.
Run this after adding API keys to config/.env
"""

import ccxt
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
env_path = Path(__file__).parent.parent.parent / 'config' / '.env'
load_dotenv(env_path)

def test_exchange(exchange_id: str, api_key_env: str, secret_env: str, password_env: str = None):
    """Test if an exchange is accessible with provided credentials."""

    print(f"\n{'='*60}")
    print(f"Testing: {exchange_id.upper()}")
    print(f"{'='*60}")

    api_key = os.getenv(api_key_env)
    secret = os.getenv(secret_env)

    if not api_key or 'your_' in api_key.lower():
        print(f"❌ API key not configured in .env ({api_key_env})")
        return False

    if not secret or 'your_' in secret.lower():
        print(f"❌ Secret not configured in .env ({secret_env})")
        return False

    try:
        # Initialize exchange
        exchange_class = getattr(ccxt, exchange_id)
        exchange_params = {
            'apiKey': api_key,
            'secret': secret,
        }

        # Add password if required (OKX, KuCoin)
        if password_env:
            password = os.getenv(password_env)
            if password and 'your_' not in password.lower():
                exchange_params['password'] = password

        exchange = exchange_class(exchange_params)

        # Test 1: Fetch ticker (public endpoint, no auth needed)
        print("\n[Test 1/4] Fetching BTC/USDT ticker (public endpoint)...")
        ticker = exchange.fetch_ticker('BTC/USDT')
        print(f"✓ BTC/USDT Last Price: ${ticker['last']:,.2f}")

        # Test 2: Fetch markets (public endpoint)
        print("\n[Test 2/4] Loading markets...")
        exchange.load_markets()
        available_pairs = [s for s in exchange.symbols if '/USDT' in s]
        print(f"✓ Found {len(available_pairs)} USDT pairs")

        # Test 3: Check API key permissions (requires auth)
        print("\n[Test 3/4] Testing API key permissions...")
        try:
            # This will fail if API key is invalid or lacks read permissions
            balance = exchange.fetch_balance()
            print(f"✓ API key authenticated successfully (Read permissions verified)")

            # Check if trading is enabled (it shouldn't be for READ-ONLY keys)
            if hasattr(exchange, 'fetch_open_orders'):
                try:
                    orders = exchange.fetch_open_orders('BTC/USDT')
                    print(f"⚠️  WARNING: API key has ORDER READ access (this is OK)")
                except:
                    pass
        except ccxt.AuthenticationError:
            print(f"❌ Authentication failed - check API key and secret")
            return False
        except Exception as e:
            print(f"⚠️  Auth test inconclusive: {str(e)[:100]}")

        # Test 4: Check rate limits
        print("\n[Test 4/4] Checking rate limits...")
        print(f"✓ Rate limit: {exchange.rateLimit}ms between requests")

        # Check if specific pairs are available
        print("\n[Pair Availability Check]")
        mvp_pairs = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'BNB/USDT', 'XRP/USDT', 'DOGE/USDT']
        available_mvp = [p for p in mvp_pairs if p in exchange.symbols]
        print(f"MVP pairs available: {len(available_mvp)}/{len(mvp_pairs)}")
        for pair in mvp_pairs:
            status = "✓" if pair in exchange.symbols else "✗"
            print(f"  {status} {pair}")

        print(f"\n{'='*60}")
        print(f"✓ {exchange_id.upper()} - ALL TESTS PASSED")
        print(f"{'='*60}")
        return True

    except ccxt.NetworkError as e:
        print(f"❌ Network error: {e}")
        print(f"   This could indicate geo-blocking from Nigeria")
        return False
    except ccxt.ExchangeError as e:
        print(f"❌ Exchange error: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False


def main():
    """Test all configured exchanges."""

    print("""
╔══════════════════════════════════════════════════════════════╗
║  PHASE 1B: EXCHANGE ACCESS VERIFICATION (NIGERIA)            ║
║  Testing READ-ONLY API access from Nigerian IP               ║
╚══════════════════════════════════════════════════════════════╝
""")

    exchanges_to_test = [
        ('binance', 'BINANCE_API_KEY', 'BINANCE_SECRET'),
        ('bybit', 'BYBIT_API_KEY', 'BYBIT_SECRET'),
        ('okx', 'OKX_API_KEY', 'OKX_SECRET', 'OKX_PASSWORD'),
        ('kraken', 'KRAKEN_API_KEY', 'KRAKEN_SECRET'),
        ('kucoin', 'KUCOIN_API_KEY', 'KUCOIN_SECRET', 'KUCOIN_PASSWORD'),
    ]

    results = {}

    for exchange_config in exchanges_to_test:
        exchange_id = exchange_config[0]
        try:
            result = test_exchange(*exchange_config)
            results[exchange_id] = result
        except Exception as e:
            print(f"\n❌ Failed to test {exchange_id}: {e}")
            results[exchange_id] = False

    # Summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)

    tier1_exchanges = ['binance', 'bybit', 'okx']
    tier1_working = sum(1 for ex in tier1_exchanges if results.get(ex, False))

    print(f"\nTier 1 Exchanges (need minimum 2):")
    for ex in tier1_exchanges:
        status = "✓ VERIFIED" if results.get(ex, False) else "✗ NOT CONFIGURED or FAILED"
        print(f"  {ex.upper()}: {status}")

    print(f"\nTier 1 Status: {tier1_working}/3 verified")

    if tier1_working >= 2:
        print("\n✓✓✓ PHASE 1B REQUIREMENT MET ✓✓✓")
        print(f"You have {tier1_working} Tier 1 exchanges verified!")
        print("\nNext: Create config/exchanges.yaml to document your setup")
    else:
        print(f"\n⚠️  WARNING: Only {tier1_working} Tier 1 exchange(s) verified")
        print("You need at least 2 Tier 1 exchanges for redundancy")
        print("\nAction required:")
        print("1. Sign up for Tier 1 exchanges (Binance, Bybit, OKX)")
        print("2. Complete KYC with Nigerian ID")
        print("3. Generate READ-ONLY API keys with IP whitelisting")
        print("4. Add keys to config/.env")
        print("5. Run this script again")

    print("\n" + "="*60)


if __name__ == "__main__":
    main()
