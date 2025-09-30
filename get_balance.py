#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys
import ccxt

def _load_api_credentials() -> Tuple[Optional[str], Optional[str]]:
    """Load API credentials with error handling"""
    try:
        from api_config import get_api_credentials
        api_key, api_secret = get_api_credentials()
        
        if not api_key or not api_secret:
            print("❌ API credentials not configured!")
            print("Please update api_config.py with your Binance API credentials")
            return None, None
        else:
            print("✅ API credentials loaded successfully")
            return api_key, api_secret
            
    except ImportError:
        print("⚠️  api_config.py not found, running without API credentials")
        return None, None
    
def main():
    api_key, api_secret = _load_api_credentials()
    if not api_key or not api_secret:
        print("กรุณา export BN_API_KEY และ BN_API_SECRET ก่อนรันสคริปต์", file=sys.stderr)
        sys.exit(1)

    ex = ccxt.binance({
        "apiKey": api_key,
        "secret": api_secret,
        "enableRateLimit": True,
        "options": {"adjustForTimeDifference": True, "defaultType": "spot"},
    })

    try:
        ex.load_markets()
        bal = ex.fetch_balance()   # SPOT
    except Exception as e:
        print(f"ดึงยอดเงินไม่สำเร็จ: {e}", file=sys.stderr)
        sys.exit(1)

    free = bal.get("free", {}) or {}
    used = bal.get("used", {}) or {}
    total = bal.get("total", {}) or {}

    rows = []
    for asset, tot in total.items():
        t = float(tot or 0)
        if t > 0:
            rows.append((asset, float(free.get(asset, 0) or 0),
                               float(used.get(asset, 0) or 0),
                               t))
    rows.sort(key=lambda r: r[3], reverse=True)

    print("=== Binance Spot Balances (non-zero) ===")
    print(f"{'Asset':<8} {'Free':>18} {'Used':>18} {'Total':>18}")
    for a, f, u, t in rows:
        print(f"{a:<8} {f:>18.8f} {u:>18.8f} {t:>18.8f}")

    print(f"\nFree USDT: {free.get('USDT', 0)}")

if __name__ == "__main__":
    main()
