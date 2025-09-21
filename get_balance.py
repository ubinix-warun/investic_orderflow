#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys
import ccxt

def main():
    key = os.getenv("BINANCE_KEY")
    sec = os.getenv("BINANCE_SECRET")
    if not key or not sec:
        print("กรุณา export BINANCE_KEY และ BINANCE_SECRET ก่อนรันสคริปต์", file=sys.stderr)
        sys.exit(1)

    ex = ccxt.binance({
        "apiKey": key,
        "secret": sec,
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
