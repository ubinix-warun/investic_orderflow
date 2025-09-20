#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Grid Planner (Single-Panel, Monotone Down with Price)
- แผงเดียว: ไม่แบ่ง below/above ในการจัดงบ
- เงินต่อกริด: ราคาต่ำสุดใช้เงินมากสุด แล้ว "ลดลงเรื่อย ๆ" จนถึงราคาสูงสุด
- เคารพขั้นต่ำตลาดจริง (minNotional/minQty), PRICE_TICK, LOT_SIZE
- Binary search หาความชัน (slope) หลังปัด LOT/ขั้นต่ำ ให้รวม ≈ งบ
- ถ้างบยังไม่พอขั้นต่ำทุกชั้น → ตัดชั้น "ด้านบนสุด" ออกก่อน (คงด้านล่างไว้)
"""

import os, re, time, math
import numpy as np
import pandas as pd
import ccxt
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional, Tuple

# ============== USER CONFIG ==============
CFG = {
    "symbol": "XRP/USDT",
    "timeframe": "1d",
    "lookback_bars": 1000,
    "paths": 10_000,
    "horizon_bars": 60,
    "block_len": 24,
    "band_percentiles": (10, 90),
    "http_timeout_ms": 30000,
    "http_retries": 5,
    "retry_backoff_base_sec": 1.0,

    # กรอบเลเวล
    "LOWER_STRETCH_PCT": 15.0,     # ยืดกรอบฝั่งล่างให้หย่อนลง
    "MAX_LAYERS_CAP": 200,         # เพดานทฤษฎี
    "LEVELS_CAP": 0,               # จำกัดจำนวนชั้นจริงทั้งแผง (0=ไม่จำกัด)

    # งบ/TP/ค่าธรรมเนียม
    "BUDGET_USD": 300.0,
    "TP_PCT": 0.01,                # 1% = 0.01
    "FEE_RATE": 0.0004,            # ต่อข้าง

    # ช่วยตั้งจำนวนชั้นคร่าว ๆ: เป้าต่อชั้นใกล้ ๆ = target_mult × minNotional
    "TARGET_MIN_MULTIPLIER": 2.0,  # 2 เท่าของขั้นต่ำตลาดต่อแถว

    # labels (รายงานผล)
    "ZONE_NEAR": 0.05,
    "ZONE_MID": 0.15,

    # outputs
    "macro_csv": "macro_montecarlo.csv",
    "grid_csv": "grid_plan.csv",
}

# ============== Retry helper ==============
def _with_retries(fn, retries: int, backoff_base: float, *args, **kwargs):
    last_exc = None
    for i in range(retries):
        try:
            return fn(*args, **kwargs)
        except (ccxt.RequestTimeout, ccxt.NetworkError) as e:
            last_exc = e
            wait = backoff_base * (2 ** i)
            print(f"[warn] {type(e).__name__}: {e} | retry {i+1}/{retries} in {wait:.1f}s")
            time.sleep(wait)
        except Exception:
            raise
    if last_exc:
        raise last_exc
    raise RuntimeError("Unknown error in _with_retries")

# ============== CCXT client ==============
class BinanceClient:
    def __init__(self, timeout_ms: int):
        self.ex = ccxt.binance({
            "enableRateLimit": True,
            "timeout": timeout_ms,
            "options": {"adjustForTimeDifference": True},
        })
        self.markets = None

    def load_markets(self, retries: int, backoff: float):
        self.markets = _with_retries(self.ex.load_markets, retries, backoff, reload=True)

    def market(self, symbol: str):
        if self.markets is None:
            raise RuntimeError("markets not loaded")
        return self.ex.market(symbol)

    def fetch_ohlcv_1d(self, symbol: str, lookback_bars: int, retries: int, backoff: float) -> pd.DataFrame:
        raw = _with_retries(self.ex.fetch_ohlcv, retries, backoff, symbol=symbol, timeframe="1d", limit=lookback_bars)
        if not raw or len(raw) < 50:
            raise RuntimeError("Not enough OHLCV data.")
        df = pd.DataFrame(raw, columns=["ts", "open", "high", "low", "close", "volume"])
        df["dt"] = pd.to_datetime(df["ts"], unit="ms", utc=True).dt.tz_convert("Asia/Bangkok")
        return df[["dt", "open", "high", "low", "close", "volume"]]

# ============== Market meta ==============
def _extract_meta_from_market(m: dict) -> Tuple[Optional[float], Optional[float], float, float]:
    """(min_notional, min_qty, price_tick, qty_step) — ดึงจาก exchange จริง"""
    tick_size, qty_step = None, None
    filters = (m.get("info", {}) or {}).get("filters", []) or []
    for f in filters:
        t = f.get("filterType")
        if t == "PRICE_FILTER":
            try: tick_size = float(f.get("tickSize", "0.0001"))
            except: pass
        if t == "LOT_SIZE":
            try: qty_step = float(f.get("stepSize", "0.0001"))
            except: pass
    if tick_size is None:
        pr = (m.get("precision", {}) or {}).get("price", 4)
        tick_size = 10 ** (-int(pr))
    if qty_step is None:
        pr = (m.get("precision", {}) or {}).get("amount", 4)
        qty_step = 10 ** (-int(pr))

    min_notional, min_qty = None, None
    limits = m.get("limits", {}) or {}
    cost = limits.get("cost", {}) or {}
    amt  = limits.get("amount", {}) or {}
    try:
        if cost.get("min") is not None: min_notional = float(cost["min"])
    except: pass
    try:
        if amt.get("min") is not None: min_qty = float(amt["min"])
    except: pass

    if (min_notional is None) or (min_qty is None):
        for f in filters:
            t = f.get("filterType")
            if (t in ("MIN_NOTIONAL", "NOTIONAL")) and (min_notional is None):
                raw = f.get("minNotional") or f.get("notional")
                try:
                    if raw is not None: min_notional = float(raw)
                except: pass
            if t == "LOT_SIZE" and (min_qty is None):
                raw = f.get("minQty")
                try:
                    if raw is not None: min_qty = float(raw)
                except: pass

    return (min_notional, min_qty, float(tick_size), float(qty_step))

# ============== MC core ==============
def block_bootstrap_returns(ret: np.ndarray, horizon: int, block_len: int, paths: int, rng: np.random.Generator):
    n = len(ret)
    if n < block_len + 5:
        raise ValueError("Return history too short.")
    out = np.empty((paths, horizon), dtype=float)
    valid = np.arange(0, n - block_len + 1)
    for p in range(paths):
        seg = []
        while len(seg) < horizon:
            start = rng.choice(valid)
            seg.extend(ret[start:start+block_len])
        out[p, :] = np.array(seg[:horizon])
    return out

def simulate_price_paths(spot: float, logret_paths: np.ndarray) -> np.ndarray:
    return spot * np.exp(np.cumsum(logret_paths, axis=1))

def summarize_macro(prices: np.ndarray, spot: float, p_lo=10, p_hi=90):
    path_min = prices.min(axis=1); path_max = prices.max(axis=1)
    pct = [10, 50, 90]
    return {
        "spot": float(spot),
        "min_pct": np.percentile(path_min, pct).tolist(),
        "max_pct": np.percentile(path_max, pct).tolist(),
        "band_low": float(np.percentile(path_min, p_lo)),
        "band_high": float(np.percentile(path_max, p_hi)),
        "path_min": path_min.astype(float),
        "path_max": path_max.astype(float),
    }

# ============== Small helpers ==============
def _normalize_symbol(raw: str) -> str:
    s = (raw or "").strip().upper().replace("\\", "/").replace("-", "/")
    if not s: return s
    if "/" in s:
        base, quote = s.split("/", 1)
        base = base.strip(); quote = (quote or "USDT").strip() or "USDT"
        return f"{base}/{quote}"
    if s.endswith("USDT"):
        base = s[:-4]; return f"{base}/USDT"
    return f"{s}/USDT"

def _prompt_symbol(default_symbol: str) -> str:
    try: raw = input(f"Symbol to trade [default {default_symbol}]: ").strip()
    except EOFError: raw = ""
    return _normalize_symbol(raw) if raw else default_symbol

def _parse_numbers_only(raw: str) -> Optional[float]:
    if raw is None: return None
    s = raw.strip()
    if not s: return None
    if not re.fullmatch(r"\d+(\.\d+)?", s): return None
    try: return float(s)
    except ValueError: return None

def _prompt_budget(default_budget: float) -> float:
    while True:
        try: raw = input(f"Initial budget in USDT (numbers only) [default {default_budget:,.2f}]: ")
        except EOFError: raw = ""
        if not raw.strip(): return float(default_budget)
        v = _parse_numbers_only(raw)
        if v is not None and v > 0: return float(v)
        print("Please enter a positive number (e.g., 1000, 1500.5).")

def _prompt_tp_percent(default_tp_pct: float) -> float:
    default_str = f"{default_tp_pct*100:.2f}"
    while True:
        try: raw = input(f"TP percent (numbers only) [default {default_str}%]: ").strip()
        except EOFError: raw = ""
        if not raw: return float(default_tp_pct)
        v = _parse_numbers_only(raw)
        if v is not None and v >= 0: return float(v)/100.0
        print("Please enter a non-negative number (e.g., 1, 2.5).")

def round_down_to_tick(x: float, tick: float) -> float:
    if tick <= 0: return float(x)
    return math.floor(x / tick) * tick

def round_up_to_tick(x: float, tick: float) -> float:
    if tick <= 0: return float(x)
    return math.ceil(x / tick) * tick

def snap_to_tick(x: float, tick: float) -> float:
    if tick <= 0: return float(x)
    return round(x / tick) * tick

def _row_min_cost(px: float, min_notional: Optional[float], min_qty: Optional[float]) -> float:
    """ขั้นต่ำต่อแถว ณ ราคา px (จาก exchange จริง) + buffer 1% แล้วปัดขึ้น 1 cent"""
    req1 = float(min_notional or 0.0)
    req2 = float(px) * float(min_qty or 0.0)
    y = max(req1, req2)
    return float(np.ceil((y * 1.01) * 100.0) / 100.0)

def _zone_label(px: float, spot: float, used_low: float, used_high: float, z_near: float, z_mid: float) -> str:
    if px <= spot:
        denom = max(spot - used_low, 1e-12)
        r = (spot - px) / denom
    else:
        denom = max(used_high - spot, 1e-12)
        r = (px - spot) / denom
    if r <= z_near: return "near"
    if r <= z_mid:  return "mid"
    return "far"

# ============== Single-panel allocator (descending slope) ==============
def allocate_single_panel_desc(levels: np.ndarray,
                               budget: float,
                               min_notional: Optional[float],
                               min_qty: Optional[float],
                               qty_step: float) -> Tuple[np.ndarray, np.ndarray]:
    """
    คืน (usd_alloc, coin_size) โดย:
      - ขั้นต่ำต่อแถวจากตลาดจริง (minNotional/minQty)
      - เงินต่อแถว "ลดลง" ตามราคา (ล่างมากสุด → บนน้อยสุด)
      - เคารพ LOT_SIZE และพยายามบังคับโมโนโทนไม่เพิ่มขึ้นตามราคา
      - binary search หา slope ให้รวม ≈ budget
      - ถ้างบยังไม่พอขั้นต่ำทุกชั้น → ตัดชั้น "บนสุด" ออกจนพอดี
    """
    K = len(levels)
    if K < 1:
        return np.array([]), np.array([])

    step = max(float(qty_step), 1e-12)

    # ขั้นต่ำต่อแถว
    row_min = np.array([_row_min_cost(float(p), min_notional, min_qty) for p in levels], dtype=float)

    # ถ้างบไม่พอขั้นต่ำทั้งหมด → ตัดจาก "ปลายบน" ออกก่อน
    keep = np.arange(K, dtype=int)
    while row_min.sum() - 1e-9 > budget and len(keep) > 1:
        keep = keep[:-1]  # ตัด index สุดท้าย (ราคาแพงสุด)
        levels = levels[keep]
        row_min = row_min[keep]

    if len(levels) == 0:
        return np.array([]), np.array([])

    K = len(levels)
    if K == 1:
        usd = min(row_min[0], budget)
        size = math.floor(usd / max(levels[0],1e-12) / step) * step
        if (min_qty or 0.0) > 0 and size < float(min_qty):
            size = math.ceil(float(min_qty)/step) * step
        usd = size * levels[0]
        return np.array([usd]), np.array([size])

    # ให้ S เป็นส่วนเพิ่มฝั่งล่าง แล้วไล่ "ลด" ไปจนบน
    # factor_i = (K-1-i)/(K-1)  -> i=0 (ล่าง) = 1, i=K-1 (บน) = 0
    def sum_given_S(S: float, return_series=False):
        prev_usd = float('inf')
        usd_list, size_list = [], []
        for i, px in enumerate(levels):
            base = row_min[i]
            factor = (K - 1 - i) / (K - 1)
            target = base + S * factor           # ล่างมากสุด → บนน้อยสุด
            cap_target = min(target, prev_usd)   # โมโนโทนไม่เพิ่มขึ้นตามราคา

            size = math.floor(cap_target / max(px,1e-12) / step) * step
            if (min_qty or 0.0) > 0 and size < float(min_qty):
                size = math.ceil(float(min_qty)/step) * step
            usd = size * px

            # บังคับ >= ขั้นต่ำ
            if usd + 1e-12 < row_min[i]:
                size = math.ceil(row_min[i] / max(px,1e-12) / step) * step
                usd = size * px

            # โมโนโทนแบบเข้มขึ้น: ถ้าดันเกิน prev_usd (เจอกรณี minQty*price ครอง) → clamp ลงเท่าที่ LOT อนุญาต
            if usd > prev_usd + 1e-12:
                size = math.floor(prev_usd / max(px,1e-12) / step) * step
                usd = size * px
                # ถ้าคลัมป์แล้วต่ำกว่าขั้นต่ำอีก ก็ยอม "หลุดโมโนโทน" ตรงนี้เพื่อเคารพขั้นต่ำ
                if usd + 1e-12 < row_min[i]:
                    size = math.ceil(row_min[i] / max(px,1e-12) / step) * step
                    usd = size * px
            prev_usd = usd
            usd_list.append(usd); size_list.append(size)
        tot = float(np.sum(usd_list))
        if return_series:
            return tot, np.array(usd_list), np.array(size_list)
        return tot

    # หา S ด้วย binary search
    S_lo, S_hi = 0.0, 1.0
    # ขยาย S_hi จนรวม >= budget หรือถึงเพดาน
    while sum_given_S(S_hi) < budget and S_hi < 1e9:
        S_hi *= 2.0
    for _ in range(50):
        S_mid = 0.5 * (S_lo + S_hi)
        s = sum_given_S(S_mid)
        if s > budget:
            S_hi = S_mid
        else:
            S_lo = S_mid
    _, usd_final, size_final = sum_given_S(S_lo, return_series=True)

    # เติมเศษงบที่เหลือให้ "แถวล่างสุด" เท่านั้น (ยังคงโมโนโทน)
    remain = budget - float(np.sum(usd_final))
    if remain > 0 and K >= 1:
        i = 0  # ล่างสุด
        px = levels[i]
        add = math.floor(remain / max(px,1e-12) / step) * step
        if add > 0:
            size_final[i] += add
            usd_final[i] = size_final[i] * px

    return usd_final, size_final

# ============== CSV Export (macro) ==============
def export_macro_csv(path_min: np.ndarray, path_max: np.ndarray, cfg_out: dict, out_path: str):
    paths_df = pd.DataFrame({
        "path_id": np.arange(len(path_min), dtype=int),
        "min_price": path_min.astype(float),
        "max_price": path_max.astype(float),
    })
    summ = cfg_out["_summary"]
    summ_df = pd.DataFrame([{
        "path_id": np.nan, "min_price": np.nan, "max_price": np.nan,
        "spot": float(summ["spot"]),
        "band_low": float(summ["band_low"]),
        "band_high": float(summ["band_high"]),
        "min_p10": float(summ["min_pct"][0]),
        "min_p50": float(summ["min_pct"][1]),
        "min_p90": float(summ["min_pct"][2]),
        "max_p10": float(summ["max_pct"][0]),
        "max_p50": float(summ["max_pct"][1]),
        "max_p90": float(summ["max_pct"][2]),
        "symbol": cfg_out["symbol"],
        "timeframe": cfg_out["timeframe"],
        "lookback_bars": int(cfg_out["lookback_bars"]),
        "paths": int(cfg_out["paths"]),
        "horizon_bars": int(cfg_out["horizon_bars"]),
        "block_len": int(cfg_out["block_len"]),
        "band_lo_pct": int(cfg_out["band_percentiles"][0]),
        "band_hi_pct": int(cfg_out["band_percentiles"][1]),
        "lower_level": float(cfg_out["lower_level"]),
        "upper_level": float(cfg_out["upper_level"]),
        "grid_min_step": float(cfg_out["grid_min_step"]),
        "max_layers": int(cfg_out["max_layers"]),
        "price_tick": float(cfg_out["price_tick"]),
        "qty_step": float(cfg_out["qty_step"]),
        "generated_at_gmt7": datetime.now(ZoneInfo("Asia/Bangkok")).strftime('%Y-%m-%d %H:%M:%S %Z'),
    }])
    out_all = pd.concat([paths_df, summ_df], ignore_index=True)
    out_all.to_csv(out_path, index=False)

# ============== MAIN ==============
def main():
    # รับค่าจากผู้ใช้
    sym = _prompt_symbol(CFG["symbol"])
    budget = _prompt_budget(CFG["BUDGET_USD"])
    tp_pct = _prompt_tp_percent(CFG["TP_PCT"])

    c = dict(CFG)
    c["symbol"] = sym
    c["BUDGET_USD"] = float(budget)
    c["TP_PCT"] = float(tp_pct)

    rng = np.random.default_rng()

    # ccxt
    bx = BinanceClient(timeout_ms=c["http_timeout_ms"])
    bx.load_markets(retries=c["http_retries"], backoff=c["retry_backoff_base_sec"])
    if c["symbol"] not in bx.markets:
        raise ValueError(f"Symbol '{c['symbol']}' not found on Binance spot.")

    m = bx.market(c["symbol"])
    min_notional, min_qty, price_tick, qty_step = _extract_meta_from_market(m)

    # OHLCV
    print(f"[i] Fetching {c['symbol']} {c['timeframe']} OHLCV (last {c['lookback_bars']} bars)...")
    df = bx.fetch_ohlcv_1d(c["symbol"], c["lookback_bars"], c["http_retries"], c["retry_backoff_base_sec"])
    close = df["close"].to_numpy(dtype=float)
    spot = float(close[-1])
    logret = np.diff(np.log(close))
    print(f"[i] Spot: {spot:.6f}")
    print(f"[i] Meta: tick={price_tick}, qty_step={qty_step}, min_notional≈{min_notional if min_notional is not None else 'N/A'} USDT, min_qty≈{min_qty if min_qty is not None else 'N/A'}")
    print(f"[i] Budget: {c['BUDGET_USD']:,.2f} USDT")
    print(f"[i] TP: {c['TP_PCT']*100:.2f}%")

    # Monte Carlo
    print(f"[i] MC: paths={c['paths']:,}, horizon={c['horizon_bars']}, block_len={c['block_len']}")
    log_paths = block_bootstrap_returns(logret, c["horizon_bars"], c["block_len"], c["paths"], rng)
    prices = simulate_price_paths(spot, log_paths)
    p_lo, p_hi = c["band_percentiles"]
    summ = summarize_macro(prices, spot, p_lo=p_lo, p_hi=p_hi)
    band_low, band_high = float(summ["band_low"]), float(summ["band_high"])
    width = band_high - band_low

    # frame จาก MC
    grid_min_step = float(price_tick) if price_tick and price_tick > 0 else 0.1
    lower_level = round_down_to_tick(band_low, grid_min_step)
    upper_level = round_up_to_tick(band_high, grid_min_step)
    if lower_level >= spot:
        lower_level = max(0.0, round_down_to_tick(spot - grid_min_step, grid_min_step))
    if upper_level <= spot:
        upper_level = round_up_to_tick(spot + grid_min_step, grid_min_step)

    theoretical_layers = max(0, int(math.floor((upper_level - lower_level) / max(grid_min_step, 1e-12))) + 1)
    max_layers = int(min(c["MAX_LAYERS_CAP"], theoretical_layers))

    # macro CSV
    cfg_out = dict(c)
    cfg_out.update({
        "lower_level": lower_level,
        "upper_level": upper_level,
        "grid_min_step": grid_min_step,
        "max_layers": max_layers,
        "price_tick": price_tick,
        "qty_step": qty_step,
        "_summary": {
            "spot": summ["spot"], "min_pct": summ["min_pct"], "max_pct": summ["max_pct"],
            "band_low": band_low, "band_high": band_high,
        }
    })
    export_macro_csv(summ["path_min"], summ["path_max"], cfg_out, c["macro_csv"])
    print(f"[i] Saved macro: {c['macro_csv']}")

    # ===== เลือกจำนวนชั้นคร่าว ๆ แล้วสร้างเลเวลแบบ equal-step ทั้งช่วง =====
    w_min_top = _row_min_cost(upper_level, min_notional, min_qty)  # โดยมากโดน minNotional ครอง
    target_each = w_min_top * max(1.0, float(c["TARGET_MIN_MULTIPLIER"]))
    K_est = int(max(2, min(max_layers, math.floor(c["BUDGET_USD"] / max(target_each, 1e-9)))))
    if int(c.get("LEVELS_CAP") or 0) > 0:
        K_est = min(K_est, int(c["LEVELS_CAP"]))
    if K_est < 2:
        K_est = 2

    raw_levels = np.linspace(lower_level, upper_level, K_est)
    levels = np.array([snap_to_tick(x, price_tick) for x in raw_levels], dtype=float)
    levels = np.unique(levels)
    if len(levels) < 2:
        levels = np.array([lower_level, upper_level], dtype=float)

    # ===== Single-panel (descending slope) =====
    usd_alloc, coin_size = allocate_single_panel_desc(
        levels=levels,
        budget=float(c["BUDGET_USD"]),
        min_notional=min_notional,
        min_qty=min_qty,
        qty_step=qty_step
    )

    if len(usd_alloc) == 0:
        print("\nคำสั่งซื้อ (Orders): ไม่มีออเดอร์ที่สร้างได้จากข้อจำกัดงบ/ขั้นต่ำในตอนนี้")
        return

    # labels เพื่อแสดงผล
    zones, sides = [], []
    for px in levels:
        side = "below" if px < spot else ("above" if px > spot else "at")
        sides.append(side if side != "at" else ("below" if px <= spot else "above"))
        zones.append(_zone_label(px, spot, lower_level, upper_level, c["ZONE_NEAR"], c["ZONE_MID"]))

    tp_price = levels * (1.0 + c["TP_PCT"])
    net_pct_est = np.maximum(c["TP_PCT"] - 2.0 * c["FEE_RATE"], 0.0)

    orders_df = pd.DataFrame({
        "side": sides,
        "zone": zones,
        "buy_price": levels.astype(float),
        "usd_alloc": usd_alloc.astype(float),
        "coin_size": coin_size.astype(float),
        "tp_price": tp_price.astype(float),
        "net_pct_est": net_pct_est,
    }).sort_values("buy_price").reset_index(drop=True)

    # รายงาน
    print("\n=== แผนวางกริด (แผงเดียว: เงินลดลงตามราคา) ===")
    print(f"ราคาปัจจุบัน (Spot): {spot:.6f} | ช่วงกรอบใช้จริง (Band): {lower_level:.6f} → {upper_level:.6f}")
    print(f"เลเวลทั้งหมด: {len(orders_df)} ชั้น | ใช้งบรวม ≈ {orders_df['usd_alloc'].sum():,.2f}/{c['BUDGET_USD']:,.2f} USDT\n")
    print("คำสั่งซื้อ (Orders):")
    print("SIDE | ZONE | buy_price | usd_alloc | coin_size | tp_price | net_pct_est")
    for _, r in orders_df.iterrows():
        print(f"{str(r['side']):>5} | {str(r['zone']):>4} | {float(r['buy_price']):.6f} | {float(r['usd_alloc']):>8.2f} "
              f"| {float(r['coin_size']):>9.6f} | {float(r['tp_price']):.6f} | {float(r['net_pct_est']):.4%}")

    # Save
    orders_df.to_csv(c["grid_csv"], index=False)
    print(f"\n[i] บันทึกแผนกริดลงไฟล์: {c['grid_csv']} เรียบร้อยแล้ว")

    # สรุป
    print("\n=== Summary (AUTO) ===")
    print(f" Spot: {spot:.6f}")
    print(f" Band p{p_lo}..p{p_hi}: {band_low:.6f} → {band_high:.6f} (width {width:.6f})")
    print(f" AUTO lower_level: {lower_level:.6f} | AUTO upper_level: {upper_level:.6f}")
    print(f" AUTO grid_min_step (price tick): {grid_min_step}")
    print(f" AUTO max_layers: {max_layers}  |  Levels used: {len(orders_df)}")
    print(f" Done at (GMT+7): {datetime.now(ZoneInfo('Asia/Bangkok')).strftime('%Y-%m-%d %H:%M:%S %Z')}")

if __name__ == "__main__":
    main()
