# run_macro_and_build_grid.py
"""
Pipeline: Monte Carlo (AUTO) -> Grid Plan (AUTO)
- ดึง OHLCV 1D จาก Binance + retry/backoff
- Block-bootstrap Monte Carlo บน log-returns
- คำนวณ band จาก percentiles ของ min/max ราคาในอนาคต
- AUTO:
    * lower_level  = floor_to_tick(band_low)
    * upper_level  = ceil_to_tick(band_high)
    * grid_min_step = PRICE_TICK จาก Binance
    * max_layers    = clamp(floor((upper-lower)/step), [MIN_LAYERS_FLOOR, MAX_LAYERS_CAP])
- เขียน macro_montecarlo.csv (path-level + summary 1 แถว)
- วางกริดแบบ split โดยอิงงบ/ขั้นต่ำตลาด ณ “จุดไกลสุด” ของแต่ละฝั่ง
- เขียน grid_plan.csv

ติดตั้ง: pip install ccxt pandas numpy
"""

import os
import time
import math
import numpy as np
import pandas as pd
import ccxt
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, Tuple, Optional

# ====== USER CONFIG ======
CFG = {
    # ---- Market / MC ----
    "symbol": "XRP/USDT",
    "timeframe": "1d",
    "lookback_bars": 1000,
    "paths": 10_000,
    "horizon_bars": 60,
    "block_len": 24,
    "band_percentiles": (10, 90),      # p10 of path MIN, p90 of path MAX
    "MAX_LAYERS_CAP": 200,
    "MIN_LAYERS_FLOOR": 10,
    "http_timeout_ms": 30000,
    "http_retries": 5,
    "retry_backoff_base_sec": 1.0,

    # ---- Grid planner (split) ----
    "GRID_MODE": "split",              # "split" | "symmetric"
    "LEVELS_LOWER_WEIGHT": 50,         # ยังคงไว้เพื่อความเข้ากันได้ ถ้ากลับไปแบ่งตามจำนวนชั้นในอนาคต
    "BUDGET_LOWER_WEIGHT": None,       # None = ใช้ค่าเดียวกับ LEVELS_LOWER_WEIGHT
    "MIN_SIDE_LEVELS": 1,
    "LOWER_STRETCH_PCT": 15.0,         # ยืดฝั่งล่างลึกกว่าบน (เช่น 15 = 15%)
    "BUDGET_USD": 300.0,
    "FEE_RATE": 0.0004,
    "TP_PCT": 0.01,
    "ZONE_NEAR": 0.05,
    "ZONE_MID": 0.15,
    "WEIGHT_SCHEME": "far_heavier",
    "ENFORCE_MIN_NOTIONAL": True,
    "DROP_POLICY": "farthest",         # "farthest" | "smallest"

    # ---- Outputs ----
    "macro_csv": "macro_montecarlo.csv",
    "grid_csv": "grid_plan.csv",
}

# ====== External dependency for allocation ======
from popquants_grid_allocator import build_grid


# ====== Retry helper ======
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


# ====== Binance client (reuse one instance) ======
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
            raise RuntimeError("markets not loaded; call load_markets() first")
        return self.ex.market(symbol)

    def fetch_ohlcv_1d(self, symbol: str, lookback_bars: int, retries: int, backoff: float) -> pd.DataFrame:
        raw = _with_retries(self.ex.fetch_ohlcv, retries, backoff, symbol=symbol, timeframe="1d", limit=lookback_bars)
        if not raw or len(raw) < 50:
            raise RuntimeError("Not enough OHLCV returned.")
        df = pd.DataFrame(raw, columns=["ts", "open", "high", "low", "close", "volume"])
        df["dt"] = pd.to_datetime(df["ts"], unit="ms", utc=True).dt.tz_convert("Asia/Bangkok")
        return df[["dt", "open", "high", "low", "close", "volume"]]


def _extract_meta_from_market(m: dict) -> Tuple[Optional[float], Optional[float], float, float]:
    """Return (min_notional, min_qty, price_tick, qty_step)"""
    # price_tick / qty_step
    tick_size, qty_step = None, None
    filters = (m.get("info", {}) or {}).get("filters", []) or []
    for f in filters:
        t = f.get("filterType")
        if t == "PRICE_FILTER":
            try:
                tick_size = float(f.get("tickSize", "0.0001"))
            except Exception:
                pass
        if t == "LOT_SIZE":
            try:
                qty_step = float(f.get("stepSize", "0.0001"))
            except Exception:
                pass
    if tick_size is None:
        pr = (m.get("precision", {}) or {}).get("price", 4)
        tick_size = 10 ** (-int(pr))
    if qty_step is None:
        pr = (m.get("precision", {}) or {}).get("amount", 4)
        qty_step = 10 ** (-int(pr))

    # min_notional / min_qty
    min_notional, min_qty = None, None
    limits = m.get("limits", {}) or {}
    cost = limits.get("cost", {}) or {}
    amt = limits.get("amount", {}) or {}
    try:
        if cost.get("min") is not None:
            min_notional = float(cost["min"])
    except Exception:
        pass
    try:
        if amt.get("min") is not None:
            min_qty = float(amt["min"])
    except Exception:
        pass

    if min_notional is None or min_qty is None:
        for f in filters:
            t = f.get("filterType")
            if (t in ("MIN_NOTIONAL", "NOTIONAL")) and (min_notional is None):
                raw = f.get("minNotional") or f.get("notional")
                try:
                    if raw is not None:
                        min_notional = float(raw)
                except Exception:
                    pass
            if t == "LOT_SIZE" and (min_qty is None):
                raw = f.get("minQty")
                try:
                    if raw is not None:
                        min_qty = float(raw)
                except Exception:
                    pass

    return (float(min_notional) if min_notional is not None else None,
            float(min_qty) if min_qty is not None else None,
            float(tick_size), float(qty_step))


# ====== MC core ======
def block_bootstrap_returns(ret: np.ndarray, horizon: int, block_len: int, paths: int, rng: np.random.Generator):
    n = len(ret)
    if n < block_len + 5:
        raise ValueError("Return history too short for chosen block_len.")
    out = np.empty((paths, horizon), dtype=float)
    valid_starts = np.arange(0, n - block_len + 1)
    for p in range(paths):
        # ต่อบล็อกให้ครบ horizon
        seg = []
        while len(seg) < horizon:
            start = rng.choice(valid_starts)
            seg.extend(ret[start:start+block_len])
        out[p, :] = np.array(seg[:horizon])
    return out


def simulate_price_paths(spot: float, logret_paths: np.ndarray) -> np.ndarray:
    cum = np.cumsum(logret_paths, axis=1)
    return spot * np.exp(cum)


def summarize_macro(prices: np.ndarray, spot: float, p_lo=10, p_hi=90):
    path_min = prices.min(axis=1)
    path_max = prices.max(axis=1)
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


# ====== Grid helpers ======
def round_down_to_tick(x: float, tick: float) -> float:
    if tick <= 0: return float(x)
    return math.floor(x / tick) * tick


def round_up_to_tick(x: float, tick: float) -> float:
    if tick <= 0: return float(x)
    return math.ceil(x / tick) * tick


def _clamp01(x: float) -> float:
    return max(0.0, min(1.0, float(x)))


def _split_budget(weight_lower_pct: Optional[float], total_budget: float) -> Tuple[float, float, float]:
    ratio_lower = _clamp01((weight_lower_pct / 100.0) if (weight_lower_pct is not None) else 0.5)
    b_lo = float(total_budget * ratio_lower)
    b_up = float(total_budget - b_lo)
    return ratio_lower, b_lo, b_up


def _side_of_order(px: float, spot: float, eps: float) -> str:
    if px <= spot - eps: return "below"
    if px >= spot + eps: return "above"
    return "below" if px < spot else "above"


def _ceil_cent(x: float, add_buffer_pct: float = 0.01) -> float:
    y = float(x) * (1.0 + max(0.0, add_buffer_pct))
    return float(np.ceil(y * 100.0) / 100.0)


def _w_min_at_price(px: float, min_notional: Optional[float], min_qty: Optional[float]) -> float:
    req1 = float(min_notional) if (min_notional is not None) else 0.0
    req2 = (float(px) * float(min_qty)) if (min_qty is not None) else 0.0
    return _ceil_cent(max(req1, req2), add_buffer_pct=0.01)


def _bump_to_min_notional(row, w_min: float, qstep: float):
    px = float(row["buy_price"])
    cur_qty = float(row["coin_size"])
    need_qty = np.ceil((w_min / max(px, 1e-12)) / qstep) * qstep
    if cur_qty + 1e-12 >= need_qty:
        return row, 0.0
    row["coin_size"] = float(need_qty)
    row["usd_alloc"] = float(need_qty * px)
    return row, float(row["usd_alloc"] - cur_qty * px)


def _enforce_min_notional_per_side(df_side: pd.DataFrame,
                                   budget_side: float,
                                   w_min: float,
                                   qstep: float,
                                   spot: float,
                                   side_name: str,
                                   drop_policy: str) -> pd.DataFrame:
    # 1) ยกออเดอร์ให้ผ่านขั้นต่ำ
    rows = []
    for _, r in df_side.iterrows():
        r2, _ = _bump_to_min_notional(r.copy(), w_min, qstep)
        rows.append(r2)
    df_side = pd.DataFrame(rows)

    # 2) ถ้าเกินงบ → ตัดออก
    def _dist(px: float) -> float:
        return (spot - px) if side_name == "below" else (px - spot)

    while df_side["usd_alloc"].sum() > budget_side and len(df_side) > 1:
        if drop_policy == "smallest":
            drop_idx = df_side["usd_alloc"].idxmin()
        else:  # farthest
            drop_idx = df_side["buy_price"].apply(_dist).idxmax()
        df_side = df_side.drop(index=drop_idx)

    return df_side


# ====== CSV export (macro) ======
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


# ====== MAIN PIPELINE ======
def main():
    c = CFG
    rng = np.random.default_rng()

    # -- Single CCXT client --
    bx = BinanceClient(timeout_ms=c["http_timeout_ms"])
    bx.load_markets(retries=c["http_retries"], backoff=c["retry_backoff_base_sec"])
    m = bx.market(c["symbol"])
    min_notional, min_qty, price_tick, qty_step = _extract_meta_from_market(m)

    # -- OHLCV --
    print(f"[i] Fetching {c['symbol']} {c['timeframe']} OHLCV (last {c['lookback_bars']} bars)...")
    df = bx.fetch_ohlcv_1d(c["symbol"], c["lookback_bars"], c["http_retries"], c["retry_backoff_base_sec"])
    close = df["close"].to_numpy(dtype=float)
    spot = float(close[-1])
    logret = np.diff(np.log(close))
    print(f"[i] Spot: {spot:.6f}")
    print(f"[i] Meta: tick={price_tick}, qty_step={qty_step}, min_notional≈{min_notional if min_notional is not None else 'N/A'} USDT, min_qty≈{min_qty if min_qty is not None else 'N/A'}")

    # -- Monte Carlo --
    print(f"[i] MC: paths={c['paths']:,}, horizon={c['horizon_bars']}, block_len={c['block_len']}")
    log_paths = block_bootstrap_returns(logret, c["horizon_bars"], c["block_len"], c["paths"], rng)
    prices = simulate_price_paths(spot, log_paths)
    p_lo, p_hi = c["band_percentiles"]
    summ = summarize_macro(prices, spot, p_lo=p_lo, p_hi=p_hi)
    band_low, band_high = float(summ["band_low"]), float(summ["band_high"])
    width = band_high - band_low

    # -- AUTO params from MC/market tick --
    grid_min_step = float(price_tick) if price_tick and price_tick > 0 else 0.1
    lower_level = round_down_to_tick(band_low, grid_min_step)
    upper_level = round_up_to_tick(band_high, grid_min_step)
    if lower_level >= spot:
        lower_level = max(0.0, round_down_to_tick(spot - grid_min_step, grid_min_step))
    if upper_level <= spot:
        upper_level = round_up_to_tick(spot + grid_min_step, grid_min_step)

    theoretical_layers = max(0, int(math.floor((upper_level - lower_level) / max(grid_min_step, 1e-12))) - 1)
    max_layers = int(max(c["MIN_LAYERS_FLOOR"], min(c["MAX_LAYERS_CAP"], theoretical_layers)))

    # -- Write macro CSV (for audit / downstream compatibility) --
    cfg_out = dict(c)
    cfg_out.update({
        "lower_level": lower_level,
        "upper_level": upper_level,
        "grid_min_step": grid_min_step,
        "max_layers": max_layers,
        "price_tick": price_tick,
        "qty_step": qty_step,
        "_summary": {
            "spot": summ["spot"],
            "min_pct": summ["min_pct"],
            "max_pct": summ["max_pct"],
            "band_low": band_low,
            "band_high": band_high,
        }
    })
    export_macro_csv(summ["path_min"], summ["path_max"], cfg_out, c["macro_csv"])
    print(f"[i] Saved macro: {c['macro_csv']}")

    # ===== GRID PLANNER =====
    GRID_MODE = c["GRID_MODE"]
    LOWER_STRETCH_PCT = float(c["LOWER_STRETCH_PCT"])
    _stretch_factor = 1.0 + max(0.0, LOWER_STRETCH_PCT) / 100.0

    # กรอบใช้งานจริง (ยืดล่างมากกว่าบนเล็กน้อย)
    delta_up = max(1e-9, band_high - spot)
    delta_dn = max(1e-9, spot - band_low)
    half_sym = max(delta_up, delta_dn)
    used_low = max(0.0, spot - half_sym * _stretch_factor)
    used_high = spot + half_sym

    # แบ่งงบ
    ratio_lower, budget_below, budget_above = _split_budget(
        c["BUDGET_LOWER_WEIGHT"] if c["BUDGET_LOWER_WEIGHT"] is not None else c["LEVELS_LOWER_WEIGHT"],
        c["BUDGET_USD"]
    )
    EPS = price_tick * 0.5

    # w_min ณ จุดไกลสุดแต่ละฝั่ง
    below_far_price = used_low if GRID_MODE == "split" else max(0.0, spot - half_sym * _stretch_factor)
    above_far_price = used_high if GRID_MODE == "split" else (spot + half_sym)
    w_min_below_far = _w_min_at_price(below_far_price, min_notional, min_qty)
    w_min_above_far = _w_min_at_price(above_far_price, min_notional, min_qty)

    K_cap_below = int(np.floor(budget_below / max(w_min_below_far, 1e-12)))
    K_cap_above = int(np.floor(budget_above / max(w_min_above_far, 1e-12)))

    def _apply_min_each(k_cap: int, budget_side: float, w_min_side: float) -> int:
        if k_cap <= 0: return 0
        if c["MIN_SIDE_LEVELS"] <= 1: return k_cap
        if budget_side >= (w_min_side * c["MIN_SIDE_LEVELS"]):
            return max(c["MIN_SIDE_LEVELS"], k_cap)
        return k_cap

    K_below = _apply_min_each(K_cap_below, budget_below, w_min_below_far)
    K_above = _apply_min_each(K_cap_above, budget_above, w_min_above_far)

    print("\n[i] Grid frame (used):")
    print(f"    spot        : {spot:.6f}")
    print(f"    MC band     : {band_low:.6f} → {band_high:.6f}")
    print(f"    USED band   : {used_low:.6f} → {used_high:.6f}   (mode={GRID_MODE}, lower stretch +{LOWER_STRETCH_PCT:.1f}%)")

    print("\n[i] Max feasible levels from budget + min at far point:")
    print(f"    below: cap={K_cap_below} (w_min≈{w_min_below_far:.2f} @ px≈{below_far_price:.6f})  budget={budget_below:.2f}")
    print(f"    above: cap={K_cap_above} (w_min≈{w_min_above_far:.2f} @ px≈{above_far_price:.6f})  budget={budget_above:.2f}")

    if K_below == 0: print("[warn] Budget below side insufficient for 1 order at far point.")
    if K_above == 0: print("[warn] Budget above side insufficient for 1 order at far point.")

    orders = []

    if GRID_MODE == "split":
        # BELOW
        if K_below > 0 and budget_below > 0:
            res_below = build_grid(
                spot=spot, band_low=used_low, band_high=spot,
                budget_usd=budget_below, K=K_below,
                method="equal_prob", mc_mins_samples=summ["path_min"],  # ใช้ตัวอย่าง path-level ที่คำนวณมาแล้ว
                alpha=0.8, w_min=w_min_below_far, w_max=max(w_min_below_far, 50.0),
                fee_rate=c["FEE_RATE"], tp_pct=c["TP_PCT"],
                zone_near=c["ZONE_NEAR"], zone_mid=c["ZONE_MID"], weight_scheme=c["WEIGHT_SCHEME"],
                price_tick=price_tick, qty_step=qty_step
            )
            orders_below = [o for o in (res_below.get("orders") or []) if o.get("buy_price", 0.0) <= (spot - EPS)]
        else:
            orders_below = []

        # ABOVE
        if K_above > 0 and budget_above > 0:
            res_above = build_grid(
                spot=spot, band_low=spot - EPS, band_high=used_high,
                budget_usd=budget_above, K=K_above,
                method="equal_prob", mc_mins_samples=summ["path_min"],
                alpha=0.8, w_min=w_min_above_far, w_max=max(w_min_above_far, 50.0),
                fee_rate=c["FEE_RATE"], tp_pct=c["TP_PCT"],
                zone_near=c["ZONE_NEAR"], zone_mid=c["ZONE_MID"], weight_scheme=c["WEIGHT_SCHEME"],
                price_tick=price_tick, qty_step=qty_step
            )
            orders_above_raw = res_above.get("orders") or []
            orders_above = [o for o in orders_above_raw if o.get("buy_price", 0.0) >= (spot + EPS)]
            if not orders_above and orders_above_raw:
                # กันกรณี rounding
                orders_above = [o for o in orders_above_raw if o.get("buy_price", 0.0) >= spot]
        else:
            orders_above = []

        orders = orders_below + orders_above

    else:  # symmetric (สำรอง)
        K_total_auto = max(0, K_below) + max(0, K_above)
        if K_total_auto > 0:
            res = build_grid(
                spot=spot,
                band_low=max(0.0, spot - half_sym * _stretch_factor),
                band_high=spot + half_sym,
                budget_usd=c["BUDGET_USD"], K=K_total_auto,
                method="equal_prob", mc_mins_samples=summ["path_min"],
                alpha=0.8, w_min=min(w_min_below_far, w_min_above_far),
                w_max=max(w_min_below_far, w_min_above_far, 50.0),
                fee_rate=c["FEE_RATE"], tp_pct=c["TP_PCT"],
                zone_near=c["ZONE_NEAR"], zone_mid=c["ZONE_MID"], weight_scheme=c["WEIGHT_SCHEME"],
                price_tick=price_tick, qty_step=qty_step
            )
            orders = res.get("orders") or []
        else:
            orders = []

    # ===== Post-process & enforce =====
    print("\n=== แผนวางกริด (โหมด: %s) ===" % GRID_MODE)
    print(f"ราคาปัจจุบัน (Spot): {spot:.6f} | ช่วงกรอบใช้จริง (Band): {used_low:.6f} → {used_high:.6f}")
    print(f"เพดานชั้นที่ใช้จริง  lower/upper = {K_below}/{K_above}  |  งบ lower/upper = {budget_below:.2f}/{budget_above:.2f} USDT")

    orders_df = pd.DataFrame(orders).copy()
    if not orders_df.empty and {"tp_price", "buy_price"}.issubset(orders_df.columns):
        bad = orders_df["tp_price"] < orders_df["buy_price"]
        if bad.any():
            orders_df.loc[bad, "tp_price"] = orders_df.loc[bad, "buy_price"] * (1.0 + c["TP_PCT"])

    if not orders_df.empty and "buy_price" in orders_df.columns:
        orders_df = orders_df.sort_values("buy_price").reset_index(drop=True)

    if not orders_df.empty:
        orders_df["side"] = orders_df["buy_price"].apply(lambda px: _side_of_order(px, spot, EPS))

    if c["ENFORCE_MIN_NOTIONAL"] and not orders_df.empty:
        below_df = orders_df[orders_df["side"] == "below"].copy()
        above_df = orders_df[orders_df["side"] == "above"].copy()

        if not below_df.empty:
            below_df = _enforce_min_notional_per_side(below_df, budget_below, w_min_below_far, qty_step, spot, "below", c["DROP_POLICY"])
        if not above_df.empty:
            above_df = _enforce_min_notional_per_side(above_df, budget_above, w_min_above_far, qty_step, spot, "above", c["DROP_POLICY"])

        orders_df = pd.concat([below_df, above_df], ignore_index=True)

        # safety re-check
        if {"tp_price", "buy_price"}.issubset(orders_df.columns):
            bad = orders_df["tp_price"] < orders_df["buy_price"]
            if bad.any():
                orders_df.loc[bad, "tp_price"] = orders_df.loc[bad, "buy_price"] * (1.0 + c["TP_PCT"])
        if "buy_price" in orders_df.columns:
            orders_df = orders_df.sort_values("buy_price").reset_index(drop=True)

        tot_below = orders_df.loc[orders_df["side"]=="below", "usd_alloc"].sum()
        tot_above = orders_df.loc[orders_df["side"]=="above", "usd_alloc"].sum()
        print("\n[i] หลัง enforce ขั้นต่ำ/คุมงบ:")
        print(f"    ฝั่งล่างใช้ {tot_below:.2f}/{budget_below:.2f} USDT | ฝั่งบนใช้ {tot_above:.2f}/{budget_above:.2f} USDT")

    # ===== Save grid plan =====
    if orders_df.empty:
        print("\nคำสั่งซื้อ (Orders): ไม่มีออเดอร์ที่สร้างได้จากข้อจำกัดงบและขั้นต่ำในตอนนี้")
    else:
        print("\nคำสั่งซื้อ (Orders):")
        print("SIDE | ZONE | buy_price | usd_alloc | coin_size | tp_price | net_pct_est")
        for _, r in orders_df.iterrows():
            side = r.get("side", "")
            zone = r.get("zone", "")
            print(f"{str(side):>5} | {str(zone):>4} | {float(r['buy_price']):.6f} | {float(r['usd_alloc']):>8.2f} "
                  f"| {float(r['coin_size']):>9.6f} | {float(r['tp_price']):.6f} | {float(r.get('net_pct_est', 0.0092)):.4%}")

    orders_df.to_csv(c["grid_csv"], index=False)
    print(f"\n[i] บันทึกแผนกริดลงไฟล์: {c['grid_csv']} เรียบร้อยแล้ว")

    # ===== Summary =====
    print("\n=== Summary (AUTO) ===")
    print(f" Spot: {spot:.6f}")
    print(f" Band p{p_lo}..p{p_hi}: {band_low:.6f} → {band_high:.6f} (width {width:.6f})")
    print(f" AUTO lower_level: {lower_level:.6f} | AUTO upper_level: {upper_level:.6f}")
    print(f" AUTO grid_min_step (price tick): {grid_min_step}")
    print(f" AUTO max_layers: {max_layers} (theoretical ~ {theoretical_layers})")
    print(f" Done at (GMT+7): {datetime.now(ZoneInfo('Asia/Bangkok')).strftime('%Y-%m-%d %H:%M:%S %Z')}")


if __name__ == "__main__":
    main()
