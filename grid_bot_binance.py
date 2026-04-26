#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Live 5s bars + Signal + Grid Execution (Spot Binance, ccxt)
- สัญญาณ: MAD-zscore (window=50), ยืนยัน 1 แท่ง
- ซื้อเฉพาะเมื่อใกล้กริด ≤ GRID_TOL
- กันเปิดซ้ำระดับ: pre-lock จาก open orders, ตรวจซ้ำก่อนยิง, map order→level
- BUY = MARKET 100% แล้ววาง TP เป็น LIMIT SELL ทันที

อัปเดตสำคัญ (insufficient fix + dust tracker + debug):
  • BUY ใช้ quoteOrderQty (ยอด USDT) เสมอ → ไม่ติด LOT_SIZE/minQty/minNotional ฝั่งซื้อ
  • คำนวณงบขั้นต่ำ = max(coin_size*px_ref, minNotional) + safety buffer แล้วปัดลงเป็นหน่วยเซ็นต์
  • ใส่ newOrderRespType='FULL' และ avg = cost/filled ถ้ามีข้อมูล
  • Dust tracker: บันทึกเศษลง dust_ledger.csv สำหรับ lot_rounding/minQty_gate/minNotional_gate/tp_place_failed
  • debug: [buy-debug], [tp-debug], และข้อความ skip ที่อธิบายเหตุผล
"""

import csv
import os
import time
import math
import ccxt
import numpy as np
import pandas as pd
from collections import deque
from typing import Optional, List, Dict, Set, Tuple
from datetime import datetime, timezone
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING, getcontext

# ===================== CONFIG =====================
getcontext().prec = 28
def _q(x) -> Decimal: return Decimal(str(x))

EXCHANGE_ID = "binance"

BOOK_LIMIT = 20
BAR_MS = 5_000
EMA_SPAN = 10

GRID_TOL = 0.002
LOCK_TOL = 0.0015
ALLOW_FALLBACK_EXEC = False

CONFIRM_BARS = 1
COOLDOWN_MS = 60_000

WINDOW = 50
CVD_Z_TH = 1.5
TS_Z_TH  = 1.5

MAX_OPEN_ORDERS = 30
GRID_CSV = "grid_plan.csv"
GRID_RELOAD_SEC = 0

DRY_RUN = False

MIN_NOTIONAL_OVERRIDE = None
SLIP_PCT = 0.0007

EXEC_FEE_RATE = 0.0004
TP_EXTRA_MARGIN = 0.0005
TP_BID_SAFETY_PCT = 0.0001

TRADES_POLL_MS = 1200
POLL_OPEN_ORDERS_SEC = 3.0
RESYNC_OPEN_ORDERS_SEC = 60

SHOW_PRELOCK_SUMMARY = False
SHOW_UNMAPPED_SELL_DEBUG = False

# Dust ledger
DUST_LEDGER_FILE = "dust_ledger.csv"

# ===================== UTILS =====================
def now_ms() -> int:
    return int(time.time() * 1000)

def ema_update(prev: Optional[float], x: float, span: int) -> float:
    if prev is None:
        return x
    alpha = 2.0 / (span + 1.0)
    return (1 - alpha) * prev + alpha * x

def sf(x, nd=5) -> str:
    try:
        v = float(x)
        if not np.isfinite(v): return "nan"
        return f"{v:.{nd}f}"
    except Exception:
        return "nan"

def _normalize_symbol(raw: str) -> str:
    s = (raw or "").strip().upper().replace("\\", "/").replace("-", "/")
    if not s: return s
    if "/" in s:
        base, quote = s.split("/", 1)
        base = base.strip()
        quote = (quote or "USDT").strip() or "USDT"
        return f"{base}/{quote}"
    if s.endswith("USDT"):
        base = s[:-4]
        return f"{base}/USDT"
    return f"{s}/USDT"

def detect_symbol_from_macro(macro_csv: str = "macro_montecarlo.csv") -> str:
    df = pd.read_csv(macro_csv)
    if "symbol" not in df.columns or df["symbol"].dropna().empty:
        raise RuntimeError("อ่าน symbol จาก macro_montecarlo.csv ไม่ได้")
    return _normalize_symbol(df["symbol"].dropna().astype(str).iloc[-1])

def load_grid_levels_from_csv(path: str) -> List[float]:
    try:
        gdf = pd.read_csv(path)
        return sorted([float(x) for x in gdf["buy_price"].tolist()])
    except Exception:
        return []

def load_grid_df(path: str) -> pd.DataFrame:
    gdf = pd.read_csv(path)
    if "tp_price" not in gdf.columns:
        if "tp_pct" in gdf.columns:
            gdf["tp_price"] = gdf["buy_price"] * (1.0 + gdf["tp_pct"].astype(float))
        else:
            gdf["tp_price"] = gdf["buy_price"] * 1.01
    return gdf[["buy_price", "coin_size", "tp_price"]].copy()

def match_grid_row(grid_df: pd.DataFrame, level: float, tol: float = 0.003) -> pd.Series:
    diffs = (grid_df["buy_price"] - level).abs()
    idx = diffs.idxmin()
    row = grid_df.loc[idx]
    if abs(row["buy_price"] - level) / max(level, 1e-9) > tol:
        return pd.Series({"buy_price": level, "coin_size": 0.0, "tp_price": level * 1.01})
    return row

def init_csv_logger(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    new_file = (not os.path.exists(path)) or os.path.getsize(path) == 0
    fh = open(path, "a", newline="")
    wr = csv.writer(fh)
    if new_file:
        wr.writerow([
            "bar_ts_ms","bar_time_utc","dq",
            "mid","cvd","cvd_z","ts_buy","ts_z",
            "confirm_count","buy_signal_raw","buy_signal_confirmed",
            "grid_candidate","action","reason",
            "bars_total","active_levels"
        ])
        fh.flush()
    return fh, wr

# ===================== DATA FETCHER =====================
class DataFetcher:
    def __init__(self, symbol: str, exchange_id: str = "binance", with_auth: bool = False):
        self.symbol = symbol
        kwargs = {"enableRateLimit": True}
        if with_auth:
            kwargs["apiKey"] = os.getenv("BINANCE_KEY", "")
            kwargs["secret"] = os.getenv("BINANCE_SECRET", "")
        self.exchange = getattr(ccxt, exchange_id)(kwargs)
        self.exchange.load_markets()
        self._imbalance_ema: Optional[float] = None
        self._last_trade_ts: Optional[int] = None
        self._last_trade_id: Optional[str] = None
        self._last_trades_fetch_ms: int = 0

    def fetch_orderbook(self) -> Dict:
        try:
            ob = self.exchange.fetch_order_book(self.symbol, limit=BOOK_LIMIT)
            ts = ob.get("timestamp") or now_ms()
            bids = ob.get("bids", []) or []
            asks = ob.get("asks", []) or []
            if not bids or not asks:
                return {"ok": False, "error": "empty_book"}
            best_bid = float(bids[0][0]); best_ask = float(asks[0][0])
            mid = (best_bid + best_ask) / 2.0

            depth_bid_5 = float(np.sum([lvl[1] for lvl in bids[:5]]))
            depth_ask_5 = float(np.sum([lvl[1] for lvl in asks[:5]]))
            total_bid_vol = float(np.sum([lvl[1] for lvl in bids]))
            total_ask_vol = float(np.sum([lvl[1] for lvl in asks]))

            denom = total_bid_vol + total_ask_vol
            imb_raw = 0.0 if denom <= 0 else (total_bid_vol - total_ask_vol) / denom
            imb_raw = float(np.clip(imb_raw, -0.99, 0.99))
            self._imbalance_ema = ema_update(self._imbalance_ema, imb_raw, EMA_SPAN)
            imb = float(self._imbalance_ema)

            return {
                "ok": True, "ts": ts,
                "best_bid": best_bid, "best_ask": best_ask, "mid_price": mid,
                "total_bid_volume": total_bid_vol, "total_ask_volume": total_ask_vol,
                "depth_bid_5": depth_bid_5, "depth_ask_5": depth_ask_5,
                "order_imbalance_raw": imb_raw, "order_imbalance": imb,
            }
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    def fetch_recent_trades(self, limit: int = 100) -> Dict:
        nowms = now_ms()
        if nowms - self._last_trades_fetch_ms < TRADES_POLL_MS:
            return {"ok": True, "trades": [], "error": None}
        self._last_trades_fetch_ms = nowms
        try:
            trades = self.exchange.fetch_trades(self.symbol, limit=limit)
            out = []
            for t in trades:
                tid = str(t.get("id", ""))
                ts_i = int(t.get("timestamp") or now_ms())
                if self._last_trade_ts is not None:
                    if ts_i < self._last_trade_ts: continue
                    if ts_i == self._last_trade_ts and self._last_trade_id and tid <= self._last_trade_id:
                        continue
                out.append({
                    "id": tid, "ts": ts_i,
                    "price": float(t["price"]), "amount": float(t["amount"]),
                    "cost": float(t["price"]) * float(t["amount"]),
                    "side": t.get("side"), "taker": t.get("takerOrMaker"),
                })
            if out:
                self._last_trade_ts = out[-1]["ts"]
                self._last_trade_id = out[-1]["id"]
            return {"ok": True, "trades": out, "error": None}
        except Exception as e:
            return {"ok": False, "trades": [], "error": f"{type(e).__name__}: {e}"}

# ===================== AGGREGATOR 5s =====================
class Aggregator5s:
    def __init__(self, bar_ms: int = 5000):
        self.bar_ms: int = bar_ms
        self.cur_window: Optional[int] = None
        self.ob_snaps: List[Dict] = []
        self.trades_buf: List[Dict] = []

    def _window_start(self, ts_ms: int) -> int:
        return ts_ms - (ts_ms % self.bar_ms)

    def add_orderbook_snapshot(self, snap: Dict) -> None:
        if not snap.get("ok"): return
        ts = int(snap["ts"])
        ws = self._window_start(ts)
        if self.cur_window is None:
            self.cur_window = ws
        self.ob_snaps.append(snap)

    def add_trades(self, payload: Dict) -> None:
        if not payload.get("ok"): return
        self.trades_buf.extend(payload["trades"])

    def roll_bar(self, now_ms_: int) -> Optional[Dict]:
        if self.cur_window is None:
            self.cur_window = self._window_start(now_ms_); return None
        if now_ms_ < self.cur_window + self.bar_ms: return None

        ob = pd.DataFrame(self.ob_snaps) if self.ob_snaps else pd.DataFrame()
        def mean_or_none(col: str) -> Optional[float]:
            return float(ob[col].mean()) if (not ob.empty and col in ob) else None

        mid  = mean_or_none("mid_price")
        tb   = mean_or_none("total_bid_volume")
        ta   = mean_or_none("total_ask_volume")
        db5  = mean_or_none("depth_bid_5")
        da5  = mean_or_none("depth_ask_5")
        imb  = mean_or_none("order_imbalance")

        tdf = pd.DataFrame(self.trades_buf) if self.trades_buf else pd.DataFrame()
        if not tdf.empty:
            trades_count = int(len(tdf))
            vol_sum = float(tdf["amount"].sum())
            vwap = float((tdf["cost"].sum() / max(vol_sum, 1e-12))) if vol_sum > 0 else None
        else:
            trades_count, vol_sum, vwap = 0, 0.0, None

        trade_size_buy = float(db5 * (1.0 + (imb if imb is not None else 0.0))) \
            if (db5 is not None and imb is not None) else None

        has_ob = all(v is not None for v in [mid, tb, ta, db5, da5, imb])
        ok_flag = bool(has_ob)
        data_quality = "ok" if (ok_flag and trades_count > 0) else ("partial" if ok_flag else "stale")

        bar = {
            "ok": ok_flag, "bar_ts": self.cur_window,
            "mid_price_5s": mid, "total_bid_volume_5s": tb, "total_ask_volume_5s": ta,
            "depth_bid_5_5s": db5, "depth_ask_5_5s": da5, "order_imbalance_5s": imb,
            "trades_count_5s": trades_count, "vol_sum_5s": vol_sum, "vwap_5s": vwap,
            "taker_buy_vol_5s": None, "taker_sell_vol_5s": None,
            "trade_size_buy_5s": trade_size_buy, "data_quality": data_quality,
        }

        self.ob_snaps.clear(); self.trades_buf.clear()
        self.cur_window += self.bar_ms
        return bar

# ===================== SIGNAL ENGINE =====================
class SignalEngine:
    def __init__(self, grid_levels: List[float], grid_tol: float = GRID_TOL,
                 confirm_needed: int = CONFIRM_BARS, cooldown_ms: int = COOLDOWN_MS,
                 window: int = WINDOW, th_cvd: float = CVD_Z_TH, th_ts: float = TS_Z_TH):
        self.grid_levels_open: List[float] = sorted(grid_levels)
        self.grid_tol = grid_tol
        self.confirm_needed = confirm_needed
        self.cooldown_ms = cooldown_ms
        self.window = window
        self.th_cvd = th_cvd
        self.th_ts  = th_ts

        self.cvd_series: deque = deque(maxlen=window)
        self.ts_buy_series: deque = deque(maxlen=window)
        self.last_cvd: float = 0.0
        self.confirm_count: int = 0
        self.cooldown_until: int = 0
        self.open_orders_count: int = 0
        self.active_levels: Set[float] = set()
        self.bars_total: int = 0

    @staticmethod
    def mad_z(x: List[float], x_now: float) -> float:
        if len(x) < 1: return np.nan
        arr = np.asarray(x, dtype=float)
        med = np.median(arr)
        mad = np.median(np.abs(arr - med))
        return 0.6745 * (x_now - med) / max(mad, 1e-9)

    def _is_locked(self, lv: float) -> bool:
        if lv in self.active_levels: return True
        for a in self.active_levels:
            if abs(lv - a) / max(lv, 1e-9) <= LOCK_TOL:
                return True
        return False

    def _pick_grid_candidate(self, mid: Optional[float]) -> Optional[float]:
        if mid is None or not self.grid_levels_open: return None
        near = [lv for lv in self.grid_levels_open if abs(lv - mid)/max(lv,1e-12) <= self.grid_tol]
        for lv in sorted(near, key=lambda x: abs(x-mid)):
            if not self._is_locked(lv): return lv
        if ALLOW_FALLBACK_EXEC:
            lowers = [lv for lv in self.grid_levels_open if lv < mid and not self._is_locked(lv)]
            if lowers: return max(lowers)
        return None

    def update(self, bar: Dict, now_ms_: int, max_open_orders: int) -> Dict:
        tb = bar.get("total_bid_volume_5s") or 0.0
        ta = bar.get("total_ask_volume_5s") or 0.0
        imb = bar.get("order_imbalance_5s") or 0.0
        total_vol = tb + ta

        self.last_cvd = float(self.last_cvd + (imb * total_vol))
        self.cvd_series.append(self.last_cvd)

        ts_buy = bar.get("taker_buy_vol_5s")
        if ts_buy is None:
            ts_buy = bar.get("trade_size_buy_5s") or 0.0
        self.ts_buy_series.append(float(ts_buy))

        cvd_z = self.mad_z(list(self.cvd_series), self.last_cvd) if len(self.cvd_series) >= self.window else np.nan
        ts_z  = self.mad_z(list(self.ts_buy_series), float(ts_buy)) if len(self.ts_buy_series) >= self.window else np.nan

        raw = int((not math.isnan(cvd_z)) and (not math.isnan(ts_z)) and (cvd_z >= self.th_cvd) and (ts_z >= self.th_ts))
        self.confirm_count = self.confirm_count + 1 if raw == 1 else 0
        confirmed = int(self.confirm_count >= self.confirm_needed)

        cooldown_active = now_ms_ < self.cooldown_until
        under_limit = self.open_orders_count < max_open_orders

        mid = bar.get("mid_price_5s")
        candidate = self._pick_grid_candidate(mid)

        within_tol = (
            candidate is not None and mid is not None and
            abs(candidate - mid) / max(candidate, 1e-9) <= self.grid_tol
        )
        level_free = (candidate is not None) and (not self._is_locked(candidate))

        if confirmed and (not cooldown_active) and under_limit and within_tol and level_free:
            action, reason = "PLACE_BUY", "signal_confirmed && near_grid && under_limits && level_free"
        elif candidate is not None and not within_tol:
            action, reason = "HOLD", "candidate_far_from_grid_tol"
        elif candidate is not None and not level_free:
            action, reason = "HOLD", "level_already_locked"
        else:
            action, reason = "HOLD", "waiting_conditions"

        self.bars_total += 1

        return {
            "bar_ts": bar.get("bar_ts"), "mid_price_5s": mid,
            "cvd": self.last_cvd, "cvd_z": cvd_z,
            "trade_size_buy_5s": float(ts_buy), "trade_size_z": ts_z,
            "buy_signal_raw": raw, "confirm_count": self.confirm_count,
            "buy_signal_confirmed": confirmed, "cooldown_active": cooldown_active,
            "open_orders_count": self.open_orders_count, "grid_candidate": candidate,
            "within_0p5pct": within_tol, "action": action, "reason": reason,
        }

    def on_order_placed(self, ts_ms: int, level: float) -> None:
        self.cooldown_until = ts_ms + self.cooldown_ms
        self.active_levels.add(level)

    def on_order_filled(self, ts_ms: int, level: float) -> None:
        self.open_orders_count += 1

    def on_tp_filled(self, ts_ms: int, level: float) -> None:
        self.open_orders_count = max(0, self.open_orders_count - 1)
        self.active_levels.discard(level)

# ===================== EXECUTION LAYER =====================
class ExecutionLayer:
    def __init__(self, fetcher: DataFetcher, symbol: str, dry_run: bool = True):
        self.ex = fetcher.exchange
        self.symbol = symbol
        self.dry = dry_run
        self.market = self.ex.market(symbol)
        self.tick_size, self.step_size, self.min_qty, self.min_notional = self._extract_filters(self.market)
        if MIN_NOTIONAL_OVERRIDE is not None:
            self.min_notional = max(float(self.min_notional or 0.0), float(MIN_NOTIONAL_OVERRIDE))
        self.buy_ids: Dict[float, str] = {}
        self.tp_ids: Dict[float, str] = {}
        self._last_poll_ts: float = 0.0

    # ---------- filters ----------
    def _extract_filters(self, market: Dict) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
        tick = step = min_qty = min_notional = None
        for f in market.get("info", {}).get("filters", []):
            t = f.get("filterType")
            if t == "PRICE_FILTER":
                tick = float(f.get("tickSize", "0.0001"))
            elif t == "LOT_SIZE":
                step = float(f.get("stepSize", "0.0001"))
                min_qty = float(f.get("minQty", "0.0"))
            elif t in ("MIN_NOTIONAL", "NOTIONAL"):
                mn = f.get("minNotional") or f.get("notional")
                if mn is not None:
                    min_notional = float(mn)
        if tick is None:
            tick = 10 ** -(market.get("precision", {}).get("price", 4))
        if step is None:
            step = 10 ** -(market.get("precision", {}).get("amount", 4))
        return tick, step, min_qty, (min_notional or 0.0)

    def _price_tol(self, ref_px: float) -> float:
        tick = self.tick_size or 0.0
        return max(LOCK_TOL * max(ref_px, 1e-9), 5.0 * tick)

    # ---------- assets & balances ----------
    def _quote_asset(self) -> str:
        try:
            return self.market.get("quote") or self.symbol.split("/")[1]
        except Exception:
            return self.symbol.split("/")[1]

    def _base_asset(self) -> str:
        try:
            return self.market.get("base") or self.symbol.split("/")[0]
        except Exception:
            return self.symbol.split("/")[0]

    def _get_free_quote(self) -> float:
        try:
            bal = self.ex.fetch_balance()
            q = self._quote_asset()
            if "free" in bal and q in bal["free"]:
                return float(bal["free"][q])
            if q in bal and isinstance(bal[q], dict):
                return float(bal[q].get("free", 0.0))
        except Exception:
            pass
        return 0.0

    def _get_free_base(self) -> float:
        try:
            bal = self.ex.fetch_balance()
            base = self._base_asset()
            if "free" in bal and base in bal["free"]:
                return float(bal["free"][base])
            if base in bal and isinstance(bal[base], dict):
                return float(bal[base].get("free", 0.0))
        except Exception:
            pass
        return 0.0

    # ---------- dust ledger ----------
    def _append_dust_ledger(self,
                            remainder_qty: float,
                            unit_cost_usdt: float,
                            reason: str,
                            ctx: Optional[dict] = None) -> None:
        """บันทึกเศษลง CSV: ts,symbol,remainder_qty,unit_cost_usdt,est_cost_total_usdt,reason,order_id,planned_tp,stepSize,minQty,minNotional"""
        if remainder_qty is None or remainder_qty <= 0:
            return
        row = {
            "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": getattr(self, "symbol", ""),
            "remainder_qty": f"{float(remainder_qty):.10f}",
            "unit_cost_usdt": f"{float(unit_cost_usdt):.8f}",
            "est_cost_total_usdt": f"{float(remainder_qty) * float(unit_cost_usdt):.8f}",
            "reason": reason,
            "order_id": (ctx or {}).get("order_id", ""),
            "planned_tp": (ctx or {}).get("planned_tp", ""),
            "stepSize": (ctx or {}).get("step", self.step_size),
            "minQty": (ctx or {}).get("min_qty", self.min_qty),
            "minNotional": (ctx or {}).get("min_notional", self.min_notional),
        }
        need_header = not os.path.exists(DUST_LEDGER_FILE)
        try:
            with open(DUST_LEDGER_FILE, "a", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=list(row.keys()))
                if need_header:
                    w.writeheader()
                w.writerow(row)
        except Exception as e:
            print(f"[WARN] cannot write {DUST_LEDGER_FILE}: {e}")

    # ---------- helpers ----------
    def round_price(self, p: float, side: Optional[str] = None) -> float:
        if (self.tick_size or 0) <= 0:
            return p
        q = _q(p) / _q(self.tick_size)
        q = q.to_integral_value(rounding=(ROUND_CEILING if side == "sell" else ROUND_FLOOR))
        return float(q * _q(self.tick_size))

    def round_amount_up(self, a: float) -> float:
        step = self.step_size or 1e-12
        q = (_q(a) / _q(step)).to_integral_value(rounding=ROUND_CEILING)
        return float(q * _q(step))

    def round_amount_down(self, a: float) -> float:
        step = self.step_size or 1e-12
        q = (_q(a) / _q(step)).to_integral_value(rounding=ROUND_FLOOR)
        return float(q * _q(step))

    def ensure_min_notional(self, price: float, amount: float) -> float:
        if (self.min_notional or 0.0) <= 0.0:
            need = amount
        else:
            need_by_notional = (self.min_notional / max(price, 1e-12))
            need = max(amount, need_by_notional)
        if self.min_qty:
            need = max(need, self.min_qty)
        return self.round_amount_up(need)

    def _best_prices(self) -> tuple:
        try:
            ob = self.ex.fetch_order_book(self.symbol, limit=5)
            best_bid = float(ob["bids"][0][0]); best_ask = float(ob["asks"][0][0])
            return best_bid, best_ask
        except Exception:
            return (0.0, 0.0)

    def _avg_fill_price_from_order(self, order: dict, fallback_price: float) -> float:
        try:
            filled = float(order.get("filled", 0.0) or 0.0)
            cost   = float(order.get("cost",   0.0) or 0.0)
            if filled > 0 and cost > 0:
                return cost / filled
        except Exception:
            pass
        return float(fallback_price or 0.0)

    def _cid(self, prefix: str, price: float) -> str:
        sym = self.symbol.replace("/", "")
        tick = self.tick_size or 1e-8
        p_ticks = int(round(price / tick))
        t = int(time.time())
        return f"{prefix}-{sym}-{p_ticks}-{t}"[:36]

    # ---------- MARKET BUY via quoteOrderQty ----------
    def place_market_buy(self, level: float, desired_amount: float) -> dict:
        _bid, ask = self._best_prices()
        px_ref = ask * (1.0 + max(0.0, SLIP_PCT)) if ask > 0 else level

        # เงินว่าง USDT
        quote_free = float(self._get_free_quote() or 0.0)

        # งบขั้นต่ำ = max(coin_size*px_ref, minNotional) + safety
        base_cost = max(desired_amount * px_ref, float(self.min_notional or 5.0))
        pad_abs = 0.05
        pad_pct = 0.002
        safety = max(pad_abs, base_cost * pad_pct)
        quote_need = base_cost + safety

        # อย่าเกินเงินว่าง และปัดลงเป็นหน่วยเซ็นต์
        q_send = min(quote_need, quote_free)
        q_send = math.floor(max(0.0, q_send) * 100.0) / 100.0

        if q_send < (self.min_notional or 5.0):
            print(f"[skip] quoteOrderQty<{self.min_notional} USDT (want≈{quote_need:.2f}, free≈{quote_free:.2f}, send={q_send:.2f})")
            return {"id": None, "filled": 0.0, "avg": None, "amt_sent": 0.0, "px_ref": px_ref}

        if self.dry:
            print(f"[DRY] MARKET BUY by quoteOrderQty={q_send:.2f} {self.symbol} (px_ref≈{px_ref:.8f})")
            return {"id": f"dry-mkt-quote-{level}", "filled": q_send/px_ref, "avg": px_ref, "amt_sent": q_send/px_ref, "px_ref": px_ref}

        try:
            o = self.ex.create_order(
                self.symbol, "market", "buy", None, None,
                {"quoteOrderQty": q_send, "newClientOrderId": self._cid("gbQ", px_ref), "newOrderRespType": "FULL"}
            )
            filled = float(o.get("filled") or 0.0)
            avg = self._avg_fill_price_from_order(o, px_ref)
            print(f"[buy-debug] level={level:.2f} px_ref={px_ref:.8f} coin_size_req={desired_amount} "
                  f"quote_need≈{quote_need:.2f} quote_sent={q_send:.2f} filled={filled:.8f} avg={avg:.8f} "
                  f"filters(minNotional={self.min_notional}, minQty={self.min_qty}, step={self.step_size})")
            return {"id": o.get("id"), "filled": filled, "avg": avg, "amt_sent": filled, "px_ref": px_ref}
        except Exception as e:
            print(f"[ERR] market buy (quoteOrderQty) failed: {e}")
            return {"id": None, "filled": 0.0, "avg": None, "amt_sent": 0.0, "px_ref": px_ref}

    # ---------- LIMIT SELL TP ----------
    def place_limit_sell_tp(self, level: float, amount: float, tp_price: float) -> Optional[str]:
        px = self.round_price(tp_price, side="sell")
        amt = self.round_amount_down(amount)
        if amt <= 0:
            return None
        if self.dry:
            print(f"[DRY] place TP SELL {self.symbol} px={px} amt={amt}")
            return f"dry-tp-{level}"
        try:
            params = {"timeInForce": "GTC", "newClientOrderId": self._cid("gtp", px)}
            o = self.ex.create_order(self.symbol, "limit", "sell", amt, px, params)
            return o.get("id")
        except Exception as e:
            print(f"[ERR] place TP failed @ {px}: {e}")
            return None

    def _wait_filled(self, order_id: str, timeout: float = 2.0) -> float:
        if not order_id:
            return 0.0
        end_ts = time.time() + max(0.0, timeout)
        last_filled = 0.0
        while time.time() < end_ts:
            try:
                o = self.ex.fetch_order(order_id, self.symbol)
                last_filled = float(o.get("filled") or 0.0)
                status = (o.get("status") or "").lower()
                if last_filled > 0.0 or status in {"closed", "canceled"}:
                    break
            except Exception:
                pass
            time.sleep(0.1)
        return last_filled

    def _safe_tp_amount(self, desired_amt: float, tp_price: float) -> float:
        amt = max(0.0, float(desired_amt))
        if amt <= 0:
            return 0.0
        amt = self.round_amount_down(amt)
        if self.min_qty and amt < self.min_qty:
            return 0.0
        if self.min_notional and (amt * tp_price < self.min_notional):
            return 0.0
        return amt

    def place_tp_after_market(self, level: float, market_order_id: Optional[str],
                              desired_amt: float, tp_price: float) -> Optional[str]:
        """ตั้ง TP แบบปลอดภัย + จัดการบันทึก dust"""
        if self.dry:
            print(f"[DRY] place TP after market: tp_price={tp_price}")
            return f"dry-tp-{level}"

        # ดึงคำสั่งซื้อเพื่ออ่าน filled/cost (สำหรับ avg)
        order_obj = None
        filled_from_order = 0.0
        try:
            if market_order_id:
                order_obj = self.ex.fetch_order(market_order_id, self.symbol)
                filled_from_order = float(order_obj.get("filled") or 0.0)
        except Exception:
            filled_from_order = self._wait_filled(market_order_id, 2.0) if market_order_id else 0.0

        _best_bid, best_ask = self._best_prices()
        avg_cost = self._avg_fill_price_from_order(order_obj or {}, fallback_price=best_ask or tp_price)

        time.sleep(0.25)  # ให้ balance sync
        free_base = self._get_free_base()

        cap = min(max(desired_amt, 0.0),
                  filled_from_order if filled_from_order > 0 else free_base,
                  free_base)
        amt_floor = self.round_amount_down(cap)

        # log เศษจาก LOT_SIZE
        remainder = max(0.0, cap - amt_floor)
        if remainder > 0:
            self._append_dust_ledger(
                remainder_qty=remainder, unit_cost_usdt=avg_cost, reason="lot_rounding",
                ctx={"order_id": market_order_id, "planned_tp": tp_price,
                     "step": self.step_size, "min_qty": self.min_qty, "min_notional": self.min_notional}
            )

        if self.min_qty and amt_floor < self.min_qty:
            self._append_dust_ledger(
                remainder_qty=amt_floor, unit_cost_usdt=avg_cost, reason="minQty_gate",
                ctx={"order_id": market_order_id, "planned_tp": tp_price,
                     "step": self.step_size, "min_qty": self.min_qty, "min_notional": self.min_notional}
            )
            print(f"[skip] TP not placed (amt<{self.min_qty} minQty | free={free_base} | filled={filled_from_order})")
            return None

        if (self.min_notional or 0.0) > 0 and amt_floor > 0:
            tp_need = (self.min_notional + 1e-12) / amt_floor
            tp_price = max(tp_price, tp_need)

        best_bid, _ = self._best_prices()
        if best_bid > 0:
            tp_price = max(tp_price, best_bid * (1.0 + TP_BID_SAFETY_PCT))

        sell_amt = self._safe_tp_amount(amt_floor, tp_price)
        if sell_amt <= 0:
            self._append_dust_ledger(
                remainder_qty=amt_floor, unit_cost_usdt=avg_cost,
                reason="minNotional_gate" if (self.min_notional and amt_floor * tp_price < self.min_notional) else "tp_not_placed",
                ctx={"order_id": market_order_id, "planned_tp": tp_price,
                     "step": self.step_size, "min_qty": self.min_qty, "min_notional": self.min_notional}
            )
            print(f"[skip] TP not placed (sell_amt={sell_amt} | free={free_base} | filled={filled_from_order} "
                  f"| minQty={self.min_qty} | minNotional={self.min_notional} | step={self.step_size})")
            return None

        try:
            print(f"[tp-debug] avg_cost={avg_cost:.6f} tp_px_in={tp_price:.6f} sell_amt≈{sell_amt}")
        except Exception:
            pass

        oid = self.place_limit_sell_tp(level, sell_amt, tp_price)
        if not oid:
            self._append_dust_ledger(
                remainder_qty=sell_amt, unit_cost_usdt=avg_cost, reason="tp_place_failed",
                ctx={"order_id": market_order_id, "planned_tp": tp_price,
                     "step": self.step_size, "min_qty": self.min_qty, "min_notional": self.min_notional}
            )
        return oid

    # ---------- open orders sync ----------
    def poll(self, engine: "SignalEngine", grid_rows: pd.DataFrame) -> None:
        if self.dry or not self.tp_ids:
            return
        now = time.time()
        if now - self._last_poll_ts < POLL_OPEN_ORDERS_SEC:
            return
        self._last_poll_ts = now
        for level, oid in list(self.tp_ids.items()):
            try:
                o = self.ex.fetch_order(oid, self.symbol)
                status = (o.get("status") or "").lower()
                if status == "closed":
                    engine.on_tp_filled(now_ms(), level)
                    self.tp_ids.pop(level, None); self.buy_ids.pop(level, None)
                elif status == "canceled":
                    engine.active_levels.discard(level)
                    self.tp_ids.pop(level, None); self.buy_ids.pop(level, None)
            except Exception:
                pass

    def resync_open_orders(self, engine: "SignalEngine", grid_df: pd.DataFrame) -> None:
        try:
            oo = self.ex.fetch_open_orders(self.symbol)
        except Exception:
            return
        new_active = set(); new_tp_ids, new_buy_ids = {}, {}
        for o in oo:
            px = float(o.get("price") or 0.0)
            side = (o.get("side") or "").lower()
            lv = self.level_key_from_order(grid_df, px, side)
            if lv is None: continue
            new_active.add(lv)
            if side == "sell": new_tp_ids[lv] = o.get("id")
            elif side == "buy": new_buy_ids[lv] = o.get("id")
        self.tp_ids = new_tp_ids; self.buy_ids = new_buy_ids
        engine.active_levels = new_active
        engine.open_orders_count = sum(1 for o in oo if (o.get("side") or "").lower() == "sell")

    def level_key_from_order(self, grid_df: pd.DataFrame, px: float, side: str) -> Optional[float]:
        side = (side or "").lower()
        if px <= 0: return None
        if side == "sell" and "tp_price" in grid_df.columns:
            diffs = (grid_df["tp_price"] - px).abs(); i = int(diffs.idxmin())
            ref = float(grid_df.loc[i, "tp_price"]); tol = self._price_tol(ref)
            return float(grid_df.loc[i, "buy_price"]) if abs(ref - px) <= tol else None
        else:
            diffs = (grid_df["buy_price"] - px).abs(); i = int(diffs.idxmin())
            ref = float(grid_df.loc[i, "buy_price"]); tol = self._price_tol(ref)
            return float(ref) if abs(ref - px) <= tol else None

    # ---------- pre-lock existing open orders ----------
    def prelock_existing(self, engine: "SignalEngine", grid_df: pd.DataFrame) -> None:
        """
        อ่าน open orders จากเอ็กซ์เชนจ์ แล้ว map → level เพื่อ:
          - set engine.open_orders_count = จำนวน SELL (TP) ที่ค้างจริง
          - เติม self.buy_ids / self.tp_ids
          - ใส่ engine.active_levels ให้ตรงกับของจริง
        """
        try:
            oo = self.ex.fetch_open_orders(self.symbol)
        except Exception:
            oo = []

        buys = sells = locked = 0

        # จำนวนดีลเปิดจริง = นับ SELL ที่ค้าง (TP)
        exchange_sells = sum(1 for o in oo if (o.get("side") or "").lower() == "sell")
        engine.open_orders_count = exchange_sells

        for o in oo:
            px = float(o.get("price") or 0.0)
            side = (o.get("side") or "").lower()

            lv = self.level_key_from_order(grid_df, px, side)

            # debug: SELL ที่หา level ไม่เจอ
            if lv is None:
                if side == "sell" and SHOW_UNMAPPED_SELL_DEBUG:
                    try:
                        if ("tp_price" in grid_df.columns) and (len(grid_df) > 0):
                            nearest_idx = (grid_df["tp_price"] - px).abs().idxmin()
                            nearest_tp = float(grid_df.loc[nearest_idx, "tp_price"])
                            tick = self.tick_size or 1e-8
                            diff_pct = abs(px - nearest_tp) / max(nearest_tp, 1e-9) * 100.0
                            ticks = abs(px - nearest_tp) / max(tick, 1e-12)
                            tol_abs = (LOCK_TOL * nearest_tp)
                            print(f"[d] unmapped SELL px={px} vs nearest tp={nearest_tp}  Δ={diff_pct:.3f}% (~{ticks:.1f} ticks)  tol≈{tol_abs}")
                    except Exception:
                        pass
                continue

            engine.active_levels.add(lv); locked += 1
            if side == "buy":
                self.buy_ids[lv] = o.get("id"); buys += 1
            elif side == "sell":
                self.tp_ids[lv]  = o.get("id"); sells += 1

        if SHOW_PRELOCK_SUMMARY and (buys or sells or locked):
            print(
                f"[i] pre-locked {locked} level(s) from open orders (buy={buys}, sell={sells}). "
                f"open_deals={engine.open_orders_count}"
            )


# ===================== MAIN =====================
def main() -> None:
    print("[i] เริ่มดึงข้อมูลสดจาก Binance …")

    grid_levels = load_grid_levels_from_csv(GRID_CSV)
    if not grid_levels:
        print(f"[!] ไม่พบไฟล์กริด {GRID_CSV} หรืออ่านไม่ได้ — จะทำงานเฉพาะสัญญาณ (ไม่เช็คใกล้กริด)")
    try:
        grid_df = load_grid_df(GRID_CSV)
    except Exception:
        grid_df = pd.DataFrame(columns=["buy_price","coin_size","tp_price"])

    symbol = detect_symbol_from_macro("macro_montecarlo.csv")
    symbol_safe = symbol.replace("/", "").lower()
    log_csv_path = os.path.join("logs", f"{symbol_safe}_5s_decisions.csv")
    print(f"[i] Trading symbol = {symbol} (from macro_montecarlo.csv)")

    engine = SignalEngine(
        grid_levels=grid_levels,
        confirm_needed=CONFIRM_BARS,
        cooldown_ms=COOLDOWN_MS,
        window=WINDOW,
        th_cvd=CVD_Z_TH,
        th_ts=TS_Z_TH,
    )
    fetcher = DataFetcher(symbol, EXCHANGE_ID, with_auth=(not DRY_RUN))
    execu = ExecutionLayer(fetcher, symbol, dry_run=DRY_RUN)
    aggr = Aggregator5s(BAR_MS)

    csv_fh, csv_wr = init_csv_logger(log_csv_path)

    execu.prelock_existing(engine, grid_df)

    last_grid_reload = time.time()
    last_resync = time.time()

    try:
        while True:
            t0 = now_ms()

            # optional reload grid
            if GRID_RELOAD_SEC > 0 and (time.time() - last_grid_reload) >= GRID_RELOAD_SEC:
                try:
                    grid_levels = load_grid_levels_from_csv(GRID_CSV)
                    if grid_levels:
                        engine.grid_levels_open = sorted(grid_levels)
                        grid_df = load_grid_df(GRID_CSV)
                        print("[i] reloaded grid_plan.csv")
                except Exception as e:
                    print(f"[!] reload grid failed: {e}")
                last_grid_reload = time.time()

            # periodic resync open orders (ground truth from exchange)
            if RESYNC_OPEN_ORDERS_SEC > 0 and (time.time() - last_resync) >= RESYNC_OPEN_ORDERS_SEC:
                execu.resync_open_orders(engine, grid_df)
                last_resync = time.time()

            ob = fetcher.fetch_orderbook()
            tr = fetcher.fetch_recent_trades(limit=100)
            aggr.add_orderbook_snapshot(ob)
            aggr.add_trades(tr)

            bar = aggr.roll_bar(now_ms())
            if not bar:
                time.sleep(0.01)
                continue

            ts_str = datetime.fromtimestamp(bar["bar_ts"]/1000, tz=timezone.utc).strftime("%H:%M:%S")
            dq = bar.get("data_quality", "n/a")

            if not bar.get("ok", False):
                print(f"[{ts_str}] dq={dq} (skip partial/stale bar)")
                if csv_wr:
                    csv_wr.writerow([
                        int(bar["bar_ts"]),
                        datetime.fromtimestamp(bar["bar_ts"]/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                        dq, None, None, None, None, None, None, None, None, None, None, "SKIP_PARTIAL", "",
                        engine.bars_total, len(engine.active_levels)
                    ])
                    csv_fh.flush()
                continue

            # ---- คำนวณสัญญาณเฉพาะบาร์ที่ ok ----
            decision = engine.update(bar, now_ms(), MAX_OPEN_ORDERS)

            print(
                f"[{ts_str}] mid={sf(decision['mid_price_5s'])}  "
                f"cvd_z={sf(decision['cvd_z'],2)}  ts_z={sf(decision['trade_size_z'],2)}  "
                f"conf={decision['confirm_count']}  grid={sf(decision['grid_candidate'])}  "
                f"act={decision['action']}  dq={dq}  bars={engine.bars_total}  "
                f"active_lv={len(engine.active_levels)}  open={engine.open_orders_count}"
            )

            # --- เขียน CSV ---
            if csv_wr:
                bar_dt = datetime.fromtimestamp(bar["bar_ts"]/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                csv_wr.writerow([
                    int(bar["bar_ts"]), bar_dt, dq,
                    decision.get("mid_price_5s"),
                    decision.get("cvd"),
                    decision.get("cvd_z"),
                    decision.get("trade_size_buy_5s"),
                    decision.get("trade_size_z"),
                    decision.get("confirm_count"),
                    decision.get("buy_signal_raw"),
                    decision.get("buy_signal_confirmed"),
                    decision.get("grid_candidate"),
                    decision.get("action"),
                    decision.get("reason"),
                    engine.bars_total,
                    len(engine.active_levels),
                ])
                csv_fh.flush()

            # --- Execution ---
            if decision["action"] == "PLACE_BUY" and decision["grid_candidate"] is not None:
                level = float(decision["grid_candidate"])

                # FIX: double-check ใกล้กริด ณ เวลาจะยิง (กัน drift)
                mid_now = float(decision.get("mid_price_5s") or 0.0)
                if (mid_now <= 0.0) or (abs(level - mid_now) / max(level, 1e-9) > GRID_TOL):
                    print(f"[skip] candidate {level} drifted away from GRID_TOL at exec (mid={mid_now}).")
                    engine.active_levels.discard(level)
                    continue

                if level in engine.active_levels:
                    print(f"[skip] level {level} already locked.")
                    continue

                # ล็อกเลเวลก่อน (กันสัญญาณรัว)
                engine.on_order_placed(now_ms(), level)

                # เลือกแถวจากกริด (ต้องแมตช์และอยู่ใน GRID_TOL จริง)
                row = match_grid_row(grid_df, level, tol=GRID_TOL)
                coin_size = float(row["coin_size"])
                if (coin_size <= 0.0) or (abs(float(row["buy_price"]) - level) / max(level, 1e-9) > GRID_TOL):
                    print(f"[skip] candidate {level} not within GRID_TOL or not in CSV rows.")
                    engine.active_levels.discard(level)
                    continue

                # ยิง MARKET BUY (quoteOrderQty ภายในฟังก์ชัน)
                res = execu.place_market_buy(level, coin_size)
                market_id = res.get("id")
                filled_amt = res["filled"] if res["filled"] > 0 else coin_size

                if res["id"] or DRY_RUN:
                    # mark ว่ามีดีลเปิดแล้ว
                    engine.on_order_filled(now_ms(), level)

                    # ==== ความปลอดภัย TP ====
                    row_tp   = float(row["tp_price"])
                    fill_avg = res.get("avg") or res.get("px_ref") or level
                    safe_tp_min = float(fill_avg) * (1.0 + (2.0 * EXEC_FEE_RATE) + TP_EXTRA_MARGIN)
                    tp_px = max(row_tp, safe_tp_min)

                    try:
                        print(f"[tp-debug] fill_avg={fill_avg:.6f} row_tp={row_tp:.6f} "
                              f"safe_tp_min={safe_tp_min:.6f} tp_px_before_round={tp_px:.6f}")
                    except Exception:
                        pass

                    # วาง TP แบบปลอดภัย (รอฟิลล์ + เช็ค balance + notional + dust tracker ในตัว)
                    tp_id = execu.place_tp_after_market(level, market_id, filled_amt, tp_px)
                    if tp_id:
                        execu.tp_ids[level] = tp_id
                else:
                    # ซื้อไม่สำเร็จ → ปลดล็อกเลเวล
                    engine.active_levels.discard(level)

            # โพลล์สถานะ TP รายออเดอร์แบบเว้นช่วง
            execu.poll(engine, grid_df)

            elapsed = now_ms() - t0
            time.sleep(max(0.0, 1.0 - elapsed/1000.0))

    except KeyboardInterrupt:
        print("\n[!] KeyboardInterrupt — shutting down …")
    finally:
        try:
            csv_fh.close()
        except Exception:
            pass

# --- entrypoint ---
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        print(f"[FATAL] {type(e).__name__}: {e}")
        traceback.print_exc()

