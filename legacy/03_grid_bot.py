#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Live 5s bars + Signal + Grid Execution (Spot Binance, ccxt)
- สัญญาณ: MAD-zscore (window=50), ยืนยัน 1 แท่ง
- ซื้อเฉพาะเมื่อใกล้กริด ≤ GRID_TOL
- กันเปิดซ้ำระดับ: pre-lock จาก open orders, ตรวจซ้ำก่อนยิง, map order→level
- โหมดนี้: BUY = MARKET 100% แล้ววาง TP เป็น LIMIT SELL ทันที (เวอร์ชันปลอดภัย: รอฟิลล์ + เช็ค balance ก่อน)
"""
import csv
import os
import time
import math
import ccxt
import numpy as np
import pandas as pd
from collections import deque
from typing import Optional, List, Dict, Set
from datetime import datetime, timezone

# ===================== CONFIG =====================

# SYMBOL = "XRP/USDT"         # (Base/Quote)
SYMBOL = "JTO/USDT"         # (Base/Quote)
EXCHANGE_ID = "binance"     # ชื่อเอ็กซ์เชนจ์ใน ccxt

BOOK_LIMIT = 1000           # จำนวนระดับความลึกของ OrderBook ที่ดึงมาใช้ต่อรอบ (ยิ่งมากยิ่งหนัก)
BAR_MS = 5_000              # ความยาวหนึ่งแท่ง (มิลลิวินาที) — 5,000 ms = 5 วินาที/บาร์
EMA_SPAN = 10               # ช่วงของ EMA ที่ใช้ทำให้ order_imbalance เนียนขึ้น (ยิ่งมากยิ่งหน่วง)

GRID_TOL = 0.005            # เกณฑ์ “ใกล้กริด”: abs(mid - level)/level ≤ GRID_TOL  (0.005 = 0.5%)
LOCK_TOL = 0.0015           # เกณฑ์จับคู่ราคาออเดอร์เปิดกับระดับกริดเพื่อ “ล็อก” เลเวลไม่ให้ยิงซ้ำ
FALLBACK_TO_NEXT_LOWER = True  # ถ้าเลเวลที่ใกล้ถูกล็อกแล้ว ให้เลื่อนไปหาเลเวลล่างที่ยังว่าง

CONFIRM_BARS = 1            # จำนวนแท่งที่ต้อง “ติด” raw-signal ต่อเนื่องเพื่อยืนยัน
COOLDOWN_MS = 60_000        # คูลดาวน์หลังยิงคำสั่งซื้อสำเร็จ (มิลลิวินาที)

# Threshold z-score ของสัญญาณทั้งสองตัว (ตั้งอิสระ)
WINDOW = 50                 # ขนาดหน้าต่างข้อมูลย้อนหลังสำหรับคำนวณ MAD-zscore (หน่วย = จำนวนบาร์)
CVD_Z_TH = 2                # เกณฑ์ z-score ของ CVD
TS_Z_TH  = 1.5              # เกณฑ์ z-score ของ Trade Size

MAX_OPEN_ORDERS = 10        # จำกัดจำนวนดีลที่เปิดพร้อมกันสูงสุด (สำหรับการคุมความเสี่ยง)

GRID_CSV = "grid_plan.csv"  # ไฟล์กริด: ต้องมี buy_price, coin_size, tp_price (หรือ tp_pct)
GRID_RELOAD_SEC = 0         # รีโหลดไฟล์กริดอัตโนมัติทุก N วินาที (0 = ปิด)

DRY_RUN = True             # โหมด paper (ไม่ส่งออเดอร์จริง)

MIN_NOTIONAL_OVERRIDE = 5   # บังคับ notional ขั้นต่ำต่อคำสั่ง (USDT); ตั้ง None เพื่อใช้ค่าจากตลาด
SLIP_PCT = 0.0007           # buffer สำหรับคำนวณจำนวนเหรียญขั้นต่ำให้ผ่าน notional จาก best_ask*(1+SLIP_PCT)

# >>> เพิ่มค่าตั้งเพื่อความปลอดภัยในการวาง TP หลัง MARKET BUY <<<
FILL_WAIT_SEC = 2.0         # เวลารอเช็คฟิลล์สูงสุด (วินาที)
TP_DELAY_SEC  = 0.25        # หน่วงสั้น ๆ ก่อนส่ง TP (วินาที)

# ชื่อไฟล์ log จะอิงจาก SYMBOL โดยอัตโนมัติ
SYMBOL_SAFE = SYMBOL.replace("/", "").lower()
LOG_CSV_PATH = os.path.join("logs", f"{SYMBOL_SAFE}_5s_decisions.csv")

# ===================== UTILS =====================

def now_ms() -> int:
    return int(time.time() * 1000)

def ema_update(prev: Optional[float], x: float, span: int) -> float:
    if prev is None:
        return x
    alpha = 2.0 / (span + 1.0)
    return (1 - alpha) * prev + alpha * x

def sf(x, nd=5) -> str:
    """safe format: คืน 'nan' ถ้า x ไม่ใช่ตัวเลขปกติ"""
    try:
        v = float(x)
        if not np.isfinite(v):
            return "nan"
        return f"{v:.{nd}f}"
    except Exception:
        return "nan"

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
    """เตรียมไฟล์ CSV สำหรับบันทึกการตัดสินใจรายบาร์"""
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
    
class DataFetcher:
    def __init__(self, symbol: str, exchange_id: str = "binance", with_auth: bool = False):
        self.symbol = symbol
        api_key, api_secret = _load_api_credentials()
        kwargs = {"enableRateLimit": True}
        if with_auth:
            kwargs["apiKey"] = api_key
            kwargs["secret"] = api_secret
        self.exchange = getattr(ccxt, exchange_id)(kwargs)
        self.exchange.load_markets()
        self._imbalance_ema: Optional[float] = None
        self._last_trade_id: Optional[str] = None

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

            total_bid_vol = float(np.sum([lvl[1] for lvl in bids]))
            total_ask_vol = float(np.sum([lvl[1] for lvl in asks]))
            depth_bid_5 = float(np.sum([lvl[1] for lvl in bids[:5]]))
            depth_ask_5 = float(np.sum([lvl[1] for lvl in asks[:5]]))

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
        try:
            trades = self.exchange.fetch_trades(self.symbol, limit=limit)
            out = []
            for t in trades:
                tid = str(t.get("id", ""))
                if self._last_trade_id and tid <= self._last_trade_id:
                    continue
                out.append({
                    "id": tid,
                    "ts": int(t.get("timestamp") or now_ms()),
                    "price": float(t["price"]), "amount": float(t["amount"]),
                    "cost": float(t["price"]) * float(t["amount"]),
                    "side": t.get("side"), "taker": t.get("takerOrMaker"),
                })
            if trades:
                self._last_trade_id = str(trades[-1].get("id", self._last_trade_id or ""))

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
        if not snap.get("ok"):
            return
        ts = int(snap["ts"])
        ws = self._window_start(ts)
        if self.cur_window is None:
            self.cur_window = ws
        self.ob_snaps.append(snap)

    def add_trades(self, payload: Dict) -> None:
        if not payload.get("ok"):
            return
        self.trades_buf.extend(payload["trades"])

    def roll_bar(self, now_ms_: int) -> Optional[Dict]:
        if self.cur_window is None:
            self.cur_window = self._window_start(now_ms_)
            return None
        if now_ms_ < self.cur_window + self.bar_ms:
            return None

        ob = pd.DataFrame(self.ob_snaps) if self.ob_snaps else pd.DataFrame()

        def mean_or_none(col: str) -> Optional[float]:
            return float(ob[col].mean()) if (not ob.empty and col in ob) else None

        mid = mean_or_none("mid_price")
        tb  = mean_or_none("total_bid_volume")
        ta  = mean_or_none("total_ask_volume")
        db5 = mean_or_none("depth_bid_5")
        da5 = mean_or_none("depth_ask_5")
        imb = mean_or_none("order_imbalance")

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

        self.ob_snaps.clear()
        self.trades_buf.clear()
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
        if len(x) < 1:
            return np.nan
        arr = np.asarray(x, dtype=float)
        med = np.median(arr)
        mad = np.median(np.abs(arr - med))
        return 0.6745 * (x_now - med) / max(mad, 1e-9)

    def _is_locked(self, lv: float) -> bool:
        if lv in self.active_levels:
            return True
        for a in self.active_levels:
            if abs(lv - a) / max(lv, 1e-9) <= LOCK_TOL:
                return True
        return False

    def _pick_grid_candidate(self, mid: Optional[float]) -> Optional[float]:
        if mid is None or not self.grid_levels_open:
            return None
        near = [lv for lv in self.grid_levels_open
                if abs(lv - mid) / max(lv, 1e-12) <= self.grid_tol]
        for lv in sorted(near, key=lambda x: abs(x - mid)):
            if not self._is_locked(lv):
                return lv
        if FALLBACK_TO_NEXT_LOWER:
            lowers = [lv for lv in self.grid_levels_open if lv < mid and not self._is_locked(lv)]
            if lowers:
                return max(lowers)
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
        within_tol = candidate is not None
        level_free = (candidate is not None) and (candidate not in self.active_levels)

        if confirmed and (not cooldown_active) and under_limit and within_tol and level_free:
            action, reason = "PLACE_BUY", "signal_confirmed && near_grid && under_limits && level_free"
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
            self.min_notional = float(MIN_NOTIONAL_OVERRIDE)
        self.buy_ids: Dict[float, str] = {}
        self.tp_ids: Dict[float, str] = {}

    def _extract_filters(self, market: Dict):
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

    def round_price(self, p: float) -> float:
        return math.floor(p / self.tick_size) * self.tick_size

    def round_amount_up(self, a: float) -> float:
        step = self.step_size or 1e-12
        return math.ceil((a - 1e-15) / step) * step

    def ensure_min_notional(self, price: float, amount: float) -> float:
        if (self.min_notional or 0.0) <= 0.0:
            need = amount
        else:
            need_by_notional = self.min_notional / max(price, 1e-12)
            need = max(amount, need_by_notional)
        if self.min_qty:
            need = max(need, self.min_qty)
        return self.round_amount_up(need)

    # ---------- best prices ----------
    def _best_prices(self) -> tuple:
        try:
            ob = self.ex.fetch_order_book(self.symbol, limit=5)
            best_bid = float(ob["bids"][0][0]); best_ask = float(ob["asks"][0][0])
            return best_bid, best_ask
        except Exception:
            return (0.0, 0.0)

    # ---------- สร้าง clientOrderId ที่ถูกกติกา ----------
    def _cid(self, prefix: str, price: float) -> str:
        sym = self.symbol.replace("/", "")
        tick = self.tick_size or 1e-8
        p_ticks = int(round(price / tick))
        t = int(time.time())
        cid = f"{prefix}-{sym}-{p_ticks}-{t}"
        return cid[:36]

    # ---------- MARKET BUY (taker 100%) ----------
    def place_market_buy(self, level: float, desired_amount: float) -> Dict:
        """
        ยิงคำสั่งตลาดโดยส่ง 'amount' เป็นจำนวนเหรียญ (base) ให้ผ่าน notional+lot-size
        ใช้ราคาอ้างอิง px_ref = best_ask*(1+SLIP_PCT) ในการคำนวณจำนวนขั้นต่ำ
        คืนค่า dict: {"id", "filled", "avg", "amt_sent", "px_ref"}
        """
        _bid, ask = self._best_prices()
        px_ref = ask * (1.0 + max(0.0, SLIP_PCT)) if ask > 0 else level
        amt = self.ensure_min_notional(px_ref, desired_amount)

        if amt <= 0:
            print(f"[!] market-buy amount=0 after min_notional/lot_size — skip level {level}")
            return {"id": None, "filled": 0.0, "avg": None, "amt_sent": 0.0, "px_ref": px_ref}

        if self.dry:
            print(f"[DRY] MARKET BUY {self.symbol} amt≈{amt} (px_ref≈{px_ref:.8f})")
            return {"id": f"dry-mkt-{level}", "filled": amt, "avg": px_ref, "amt_sent": amt, "px_ref": px_ref}

        try:
            o = self.ex.create_order(self.symbol, "market", "buy", amt, None,
                                     {"newClientOrderId": self._cid("gbM", px_ref)})
            filled = float(o.get("filled") or 0.0)
            avg    = float(o.get("average") or px_ref) if filled > 0 else None
            return {"id": o.get("id"), "filled": filled, "avg": avg, "amt_sent": amt, "px_ref": px_ref}
        except Exception as e:
            print(f"[ERR] market buy failed: {e}")
            return {"id": None, "filled": 0.0, "avg": None, "amt_sent": 0.0, "px_ref": px_ref}

    # ---------- LIMIT SELL TP (พื้นฐาน) ----------
    def place_limit_sell_tp(self, level: float, amount: float, tp_price: float) -> Optional[str]:
        px = self.round_price(tp_price)
        amt = self.round_amount_up(amount)
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

    # ---------- helper: หาตัวย่อเหรียญ base / balance ----------
    def _base_asset(self) -> str:
        try:
            return self.market.get("base") or self.symbol.split("/")[0]
        except Exception:
            return self.symbol.split("/")[0]

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

    def _wait_filled(self, order_id: str, timeout: float = FILL_WAIT_SEC) -> float:
        """รอจนคำสั่ง market ถูกฟิลล์และ ccxt คืน filled > 0 (หรือหมดเวลา)"""
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
        """ปัดจำนวนลงตาม LOT_SIZE และบังคับผ่าน minQty/minNotional"""
        amt = max(0.0, float(desired_amt))
        if amt <= 0:
            return 0.0
        step = self.step_size or 1e-12
        amt = math.floor(amt / step) * step  # ปัดลง
        if self.min_qty and amt < self.min_qty:
            return 0.0
        if self.min_notional and (amt * tp_price < self.min_notional):
            return 0.0
        return amt

    def place_tp_after_market(self, level: float, market_order_id: Optional[str],
                              desired_amt: float, tp_price: float) -> Optional[str]:
        """
        วาง TP แบบปลอดภัย: (1) รอผล filled ของคำสั่งตลาด, (2) หน่วงให้ balance sync,
        (3) อ่าน free base, (4) คำนวณจำนวนขายที่ปลอดภัยและปัดตาม LOT/MIN_NOTIONAL
        """
        if self.dry:
            print(f"[DRY] place TP after market: tp_price={tp_price}")
            return f"dry-tp-{level}"

        filled_from_order = self._wait_filled(market_order_id, FILL_WAIT_SEC) if market_order_id else 0.0
        time.sleep(max(0.0, TP_DELAY_SEC))
        free_base = self._get_free_base()

        sell_raw = min(max(desired_amt, 0.0),
                       filled_from_order if filled_from_order > 0 else free_base,
                       free_base)

        sell_amt = self._safe_tp_amount(sell_raw, tp_price)
        if sell_amt <= 0:
            print(f"[skip] TP not placed (sell_amt={sell_amt} | free={free_base} | filled={filled_from_order})")
            return None

        return self.place_limit_sell_tp(level, sell_amt, tp_price)

    # ---------- pre-lock existing open orders ----------
    def level_key_from_price(self, grid_df: pd.DataFrame, px: float) -> float:
        diffs = (grid_df["buy_price"] - px).abs()
        i = diffs.idxmin()
        lv = float(grid_df.loc[i, "buy_price"])
        if abs(lv - px) / max(px, 1e-9) <= LOCK_TOL:
            return lv
        return self.round_price(px)

    def prelock_existing(self, engine: SignalEngine, grid_df: pd.DataFrame) -> None:
        try:
            oo = self.ex.fetch_open_orders(self.symbol)
        except Exception:
            oo = []
        for o in oo:
            px = float(o.get("price") or 0.0)
            side = o.get("side", "").lower()
            level_key = self.level_key_from_price(grid_df, px)
            if side == "buy":
                self.buy_ids[level_key] = o.get("id")
                engine.active_levels.add(level_key)
            elif side == "sell":
                self.tp_ids[level_key] = o.get("id")
                engine.active_levels.add(level_key)
        if oo:
            print(f"[i] pre-locked {len(engine.active_levels)} level(s) from existing open orders.")

    # ---------- poll: เฉพาะดู TP (ไม่มี buy ค้างในโหมด market) ----------
    def poll(self, engine: SignalEngine, grid_rows: pd.DataFrame) -> None:
        if self.dry:
            return
        try:
            open_orders = self.ex.fetch_open_orders(self.symbol)
            open_ids = {o["id"] for o in open_orders}
        except Exception:
            open_ids = set()

        # TP filled → unlock level
        for level, oid in list(self.tp_ids.items()):
            if oid not in open_ids:
                engine.on_tp_filled(now_ms(), level)
                self.tp_ids.pop(level, None)
                self.buy_ids.pop(level, None)

# ===================== MAIN =====================

def main() -> None:
    print("[i] เริ่มดึงข้อมูลสดจาก Binance …")
    fetcher = DataFetcher(SYMBOL, EXCHANGE_ID, with_auth=(not DRY_RUN))
    aggr = Aggregator5s(BAR_MS)

    grid_levels = load_grid_levels_from_csv(GRID_CSV)
    if not grid_levels:
        print(f"[!] ไม่พบไฟล์กริด {GRID_CSV} หรืออ่านไม่ได้ — จะทำงานเฉพาะสัญญาณ (ไม่เช็คใกล้กริด)")
    engine = SignalEngine(
        grid_levels=grid_levels,
        confirm_needed=CONFIRM_BARS,
        cooldown_ms=COOLDOWN_MS,
        window=WINDOW,
        th_cvd=CVD_Z_TH,
        th_ts=TS_Z_TH,
    )

    grid_df = load_grid_df(GRID_CSV)
    execu = ExecutionLayer(fetcher, SYMBOL, dry_run=DRY_RUN)

    # ---- CSV logger (อิงชื่อจาก SYMBOL) ----
    csv_fh, csv_wr = init_csv_logger(LOG_CSV_PATH)

    # ---- สำคัญ: pre-lock จาก open orders ----
    execu.prelock_existing(engine, grid_df)

    last_grid_reload = time.time()

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

        # ถ้าบาร์ไม่พร้อม (ไม่มี mid/depth ครบ) ให้ข้ามเพื่อลด error formatting
        if not bar.get("ok", False):
            print(f"[{ts_str}] dq={dq} (skip partial/stale bar)")
            if csv_wr:
                csv_wr.writerow([
                    int(bar["bar_ts"]),
                    datetime.fromtimestamp(bar["bar_ts"]/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    dq, None, None, None, None, None, None, None, None, None, "SKIP_PARTIAL", "",
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
            f"active_lv={len(engine.active_levels)}"
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

            if level in engine.active_levels:
                print(f"[skip] level {level} already locked.")
            else:
                # ล็อกเลเวลก่อน (กันสัญญาณรัว ๆ)
                engine.on_order_placed(now_ms(), level)

                # ซื้อด้วย MARKET 100%
                row = match_grid_row(grid_df, level)
                coin_size = float(row["coin_size"])
                res = execu.place_market_buy(level, coin_size)
                market_id = res.get("id")
                filled_amt = res["filled"] if res["filled"] > 0 else coin_size

                if res["id"] or DRY_RUN:
                    # นับเป็นฟิลแล้ว (market) → เพิ่ม open_orders_count
                    engine.on_order_filled(now_ms(), level)
                    tp_px = float(row["tp_price"])

                    # วาง TP แบบปลอดภัย: รอฟิลล์ + เช็ค balance ก่อน
                    tp_id = execu.place_tp_after_market(level, market_id, filled_amt, tp_px)
                    if tp_id:
                        execu.tp_ids[level] = tp_id
                else:
                    # ซื้อไม่สำเร็จ → ปลดล็อกเลเวล
                    engine.active_levels.discard(level)

        execu.poll(engine, grid_df)

        elapsed = now_ms() - t0
        time.sleep(max(0.0, 1.0 - elapsed/1000.0))

if __name__ == "__main__":
    main()
