#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Live 5s bars + Signal + Grid Execution (Spot Binance, ccxt)
- สัญญาณ: MAD-zscore (window=50), ยืนยัน 1 แท่ง
- ซื้อเฉพาะเมื่อใกล้กริด ≤ GRID_TOL
- กันเปิดซ้ำระดับ: pre-lock จาก open orders, ตรวจซ้ำก่อนยิง, map order → level
- BUY = MARKET 100% แล้ววาง TP เป็น LIMIT SELL ทันที

แพตช์/อัปเดตล่าสุด
  [High-priced buy rule]
    - ถ้า best_ask ≥ HIGH_PRICE_THRESHOLD (ดีฟอลต์ 1000 USDT) → MARKET BUY ด้วย quoteOrderQty
      โดยคำนวณ q = coin_size * px_ref * QUOTE_SAFETY (ดีฟอลต์ 0.999) เพื่อเลี่ยง insufficient funds
    - ใช้ newOrderRespType='FULL' และคำนวณ avg price จาก cost/filled (ถ้ามี)

  [Dust ledger (บันทึกเศษไว้ขายมือ)]
    - ไม่จัดการเศษอัตโนมัติ แต่ “บันทึก” เศษและต้นทุนลงไฟล์ DUST_LEDGER_FILE (dust_ledger.csv)
    - กรณีที่บันทึก: 
        • lot_rounding (ปัด LOT_SIZE แล้วเหลือเศษ) 
        • minQty_gate / minNotional_gate (ตั้งขายไม่ได้เพราะต่ำกว่าเกณฑ์ตลาด) 
        • tp_place_failed (ส่ง TP แล้วถูกปฏิเสธ)
    - คอลัมน์ไฟล์: ts, symbol, remainder_qty, unit_cost_usdt, est_cost_total_usdt, 
      reason, order_id, planned_tp, stepSize, minQty, minNotional

  [API load]
    - ลด BOOK_LIMIT เหลือ 20 (พอสำหรับ top-of-book/depth 5)
    - fetch_recent_trades ทุก ~1.2s (Throttle)
    - poll open orders แบบเว้นช่วง + ตรวจสถานะรายออเดอร์ (closed/canceled)

  [ความถูกต้อง]
    - กันเทรดซ้ำด้วย timestamp+id (ไม่เทียบ id เป็นสตริงล้วน)
    - MARKET BUY ส่ง newOrderRespType='FULL'; average ใช้ cost/filled เป็นหลัก
    - ปัดราคา/จำนวนด้วย Decimal: ราคา BUY→floor, SELL→ceil; จำนวนขายปัดลงตาม LOT_SIZE

  [กติกา exchange]
    - min_notional = max(ค่าจากตลาด, MIN_NOTIONAL_OVERRIDE)
    - TP ขั้นต่ำ = max(row_tp, fill_avg*(1+2*EXEC_FEE_RATE+TP_EXTRA_MARGIN), tp_need_from_minNotional)
    - ราคา TP ฝั่งขายปัดขึ้นตาม tick, จำนวนขายปัดลงตาม LOT_SIZE
    - กัน TP ต่ำเกิน bid ด้วย best_bid * (1 + TP_BID_SAFETY_PCT)

  [UX/เสถียรภาพ]
    - pre-lock เฉพาะกรณีราคาเปิดใกล้ buy_price จริง
    - รอ balance sync สั้น ๆ ก่อนตั้ง TP
    - ปิดไฟล์ CSV อย่างปลอดภัยเมื่อ Ctrl-C (try/finally)

  [FIX บั๊กสำคัญ]
    - _pick_grid_candidate(): คืนเฉพาะเลเวลที่ “ใกล้กริดจริง” เท่านั้น (ตัด fallback)
    - update(): within_tol = |mid − level| / level ≤ GRID_TOL
    - ก่อนยิงคำสั่ง เช็คแถวใน CSV + ความใกล้กริดอีกครั้ง ไม่ผ่าน → skip และปลดล็อก
    - prelock_existing(): แม็พ SELL (TP) ด้วย tp_price และตั้ง open_orders_count จากจำนวน SELL ที่ค้าง
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

# ตั้ง precision ของ Decimal ให้พอสำหรับคริปโต
getcontext().prec = 28
def _q(x) -> Decimal: return Decimal(str(x))

# ===================== CONFIG =====================

EXCHANGE_ID = "binance"     # ชื่อเอ็กซ์เชนจ์ใน ccxt

BOOK_LIMIT = 20             # ความลึกของ OrderBook ที่ดึงต่อรอบ (พอสำหรับ top-of-book)
BAR_MS = 5_000              # 1 บาร์ = 5 วินาที
EMA_SPAN = 10               # EMA span ของ order_imbalance

GRID_TOL = 0.005            # ใกล้กริด: abs(mid - level)/level ≤ 0.5%
LOCK_TOL = 0.0015           # แมตช์ออเดอร์เปิด ↔ ระดับกริดเพื่อ “ล็อก” เลเวล

# FIX: ปิด fallback เองตามดีฟอลต์ (เพื่อไม่ให้ยิงตอนห่างกริด)
ALLOW_FALLBACK_EXEC = False

CONFIRM_BARS = 1            # ต้องติด raw-signal กี่แท่งถึงยืนยัน
COOLDOWN_MS = 60_000        # คูลดาวน์หลัง BUY สำเร็จ

# z-score threshold
WINDOW = 50                 # ขนาดหน้าต่างย้อนหลัง
CVD_Z_TH = 1.5
TS_Z_TH  = 1.5

MAX_OPEN_ORDERS = 20        # จำกัดดีลเปิดพร้อมกันสูงสุด

GRID_CSV = "grid_plan.csv"  # ไฟล์กริด (ต้องมี buy_price, coin_size, tp_price/tp_pct)
GRID_RELOAD_SEC = 0         # รีโหลดกริดอัตโนมัติทุก N วินาที (0=ปิด)

DRY_RUN = True             # True = โหมดเดโม, False = ส่งออเดอร์จริง

MIN_NOTIONAL_OVERRIDE = None   # ไม่ลดต่ำกว่าค่าจริง: ใช้ max(ค่าจากตลาด, override); ตั้ง None เพื่อใช้ค่าจากตลาดล้วน
SLIP_PCT = 0.0007           # buffer คำนวณจำนวนเหรียญขั้นต่ำ: ใช้ best_ask*(1+SLIP_PCT)

# >>> ความปลอดภัย TP <<<
EXEC_FEE_RATE = 0.0004      # 0.04% ต่อข้าง (ใช้ในการยก TP ให้พ้นค่าธรรมเนียม)
TP_EXTRA_MARGIN = 0.0005    # กันเผื่อจากฟิลจริง +0.05% (ปรับได้)
TP_BID_SAFETY_PCT = 0.0001  # 0.01% กัน TP ต่ำกว่าบิดมากไปจนโดนรับทันที

# Poll/Throttle
TRADES_POLL_MS = 2000       # ดึง recent trades อย่างน้อยทุก 2s
POLL_OPEN_ORDERS_SEC = 3.0  # โพลล์สถานะ TP รายออเดอร์ทุก 3 วินาที

# resync actual orders
RESYNC_OPEN_ORDERS_SEC = 60  # 0=ปิด

# =============== LOGGING SWITCHES ===============
SHOW_PRELOCK_SUMMARY = False      # ปิดสรุป pre-locked (บรรทัดยาวชวนงง)
SHOW_UNMAPPED_SELL_DEBUG = False  # ปิด debug unmapped SELL ... Δ=... ticks


# ==== Dust logging & high-price rule ====
HIGH_PRICE_THRESHOLD = 1000.0     # ถ้าราคา >= ค่านี้ ใช้ market buy แบบ quoteOrderQty
QUOTE_SAFETY         = 0.999      # กันงบเผื่อจิ๋ว ๆ เวลา quoteOrderQty
DUST_LEDGER_FILE     = "dust_ledger.csv"  # ไฟล์บันทึกเศษเพื่อขาย manual


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

def _normalize_symbol(raw: str) -> str:
    s = (raw or "").strip().upper().replace("\\", "/").replace("-", "/")
    if not s:
        return s
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
    """
    อ่านคอลัมน์ 'symbol' จากไฟล์ macro_montecarlo.csv (แถวสรุปสุดท้าย)
    แล้ว normalize ให้อยู่รูป BASE/QUOTE เช่น XRP/USDT
    """
    try:
        df = pd.read_csv(macro_csv)
    except Exception as e:
        raise RuntimeError(f"อ่านไฟล์ {macro_csv} ไม่ได้: {e}")
    if "symbol" not in df.columns:
        raise RuntimeError(f"ไม่พบคอลัมน์ 'symbol' ใน {macro_csv}")
    series = df["symbol"].dropna().astype(str)
    if series.empty:
        raise RuntimeError(f"คอลัมน์ 'symbol' ใน {macro_csv} ว่างเปล่า")
    return _normalize_symbol(series.iloc[-1])

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
        # ป้องกันดีดู้พ: ใช้ ts + id
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

            # ใช้เฉพาะ top-5 depth
            depth_bid_5 = float(np.sum([lvl[1] for lvl in bids[:5]]))
            depth_ask_5 = float(np.sum([lvl[1] for lvl in asks[:5]]))
            # ใช้ total volume ที่ดึงมา (20 เลเวลก็พอประมาณ)
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
        # throttle
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
                # กรองด้วย ts ก่อน ถ้าเท่ากันค่อยเทียบ id
                if self._last_trade_ts is not None:
                    if ts_i < self._last_trade_ts:
                        continue
                    if ts_i == self._last_trade_ts and self._last_trade_id and tid <= self._last_trade_id:
                        continue
                out.append({
                    "id": tid,
                    "ts": ts_i,
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
        """
        FIX: คืนเฉพาะเลเวลที่อยู่ในระยะ GRID_TOL และยังไม่ถูกล็อก
        """
        if mid is None or not self.grid_levels_open:
            return None
        near = [lv for lv in self.grid_levels_open
                if abs(lv - mid) / max(lv, 1e-12) <= self.grid_tol]
        for lv in sorted(near, key=lambda x: abs(x - mid)):
            if not self._is_locked(lv):
                return lv
        if ALLOW_FALLBACK_EXEC:
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
        """
        เกณฑ์ทนทาน: มากสุดระหว่าง LOCK_TOL แบบ % และ 5 ticks
        ทำให้ TP ที่ปัดราคาขึ้นคนละทศนิยม/ไฟล์กริดคนละเวอร์ชัน ยังจับคู่ได้
        """
        tick = self.tick_size or 0.0
        return max(LOCK_TOL * max(ref_px, 1e-9), 5.0 * tick)

    def _quote_asset(self) -> str:
        try:
            return self.market.get("quote") or self.symbol.split("/")[1]
        except Exception:
            return self.symbol.split("/")[1]

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


    def _append_dust_ledger(self,
                            remainder_qty: float,
                            unit_cost_usdt: float,
                            reason: str,
                            ctx: Optional[dict] = None) -> None:
        """
        บันทึกเศษที่ขายไม่ออกลงไฟล์ CSV:
        - remainder_qty: ปริมาณ BASE (เช่น PAXG) ที่เหลือขายไม่ได้
        - unit_cost_usdt: ต้นทุนต่อ 1 หน่วย (USDT per BASE), คิดจากคำสั่ง buy ล่าสุด
        - reason: สาเหตุสั้น ๆ (e.g., 'lot_rounding', 'minNotional_gate', 'tp_place_failed')
        - ctx: ข้อมูลประกอบ เช่น order_id, planned_tp, stepSize ฯลฯ
        """
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
            "stepSize": (ctx or {}).get("step", ""),
            "minQty": (ctx or {}).get("min_qty", ""),
            "minNotional": (ctx or {}).get("min_notional", ""),
        }
        header = list(row.keys())
        need_header = not os.path.exists(DUST_LEDGER_FILE)
        try:
            with open(DUST_LEDGER_FILE, "a", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=header)
                if need_header:
                    w.writeheader()
                w.writerow(row)
        except Exception as e:
            print(f"[WARN] cannot write {DUST_LEDGER_FILE}: {e}")


    def _avg_fill_price_from_order(self, order: dict, fallback_price: float) -> float:
        """
        พยายามอ่าน avg price จากโครงสร้าง ccxt:
        - ถ้า order มี 'cost' และ 'filled' → ใช้ cost/filled
        - ถ้าไม่มี ใช้ fallback_price (เช่น best_ask ตอนซื้อ) แทน
        """
        try:
            filled = float(order.get("filled", 0.0) or 0.0)
            cost   = float(order.get("cost",   0.0) or 0.0)
            if filled > 0 and cost > 0:
                return cost / filled
            # บาง exchange แนบ 'trades' รายการย่อยมา → รวมเองได้ แต่ส่วนใหญ่ 'cost' ถูกต้องแล้ว
        except Exception:
            pass
        return float(fallback_price or 0.0)


    def resync_open_orders(self, engine: "SignalEngine", grid_df: pd.DataFrame) -> None:
        try:
            oo = self.ex.fetch_open_orders(self.symbol)
        except Exception:
            return

        # rebuild maps from scratch
        new_active = set()
        new_tp_ids, new_buy_ids = {}, {}

        for o in oo:
            px = float(o.get("price") or 0.0)
            side = (o.get("side") or "").lower()
            lv = self.level_key_from_order(grid_df, px, side)
            if lv is None:
                continue
            new_active.add(lv)
            if side == "sell": new_tp_ids[lv] = o.get("id")
            elif side == "buy": new_buy_ids[lv] = o.get("id")

        self.tp_ids = new_tp_ids
        self.buy_ids = new_buy_ids
        engine.active_levels = new_active
        engine.open_orders_count = sum(1 for o in oo if (o.get("side") or "").lower() == "sell")

    def level_key_from_order(self, grid_df: pd.DataFrame, px: float, side: str) -> Optional[float]:
        """
        ฝั่ง BUY: แม็พกับ buy_price
        ฝั่ง SELL: แม็พกับ tp_price แล้วคืน key เป็น buy_price
        ใช้ tolerance แบบ max(% , ticks)
        """
        side = (side or "").lower()
        if px <= 0:
            return None

        if side == "sell" and "tp_price" in grid_df.columns:
            diffs = (grid_df["tp_price"] - px).abs()
            i = int(diffs.idxmin())
            ref = float(grid_df.loc[i, "tp_price"])
            tol = self._price_tol(ref)
            if abs(ref - px) <= tol:
                return float(grid_df.loc[i, "buy_price"])
            return None
        else:
            diffs = (grid_df["buy_price"] - px).abs()
            i = int(diffs.idxmin())
            ref = float(grid_df.loc[i, "buy_price"])
            tol = self._price_tol(ref)
            return float(ref) if abs(ref - px) <= tol else None

    # ---- rounding helpers (Decimal) ----
    def round_price(self, p: float, side: Optional[str] = None) -> float:
        """BUY → floor, SELL → ceil"""
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
        เหรียญแพง (ask >= HIGH_PRICE_THRESHOLD) → ซื้อแบบ quoteOrderQty = desired_amount * px_ref * QUOTE_SAFETY
        เหรียญไม่แพง → ใช้วิธีเดิม (คำนวณ BASE จาก desired_amount แล้วตรวจ notional)
        คืนค่า dict เดิม: {id, filled, avg, amt_sent, px_ref}
        """
        _bid, ask = self._best_prices()
        px_ref = ask * (1.0 + max(0.0, SLIP_PCT)) if ask > 0 else level

        if ask > 0 and ask >= HIGH_PRICE_THRESHOLD:
            # ---- เหรียญแพง: ล็อกงบเป็น USDT ด้วย quoteOrderQty ----
            quote_to_spend = max(0.0, desired_amount * px_ref * QUOTE_SAFETY)
            quote_free = float(self._get_free_quote() or 0.0)
            # ปัด 2 ทศนิยมสำหรับ USDT (safe default)
            q = math.floor(quote_to_spend * 100) / 100.0

            if (q <= 0) or (quote_free + 1e-9 < q) or (self.min_notional and q < self.min_notional):
                print(f"[skip] insufficient quote for high-priced market buy level {level} "
                      f"(quote_to_spend≈{quote_to_spend:.2f}, q≈{q:.2f}, quote_free≈{quote_free:.2f}, "
                      f"minNotional={self.min_notional})")
                return {"id": None, "filled": 0.0, "avg": None, "amt_sent": 0.0, "px_ref": px_ref}

            if self.dry:
                print(f"[DRY] MARKET BUY by quoteOrderQty={q} {self.symbol} (px_ref≈{px_ref:.8f})")
                return {"id": f"dry-mkt-quote-{level}", "filled": 0.0, "avg": None, "amt_sent": 0.0, "px_ref": px_ref}

            try:
                o = self.ex.create_order(
                    self.symbol, "market", "buy", None, None,
                    {"quoteOrderQty": q, "newClientOrderId": self._cid("gbMq", px_ref), "newOrderRespType": "FULL"}
                )
                filled = float(o.get("filled") or 0.0)
                # ใช้ cost/filled ถ้ามี เพื่อให้ avg แม่น
                if filled > 0:
                    cost = float(o.get("cost") or 0.0)
                    avg = (cost / filled) if cost > 0 else float(o.get("average") or px_ref)
                else:
                    avg = None
                return {"id": o.get("id"), "filled": filled, "avg": avg, "px_ref": px_ref, "amt_sent": 0.0}
            except Exception as e:
                print(f"[ERR] market buy (quoteOrderQty) failed: {e}")
                return {"id": None, "filled": 0.0, "avg": None, "amt_sent": 0.0, "px_ref": px_ref}

        # ---- เหรียญไม่แพง: วิธีเดิม (คำนวณ BASE แล้วตรวจ notional) ----
        need_amt = self.ensure_min_notional(px_ref, desired_amount)
        quote_free = self._get_free_quote()
        affordable_amt = self.round_amount_down(max(0.0, quote_free / max(px_ref, 1e-12)))
        amt = min(need_amt, affordable_amt)

        if (amt <= 0) or (self.min_qty and amt < self.min_qty) or \
           (self.min_notional and amt * px_ref < self.min_notional):
            print(f"[skip] insufficient quote balance for level {level} "
                  f"(need≈{need_amt}, affordable≈{affordable_amt}, quote_free≈{quote_free})")
            return {"id": None, "filled": 0.0, "avg": None, "amt_sent": 0.0, "px_ref": px_ref}

        if self.dry:
            print(f"[DRY] MARKET BUY {self.symbol} amt≈{amt} (px_ref≈{px_ref:.8f})")
            return {"id": f"dry-mkt-{level}", "filled": amt, "avg": px_ref, "amt_sent": amt, "px_ref": px_ref}

        try:
            o = self.ex.create_order(
                self.symbol, "market", "buy", amt, None,
                {"newClientOrderId": self._cid("gbM", px_ref), "newOrderRespType": "FULL"}
            )
            filled = float(o.get("filled") or 0.0)
            avg    = float(o.get("average") or px_ref) if filled > 0 else None
            return {"id": o.get("id"), "filled": filled, "avg": avg, "px_ref": px_ref, "amt_sent": amt}
        except Exception as e:
            print(f"[ERR] market buy failed: {e}")
            return {"id": None, "filled": 0.0, "avg": None, "amt_sent": 0.0, "px_ref": px_ref}


    # ---------- LIMIT SELL TP (Decimal: ราคา ceil, จำนวน floor) ----------
    def place_limit_sell_tp(self, level: float, amount: float, tp_price: float) -> Optional[str]:
        px = self.round_price(tp_price, side="sell")          # ปัดขึ้น
        amt = self.round_amount_down(amount)                  # ปัดลง
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

    # ---------- helper: base asset / balance ----------
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
        """floor ตาม LOT_SIZE และบังคับผ่าน minQty/minNotional"""
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
        """
        รอฟิลล์/เช็ค balance แล้ววาง TP ด้วยจำนวนที่ปลอดภัย:
        - ปัดจำนวนลงตาม LOT_SIZE
        - ถ้า amt_floor*tp ยัง < minNotional → ยก tp ให้พ้น notional
        - บังคับ TP ≥ best_bid*(1+TP_BID_SAFETY_PCT)
        - บันทึกเศษ (ส่วนต่างที่ขายไม่ได้) ลง dust_ledger.csv พร้อมต้นทุนเฉลี่ย
        """
        if self.dry:
            print(f"[DRY] place TP after market: tp_price={tp_price}")
            return f"dry-tp-{level}"

        # ดึงคำสั่งซื้อจริง (เพื่อรู้ filled + cost → avg cost)
        order_obj = None
        filled_from_order = 0.0
        try:
            if market_order_id:
                order_obj = self.ex.fetch_order(market_order_id, self.symbol)
                filled_from_order = float(order_obj.get("filled") or 0.0)
        except Exception:
            # fallback: ใช้วิธีรอแบบเดิม
            filled_from_order = self._wait_filled(market_order_id, 2.0) if market_order_id else 0.0

        # best ask ไว้เป็น fallback สำหรับ avg cost
        _best_bid, best_ask = self._best_prices()
        avg_cost = self._avg_fill_price_from_order(order_obj or {}, fallback_price=best_ask or tp_price)

        time.sleep(0.25)  # ให้ balance sync
        free_base = self._get_free_base()

        cap = min(max(desired_amt, 0.0),
                  filled_from_order if filled_from_order > 0 else free_base,
                  free_base)

        amt_floor = self.round_amount_down(cap)

        # บันทึกเศษจากการปัด LOT_SIZE (ถ้ามี)
        remainder = max(0.0, cap - amt_floor)
        if remainder > 0:
            self._append_dust_ledger(
                remainder_qty=remainder,
                unit_cost_usdt=avg_cost,
                reason="lot_rounding",
                ctx={"order_id": market_order_id, "planned_tp": tp_price,
                     "step": self.step_size, "min_qty": self.min_qty, "min_notional": self.min_notional}
            )

        # Gate: minQty
        if self.min_qty and amt_floor < self.min_qty:
            # ทั้งก้อนเป็นเศษขายไม่ได้
            self._append_dust_ledger(
                remainder_qty=amt_floor,
                unit_cost_usdt=avg_cost,
                reason="minQty_gate",
                ctx={"order_id": market_order_id, "planned_tp": tp_price,
                     "step": self.step_size, "min_qty": self.min_qty, "min_notional": self.min_notional}
            )
            print(f"[skip] TP not placed (amt<{self.min_qty} minQty | free={free_base} | filled={filled_from_order})")
            return None

        # ปรับราคา TP ให้พ้น minNotional ถ้าจำเป็น
        if (self.min_notional or 0.0) > 0 and amt_floor > 0:
            tp_need = (self.min_notional + 1e-12) / amt_floor
            tp_price = max(tp_price, tp_need)

        best_bid, _ = self._best_prices()
        if best_bid > 0:
            tp_price = max(tp_price, best_bid * (1.0 + TP_BID_SAFETY_PCT))

        sell_amt = self._safe_tp_amount(amt_floor, tp_price)
        if sell_amt <= 0:
            # ตั้งขายไม่ได้ → บันทึกทั้งก้อนเป็นเศษ
            self._append_dust_ledger(
                remainder_qty=amt_floor,
                unit_cost_usdt=avg_cost,
                reason="minNotional_gate" if (self.min_notional and amt_floor * tp_price < self.min_notional) else "tp_not_placed",
                ctx={"order_id": market_order_id, "planned_tp": tp_price,
                     "step": self.step_size, "min_qty": self.min_qty, "min_notional": self.min_notional}
            )
            print(f"[skip] TP not placed (sell_amt={sell_amt} | free={free_base} | filled={filled_from_order} "
                  f"| minQty={self.min_qty} | minNotional={self.min_notional} | step={self.step_size})")
            return None

        # วาง LIMIT SELL TP
        try:
            return self.place_limit_sell_tp(level, sell_amt, tp_price)
        except Exception as e:
            # วางไม่ผ่าน → บันทึกส่วนที่ตั้งใจจะขายเป็นเศษเพื่อให้ไปขายเอง
            self._append_dust_ledger(
                remainder_qty=sell_amt,
                unit_cost_usdt=avg_cost,
                reason="tp_place_failed",
                ctx={"order_id": market_order_id, "planned_tp": tp_price,
                     "step": self.step_size, "min_qty": self.min_qty, "min_notional": self.min_notional}
            )
            print(f"[ERR] place TP failed @ {tp_price}: {e}")
            return None


    # ---------- pre-lock existing open orders ----------
    def prelock_existing(self, engine: "SignalEngine", grid_df: pd.DataFrame) -> None:
        try:
            oo = self.ex.fetch_open_orders(self.symbol)
        except Exception:
            oo = []

        buys = sells = locked = 0

        # นับจำนวน SELL จริงในพอร์ต (ใช้เป็น open deals แบบ ground truth)
        exchange_sells = sum(1 for o in oo if (o.get("side") or "").lower() == "sell")
        engine.open_orders_count = exchange_sells  # ให้ตัวเลข 'open=' เท่าความจริงทันทีตั้งแต่เริ่ม

        # loop แม็พ order -> level
        for o in oo:
            px = float(o.get("price") or 0.0)
            side = (o.get("side") or "").lower()

            lv = self.level_key_from_order(grid_df, px, side)

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

    # โหลดกริด (สำหรับสัญญาณ/การแมตช์ระดับ)
    grid_levels = load_grid_levels_from_csv(GRID_CSV)
    if not grid_levels:
        print(f"[!] ไม่พบไฟล์กริด {GRID_CSV} หรืออ่านไม่ได้ — จะทำงานเฉพาะสัญญาณ (ไม่เช็คใกล้กริด)")
    try:
        grid_df = load_grid_df(GRID_CSV)
    except Exception:
        grid_df = pd.DataFrame(columns=["buy_price","coin_size","tp_price"])


    # <<< สำคัญ: อ่านสัญลักษณ์จาก macro_montecarlo.csv >>>
    symbol = detect_symbol_from_macro("macro_montecarlo.csv")
    symbol_safe = symbol.replace("/", "").lower()
    log_csv_path = os.path.join("logs", f"{symbol_safe}_5s_decisions.csv")
    print(f"[i] Trading symbol = {symbol} (from macro_montecarlo.csv)")

    # Engine / Fetcher / Executor
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

    
    # CSV logger (ตาม symbol)
    csv_fh, csv_wr = init_csv_logger(log_csv_path)

    # pre-lock จาก open orders
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

                # ยิง MARKET BUY
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

                    # วาง TP แบบปลอดภัย (รอฟิลล์ + เช็ค balance + notional)
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

if __name__ == "__main__":
    main()
