#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PAXG/USDT Grid Bot (Spot Binance, ccxt) — 5s live bars + Signal + Grid Execution

เฉพาะกิจสำหรับ PAXG/USDT:
- ตรวจ filter ตลาด (tickSize, stepSize, minQty, (MIN_)NOTIONAL) ทุกครั้งตอนเริ่ม
- คำนวณ "จำนวนขั้นต่ำที่ต้องซื้อจริง" = max(minQty, ceil(minNotional/ask, stepSize))
- ถ้า coin_size ใน grid ต่ำกว่าขั้นต่ำ → ยกจำนวนขึ้นอัตโนมัติ (log บอก)
- TP SELL: ปัดราคา(ceil)/จำนวน(floor), ยก TP ให้พ้นค่าธรรมเนียม, บังคับผ่าน minNotional,
  กัน TP ต่ำกว่าบิดด้วย best_bid*(1+TP_BID_SAFETY_PCT)
- เศษที่ขายไม่ได้ (ต่ำกว่า step/minQty/notional) บันทึกลง dust_ledger.csv

ไฟล์ที่ใช้:
- grid_plan.csv   (คอลัมน์: buy_price, coin_size, tp_price หรือ tp_pct)
- logs/paxgusdt_5s_decisions.csv
- dust_ledger.csv (บันทึกเศษ)

หมายเหตุ:
- ใช้ newOrderRespType='FULL' เพื่ออ่านค่า average/cost
"""

import os, time, math, csv
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING, getcontext
from typing import Optional, List, Dict, Set, Tuple

import ccxt
import numpy as np
import pandas as pd

# ---------- CONFIG เฉพาะกิจ PAXG ----------
EXCHANGE_ID = "binance"
SYMBOL = "PAXG/USDT"             # ล็อกสัญลักษณ์
BOOK_LIMIT = 20
BAR_MS = 5_000                   # 5s bar
EMA_SPAN = 10

GRID_TOL = 0.002                 # 0.5%
LOCK_TOL = 0.0015
ALLOW_FALLBACK_EXEC = False

WINDOW = 50
CVD_Z_TH = 2.0
TS_Z_TH = 1.5
CONFIRM_BARS = 1
COOLDOWN_MS = 60_000

MAX_OPEN_ORDERS = 10
GRID_CSV = "grid_plan.csv"
GRID_RELOAD_SEC = 0

DRY_RUN = False                  # ตั้ง True เพื่อทดสอบไม่ยิงจริง

# ความปลอดภัย TP
EXEC_FEE_RATE = 0.0004
TP_EXTRA_MARGIN = 0.0005
TP_BID_SAFETY_PCT = 0.0001
SLIP_PCT = 0.0007

# Poll / Sync
TRADES_POLL_MS = 1200
POLL_OPEN_ORDERS_SEC = 3.0
RESYNC_OPEN_ORDERS_SEC = 60

# CSV paths
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
DECISIONS_CSV = os.path.join(LOG_DIR, "paxgusdt_5s_decisions.csv")
DUST_LEDGER_FILE = "dust_ledger.csv"

# Decimal precision
getcontext().prec = 28
def _q(x) -> Decimal: return Decimal(str(x))


# ---------- Utils ----------
def now_ms() -> int: return int(time.time() * 1000)

def sf(x, nd=5) -> str:
    try:
        v = float(x)
        if not np.isfinite(v): return "nan"
        return f"{v:.{nd}f}"
    except Exception:
        return "nan"

def ema_update(prev: Optional[float], x: float, span: int) -> float:
    if prev is None: return x
    a = 2.0 / (span + 1.0)
    return (1-a)*prev + a*x

def load_grid_df(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "tp_price" not in df.columns:
        if "tp_pct" in df.columns:
            df["tp_price"] = df["buy_price"] * (1.0 + df["tp_pct"].astype(float))
        else:
            df["tp_price"] = df["buy_price"] * 1.01
    return df[["buy_price", "coin_size", "tp_price"]].copy()

def load_grid_levels(path: str) -> List[float]:
    try:
        df = pd.read_csv(path)
        return sorted([float(x) for x in df["buy_price"].tolist()])
    except Exception:
        return []

def match_grid_row(grid_df: pd.DataFrame, level: float, tol: float = GRID_TOL) -> pd.Series:
    diffs = (grid_df["buy_price"] - level).abs()
    i = diffs.idxmin()
    row = grid_df.loc[i]
    if abs(row["buy_price"] - level) / max(level, 1e-9) > tol:
        return pd.Series({"buy_price": level, "coin_size": 0.0, "tp_price": level * 1.01})
    return row

def init_csv_logger(path: str):
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


# ---------- Data fetch ----------
class DataFetcher:
    def __init__(self, symbol: str, with_auth: bool = True):
        self.symbol = symbol
        kwargs = {"enableRateLimit": True}
        if with_auth:
            kwargs["apiKey"] = os.getenv("BINANCE_KEY", "")
            kwargs["secret"] = os.getenv("BINANCE_SECRET", "")
        self.ex = getattr(ccxt, EXCHANGE_ID)(kwargs)
        self.ex.load_markets()
        self._ema: Optional[float] = None
        self._last_trade_ts: Optional[int] = None
        self._last_trade_id: Optional[str] = None
        self._last_trades_fetch_ms: int = 0

    def market_filters(self) -> Tuple[float, float, float, float]:
        """tickSize, stepSize, minQty, minNotional"""
        m = self.ex.market(self.symbol)
        tick = step = min_qty = min_notional = None
        for f in m.get("info", {}).get("filters", []):
            t = f.get("filterType")
            if t == "PRICE_FILTER":
                tick = float(f.get("tickSize", "0.0001"))
            elif t == "LOT_SIZE":
                step = float(f.get("stepSize", "0.0001"))
                min_qty = float(f.get("minQty", "0"))
            elif t in ("MIN_NOTIONAL", "NOTIONAL"):
                mn = f.get("minNotional") or f.get("notional")
                if mn is not None: min_notional = float(mn)
        if tick is None: tick = 10 ** -(m.get("precision", {}).get("price", 4))
        if step is None: step = 10 ** -(m.get("precision", {}).get("amount", 4))
        return float(tick), float(step), float(min_qty or 0.0), float(min_notional or 0.0)

    def fetch_orderbook(self) -> Dict:
        try:
            ob = self.ex.fetch_order_book(self.symbol, limit=BOOK_LIMIT)
            ts = ob.get("timestamp") or now_ms()
            bids = ob.get("bids", []) or []
            asks = ob.get("asks", []) or []
            if not bids or not asks:
                return {"ok": False, "error": "empty_book"}
            best_bid = float(bids[0][0]); best_ask = float(asks[0][0])
            mid = (best_bid + best_ask)/2.0
            tb = float(np.sum([x[1] for x in bids]))
            ta = float(np.sum([x[1] for x in asks]))
            denom = tb + ta
            raw = 0.0 if denom <= 0 else (tb - ta)/denom
            raw = float(np.clip(raw, -0.99, 0.99))
            self._ema = ema_update(self._ema, raw, EMA_SPAN)
            imb = float(self._ema)
            return {"ok":True,"ts":ts,
                    "best_bid":best_bid,"best_ask":best_ask,"mid_price":mid,
                    "total_bid_volume":tb,"total_ask_volume":ta,"order_imbalance":imb}
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}

    def fetch_recent_trades(self, limit: int = 100) -> Dict:
        nowms = now_ms()
        if nowms - self._last_trades_fetch_ms < TRADES_POLL_MS:
            return {"ok": True, "trades": [], "error": None}
        self._last_trades_fetch_ms = nowms
        try:
            ts = []
            for t in self.ex.fetch_trades(self.symbol, limit=limit):
                tid = str(t.get("id",""))
                ts_i = int(t.get("timestamp") or now_ms())
                if self._last_trade_ts is not None:
                    if ts_i < self._last_trade_ts: continue
                    if ts_i == self._last_trade_ts and self._last_trade_id and tid <= self._last_trade_id:
                        continue
                ts.append({"id":tid,"ts":ts_i,"price":float(t["price"]),"amount":float(t["amount"]),
                          "cost": float(t["price"])*float(t["amount"])})
            if ts:
                self._last_trade_ts = ts[-1]["ts"]; self._last_trade_id = ts[-1]["id"]
            return {"ok": True, "trades": ts, "error": None}
        except Exception as e:
            return {"ok": False, "trades": [], "error": f"{type(e).__name__}: {e}"}


# ---------- Aggregator 5s ----------
class Aggregator5s:
    def __init__(self, bar_ms: int = BAR_MS):
        self.bar_ms = bar_ms
        self.cur: Optional[int] = None
        self.obs: List[Dict] = []
        self.trs: List[Dict] = []

    def _ws(self, ts_ms: int) -> int: return ts_ms - (ts_ms % self.bar_ms)

    def add_ob(self, snap: Dict):
        if not snap.get("ok"): return
        ts = int(snap["ts"]); ws = self._ws(ts)
        if self.cur is None: self.cur = ws
        self.obs.append(snap)

    def add_trades(self, payload: Dict):
        if not payload.get("ok"): return
        self.trs.extend(payload["trades"])

    def roll(self, now_ms_: int) -> Optional[Dict]:
        if self.cur is None:
            self.cur = self._ws(now_ms_); return None
        if now_ms_ < self.cur + self.bar_ms: return None

        ob = pd.DataFrame(self.obs) if self.obs else pd.DataFrame()
        def mean_or_none(c): return float(ob[c].mean()) if (not ob.empty and c in ob) else None
        mid = mean_or_none("mid_price"); tb = mean_or_none("total_bid_volume"); ta = mean_or_none("total_ask_volume")
        imb = mean_or_none("order_imbalance")

        tdf = pd.DataFrame(self.trs) if self.trs else pd.DataFrame()
        if not tdf.empty:
            vol_sum = float(tdf["amount"].sum())
        else: vol_sum = 0.0

        ok_flag = all(v is not None for v in [mid,tb,ta,imb])
        dq = "ok" if (ok_flag and vol_sum>0) else ("partial" if ok_flag else "stale")
        bar = {"ok": ok_flag, "bar_ts": self.cur, "mid_price_5s": mid,
               "total_bid_volume_5s": tb, "total_ask_volume_5s": ta,
               "order_imbalance_5s": imb, "data_quality": dq}
        self.obs.clear(); self.trs.clear(); self.cur += self.bar_ms
        return bar


# ---------- Signal ----------
class SignalEngine:
    def __init__(self, grid: List[float], grid_tol=GRID_TOL, confirm_needed=CONFIRM_BARS,
                 cooldown_ms=COOLDOWN_MS, window=WINDOW, th_cvd=CVD_Z_TH, th_ts=TS_Z_TH):
        self.levels: List[float] = sorted(grid)
        self.grid_tol = grid_tol
        self.confirm_needed = confirm_needed
        self.cooldown_ms = cooldown_ms
        self.window = window; self.th_cvd=th_cvd; self.th_ts=th_ts
        self.cvd_series: deque = deque(maxlen=window)
        self.ts_series: deque = deque(maxlen=window)
        self.cvd = 0.0; self.confirm=0; self.cooldown_until=0
        self.open_orders=0; self.active: Set[float] = set(); self.bars=0

    @staticmethod
    def mad_z(arr: List[float], x: float) -> float:
        if len(arr)<1: return np.nan
        a = np.asarray(arr, dtype=float); med=np.median(a); mad=np.median(np.abs(a-med))
        return 0.6745 * (x - med) / max(mad, 1e-9)

    def _locked(self, lv: float) -> bool:
        if lv in self.active: return True
        for a in self.active:
            if abs(lv-a)/max(lv,1e-9)<=LOCK_TOL: return True
        return False

    def _pick(self, mid: Optional[float]) -> Optional[float]:
        if mid is None or not self.levels: return None
        near = [lv for lv in self.levels if abs(lv-mid)/max(lv,1e-12)<=self.grid_tol]
        for lv in sorted(near, key=lambda x: abs(x-mid)):
            if not self._locked(lv): return lv
        if ALLOW_FALLBACK_EXEC:
            lowers=[lv for lv in self.levels if lv<mid and not self._locked(lv)]
            if lowers: return max(lowers)
        return None

    def update(self, bar: Dict, now_ms_: int, max_open: int) -> Dict:
        tb=bar.get("total_bid_volume_5s") or 0.0; ta=bar.get("total_ask_volume_5s") or 0.0
        imb=bar.get("order_imbalance_5s") or 0.0; vol = tb+ta
        self.cvd = float(self.cvd + (imb*vol)); self.cvd_series.append(self.cvd)
        ts_buy = max(0.0, (tb if imb>0 else 0.0)); self.ts_series.append(ts_buy)
        cvd_z = self.mad_z(list(self.cvd_series), self.cvd) if len(self.cvd_series)>=self.window else np.nan
        ts_z  = self.mad_z(list(self.ts_series), ts_buy) if len(self.ts_series)>=self.window else np.nan
        raw = int((not math.isnan(cvd_z)) and (not math.isnan(ts_z)) and cvd_z>=self.th_cvd and ts_z>=self.th_ts)
        self.confirm = self.confirm+1 if raw==1 else 0
        confirmed = int(self.confirm>=self.confirm_needed)
        cooldown = now_ms_ < self.cooldown_until
        under = self.open_orders < max_open
        mid = bar.get("mid_price_5s"); cand = self._pick(mid)
        within = cand is not None and mid is not None and abs(cand-mid)/max(cand,1e-9)<=self.grid_tol
        free = (cand is not None) and (not self._locked(cand))
        if confirmed and (not cooldown) and under and within and free:
            action, reason = "PLACE_BUY", "signal_confirmed&&near_grid&&under_limits&&level_free"
        elif cand is not None and not within:
            action, reason = "HOLD", "candidate_far_from_grid"
        elif cand is not None and not free:
            action, reason = "HOLD", "level_locked"
        else:
            action, reason = "HOLD", "waiting"
        self.bars += 1
        return {"bar_ts":bar.get("bar_ts"),"mid_price_5s":mid,"cvd":self.cvd,"cvd_z":cvd_z,
                "trade_size_buy_5s":ts_buy,"trade_size_z":ts_z,"buy_signal_raw":raw,
                "confirm_count":self.confirm,"buy_signal_confirmed":confirmed,"grid_candidate":cand,
                "within_0p5pct":within,"action":action,"reason":reason}


# ---------- Execution (PAXG aware) ----------
class ExecutionLayer:
    def __init__(self, fetcher: DataFetcher, dry: bool):
        self.ex = fetcher.ex
        self.symbol = SYMBOL
        self.dry = dry
        self.tick, self.step, self.min_qty, self.min_notional = fetcher.market_filters()
        self.market = self.ex.market(self.symbol)
        self.buy_ids: Dict[float,str] = {}; self.tp_ids: Dict[float,str] = {}
        self._last_poll_ts = 0.0

        print(f"[i] Filters {self.symbol} tick={self.tick} step={self.step} "
              f"minQty={self.min_qty} minNotional={self.min_notional}")

    # rounding helpers
    def round_price(self, p: float, side: Optional[str]=None) -> float:
        if (self.tick or 0)<=0: return p
        q = _q(p)/_q(self.tick)
        q = q.to_integral_value(rounding=(ROUND_CEILING if side=="sell" else ROUND_FLOOR))
        return float(q*_q(self.tick))

    def round_up_amount(self, a: float) -> float:
        q = (_q(a)/_q(self.step)).to_integral_value(rounding=ROUND_CEILING)
        return float(q*_q(self.step))

    def round_down_amount(self, a: float) -> float:
        q = (_q(a)/_q(self.step)).to_integral_value(rounding=ROUND_FLOOR)
        return float(q*_q(self.step))

    def _best(self) -> Tuple[float,float]:
        try:
            ob = self.ex.fetch_order_book(self.symbol, limit=5)
            return float(ob["bids"][0][0]), float(ob["asks"][0][0])
        except Exception:
            return (0.0, 0.0)

    def _quote_asset(self) -> str:
        return self.market.get("quote") or self.symbol.split("/")[1]

    def _free_quote(self) -> float:
        try:
            bal = self.ex.fetch_balance()
            q = self._quote_asset()
            if "free" in bal and q in bal["free"]: return float(bal["free"][q])
            if q in bal and isinstance(bal[q], dict): return float(bal[q].get("free",0.0))
        except Exception:
            pass
        return 0.0

    def _free_base(self) -> float:
        try:
            bal = self.ex.fetch_balance()
            b = self.market.get("base") or self.symbol.split("/")[0]
            if "free" in bal and b in bal["free"]: return float(bal["free"][b])
            if b in bal and isinstance(bal[b], dict): return float(bal[b].get("free",0.0))
        except Exception:
            pass
        return 0.0

    def _min_qty_needed(self, price: float) -> float:
        """จำนวนขั้นต่ำที่ 'ต้อง' ซื้อเพื่อผ่านตลาด"""
        need_by_notional = (self.min_notional / max(price, 1e-12)) if (self.min_notional>0) else 0.0
        raw = max(self.min_qty, need_by_notional)
        return self.round_up_amount(raw)

    def _price_tol(self, ref_px: float) -> float:
        return max(LOCK_TOL * max(ref_px, 1e-9), 5.0 * self.tick)

    # ---- MARKET BUY ----
    def place_market_buy(self, level: float, desired_amount: float) -> Dict:
        bid, ask = self._best()
        px_ref = ask * (1.0 + max(0.0, SLIP_PCT)) if ask>0 else level

        # uplift coin_size ให้พ้นขั้นต่ำของตลาด PAXG
        min_need = self._min_qty_needed(px_ref)
        amt_goal = max(desired_amount, min_need)
        if amt_goal > desired_amount:
            print(f"[i] uplift coin_size from grid {desired_amount} -> {amt_goal} (pass min rules)")

        quote_free = self._free_quote()
        affordable = self.round_down_amount(max(0.0, quote_free / max(px_ref,1e-12)))
        amt = min(amt_goal, affordable)

        if (amt <= 0) or (amt < self.min_qty) or (self.min_notional and amt*px_ref < self.min_notional):
            print(f"[ERR] insufficient to place MARKET BUY: need≈{sf(amt_goal)} "
                  f"affordable≈{sf(affordable)} quote_free≈{sf(quote_free)} "
                  f"minQty={self.min_qty} minNotional={self.min_notional}")
            return {"id": None, "filled": 0.0, "avg": None, "px_ref": px_ref}

        if self.dry:
            print(f"[DRY] MARKET BUY {self.symbol} amt≈{amt} @≈{px_ref:.6f}")
            return {"id": f"dry-mkt-{level}", "filled": amt, "avg": px_ref, "px_ref": px_ref}

        try:
            o = self.ex.create_order(
                self.symbol, "market", "buy", amt, None,
                {"newOrderRespType": "FULL"}
            )
            filled = float(o.get("filled") or 0.0)
            avg = float(o.get("cost") or 0.0)/filled if (filled>0 and float(o.get("cost") or 0.0)>0) \
                  else float(o.get("average") or px_ref)
            return {"id": o.get("id"), "filled": filled, "avg": avg, "px_ref": px_ref}
        except Exception as e:
            print(f"[ERR] market buy failed: {e}")
            return {"id": None, "filled": 0.0, "avg": None, "px_ref": px_ref}

    # ---- LIMIT SELL TP ----
    def _safe_tp_amount(self, desired_amt: float, tp_price: float) -> float:
        amt = self.round_down_amount(max(0.0, desired_amt))
        if self.min_qty and amt < self.min_qty: return 0.0
        if self.min_notional and (amt * tp_price < self.min_notional): return 0.0
        return amt

    def place_limit_sell_tp(self, level: float, amount: float, tp_price: float) -> Optional[str]:
        px = self.round_price(tp_price, side="sell")
        amt = self.round_down_amount(amount)
        if amt <= 0: return None
        if self.dry:
            print(f"[DRY] place TP SELL {self.symbol} px={px} amt={amt}")
            return f"dry-tp-{level}"
        try:
            o = self.ex.create_order(self.symbol, "limit", "sell", amt, px, {"timeInForce":"GTC"})
            return o.get("id")
        except Exception as e:
            print(f"[ERR] place TP failed @ {px}: {e}")
            return None

    def _append_dust(self, remainder_qty: float, unit_cost_usdt: float, reason: str, ctx: Dict=None):
        if remainder_qty is None or remainder_qty <= 0: return
        row = {
            "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": self.symbol,
            "remainder_qty": f"{float(remainder_qty):.10f}",
            "unit_cost_usdt": f"{float(unit_cost_usdt):.8f}",
            "est_cost_total_usdt": f"{float(remainder_qty)*float(unit_cost_usdt):.8f}",
            "reason": reason,
            "minQty": self.min_qty, "minNotional": self.min_notional,
            **(ctx or {})
        }
        header = list(row.keys())
        need_header = not os.path.exists(DUST_LEDGER_FILE)
        try:
            with open(DUST_LEDGER_FILE, "a", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=header)
                if need_header: w.writeheader()
                w.writerow(row)
        except Exception as e:
            print(f"[WARN] cannot write dust_ledger: {e}")

    def place_tp_after_market(self, level: float, market_order_id: Optional[str],
                              desired_amt: float, row_tp: float) -> Optional[str]:
        if self.dry:
            return f"dry-tp-{level}"

        # รอ balance sync สั้น ๆ
        time.sleep(0.25)
        free_base = self._free_base()

        # ใช้จำนวนที่ตั้งใจ/ยอดฟรี/ยอดฟิลล์จริง (ถ้าดึงได้)
        amt_cap = min(max(desired_amt, 0.0), free_base)
        amt_floor = self.round_down_amount(amt_cap)
        remainder = max(0.0, amt_cap - amt_floor)
        if remainder > 0:
            self._append_dust(remainder, unit_cost_usdt=row_tp, reason="lot_rounding")

        # ยก TP ให้พ้นค่าธรรมเนียม
        safe_tp_min = row_tp * (1.0 + (2.0*EXEC_FEE_RATE) + TP_EXTRA_MARGIN)
        tp_px = max(row_tp, safe_tp_min)

        # ต้องไม่ต่ำกว่า bid
        best_bid, _ = self._best()
        if best_bid > 0: tp_px = max(tp_px, best_bid*(1.0+TP_BID_SAFETY_PCT))

        # ถ้า notional ต่ำเกิน → ยก tp ขึ้นพ้น notional
        if (self.min_notional or 0.0) > 0 and amt_floor > 0:
            tp_need = (self.min_notional + 1e-12) / amt_floor
            tp_px = max(tp_px, tp_need)

        sell_amt = self._safe_tp_amount(amt_floor, tp_px)
        if sell_amt <= 0:
            self._append_dust(amt_floor, unit_cost_usdt=row_tp,
                              reason=("minNotional_gate" if (self.min_notional and amt_floor*tp_px<self.min_notional)
                                      else "tp_not_placed"))
            print(f"[ERR] TP not placed (sell_amt={sell_amt} free={free_base} floor={amt_floor})")
            return None
        return self.place_limit_sell_tp(level, sell_amt, tp_px)

    # polling TP states
    def poll(self, engine: "SignalEngine", grid_df: pd.DataFrame):
        if self.dry or not self.tp_ids: return
        now = time.time()
        if now - self._last_poll_ts < POLL_OPEN_ORDERS_SEC: return
        self._last_poll_ts = now
        for level, oid in list(self.tp_ids.items()):
            try:
                o = self.ex.fetch_order(oid, self.symbol)
                st = (o.get("status") or "").lower()
                if st == "closed":
                    engine.open_orders = max(0, engine.open_orders-1)
                    engine.active.discard(level)
                    self.tp_ids.pop(level, None); self.buy_ids.pop(level, None)
                elif st == "canceled":
                    engine.active.discard(level)
                    self.tp_ids.pop(level, None); self.buy_ids.pop(level, None)
            except Exception:
                pass

    def prelock_existing(self, engine: "SignalEngine", grid_df: pd.DataFrame):
        try:
            oo = self.ex.fetch_open_orders(self.symbol)
        except Exception:
            oo = []
        engine.open_orders = sum(1 for o in oo if (o.get("side") or "").lower()=="sell")
        for o in oo:
            px = float(o.get("price") or 0.0); side = (o.get("side") or "").lower()
            lv = self._level_key_from_order(grid_df, px, side)
            if lv is None: continue
            engine.active.add(lv)
            if side=="sell": self.tp_ids[lv]=o.get("id")
            elif side=="buy": self.buy_ids[lv]=o.get("id")

    def _level_key_from_order(self, grid_df: pd.DataFrame, px: float, side: str) -> Optional[float]:
        if px<=0: return None
        if side=="sell" and "tp_price" in grid_df.columns:
            diffs=(grid_df["tp_price"]-px).abs(); i=int(diffs.idxmin())
            ref=float(grid_df.loc[i,"tp_price"]); tol=self._price_tol(ref)
            return float(grid_df.loc[i,"buy_price"]) if abs(ref-px)<=tol else None
        diffs=(grid_df["buy_price"]-px).abs(); i=int(diffs.idxmin())
        ref=float(grid_df.loc[i,"buy_price"]); tol=self._price_tol(ref)
        return float(ref) if abs(ref-px)<=tol else None


# ---------- Main ----------
def main():
    print(f"[i] Start PAXG bot on {EXCHANGE_ID} …")
    os.makedirs(LOG_DIR, exist_ok=True)

    # grid
    levels = load_grid_levels(GRID_CSV)
    try:
        grid_df = load_grid_df(GRID_CSV)
    except Exception:
        grid_df = pd.DataFrame(columns=["buy_price","coin_size","tp_price"])

    fetcher = DataFetcher(SYMBOL, with_auth=(not DRY_RUN))
    execu = ExecutionLayer(fetcher, dry=DRY_RUN)
    print(f"[i] Using symbol={SYMBOL} | decisions log: {DECISIONS_CSV}")

    # logger
    csv_fh, csv_wr = init_csv_logger(DECISIONS_CSV)

    # engine/agg
    eng = SignalEngine(levels)
    aggr = Aggregator5s(BAR_MS)

    # prelock
    execu.prelock_existing(eng, grid_df)

    last_reload = time.time()
    last_resync = time.time()

    try:
        while True:
            t0 = now_ms()

            if GRID_RELOAD_SEC>0 and (time.time()-last_reload)>=GRID_RELOAD_SEC:
                try:
                    levels = load_grid_levels(GRID_CSV)
                    if levels:
                        eng.levels = sorted(levels)
                        grid_df = load_grid_df(GRID_CSV)
                        print("[i] reloaded grid_plan.csv")
                except Exception as e:
                    print(f"[ERR] reload grid failed: {e}")
                last_reload = time.time()

            if RESYNC_OPEN_ORDERS_SEC>0 and (time.time()-last_resync)>=RESYNC_OPEN_ORDERS_SEC:
                execu.prelock_existing(eng, grid_df); last_resync=time.time()

            ob = fetcher.fetch_orderbook(); tr = fetcher.fetch_recent_trades(limit=100)
            aggr.add_ob(ob); aggr.add_trades(tr)

            bar = aggr.roll(now_ms())
            if not bar:
                time.sleep(0.01); continue

            ts_str = datetime.fromtimestamp(bar["bar_ts"]/1000, tz=timezone.utc).strftime("%H:%M:%S")
            dq = bar.get("data_quality","n/a")

            if not bar.get("ok", False):
                print(f"[{ts_str}] dq={dq} (skip)")
                csv_wr.writerow([int(bar["bar_ts"]),
                                 datetime.fromtimestamp(bar["bar_ts"]/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                                 dq, None,None,None,None,None,None,None,None,None,None,"SKIP_PARTIAL","",
                                 eng.bars, len(eng.active)])
                csv_fh.flush()
                continue

            dec = eng.update(bar, now_ms(), MAX_OPEN_ORDERS)
            print(f"[{ts_str}] mid={sf(dec['mid_price_5s'])} cvd_z={sf(dec['cvd_z'],2)} ts_z={sf(dec['trade_size_z'],2)} "
                  f"conf={dec['confirm_count']} grid={sf(dec['grid_candidate'])} act={dec['action']} "
                  f"bars={eng.bars} active={len(eng.active)} open={eng.open_orders} dq={dq}")

            # write CSV
            csv_wr.writerow([int(bar["bar_ts"]),
                             datetime.fromtimestamp(bar["bar_ts"]/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                             dq, dec.get("mid_price_5s"), dec.get("cvd"), dec.get("cvd_z"),
                             dec.get("trade_size_buy_5s"), dec.get("trade_size_z"),
                             dec.get("confirm_count"), dec.get("buy_signal_raw"),
                             dec.get("buy_signal_confirmed"), dec.get("grid_candidate"),
                             dec.get("action"), dec.get("reason"), eng.bars, len(eng.active)])
            csv_fh.flush()

            # Execution
            if dec["action"] == "PLACE_BUY" and dec["grid_candidate"] is not None:
                level = float(dec["grid_candidate"])
                mid_now = float(dec.get("mid_price_5s") or 0.0)
                if (mid_now<=0.0) or (abs(level-mid_now)/max(level,1e-9) > GRID_TOL):
                    print(f"[i] skip exec: candidate drifted (mid={mid_now}, level={level})")
                    eng.active.discard(level); continue
                if level in eng.active:
                    print("[i] level already locked"); continue

                eng.cooldown_until = now_ms()+COOLDOWN_MS
                eng.active.add(level)

                row = match_grid_row(grid_df, level, tol=GRID_TOL)
                coin_size = float(row["coin_size"])
                # ยิง MARKET BUY (มี uplift ภายในฟังก์ชัน)
                res = execu.place_market_buy(level, coin_size)
                market_id = res.get("id"); filled_amt = res.get("filled") or coin_size

                if res["id"] or DRY_RUN:
                    eng.open_orders += 1
                    row_tp = float(row["tp_price"])
                    fill_avg = res.get("avg") or res.get("px_ref") or level
                    tp_min = max(row_tp, fill_avg*(1.0 + 2.0*EXEC_FEE_RATE + TP_EXTRA_MARGIN))
                    tp_id = execu.place_tp_after_market(level, market_id, filled_amt, tp_min)
                    if tp_id: execu.tp_ids[level] = tp_id
                else:
                    eng.active.discard(level)

            execu.poll(eng, grid_df)

            elapsed = now_ms() - t0
            time.sleep(max(0.0, 1.0 - elapsed/1000.0))

    except KeyboardInterrupt:
        print("\n[!] KeyboardInterrupt — exit")
    finally:
        try: csv_fh.close()
        except Exception: pass


if __name__ == "__main__":
    main()
