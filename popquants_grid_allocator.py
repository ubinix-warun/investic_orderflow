# popquants_grid_allocator.py
# Quant-only grid allocator with reversible weighting toward FAR zone

from __future__ import annotations
import numpy as np
from dataclasses import dataclass
from typing import List, Dict, Literal, Optional

WeightScheme = Literal["near_heavier", "far_heavier"]
Method = Literal["equal_prob", "equal_step"]

@dataclass
class Order:
    zone: str
    buy_price: float
    usd_alloc: float
    coin_size: float
    tp_price: float
    net_pct_est: float

def _round_qty(q, step):
    if step <= 0: 
        return q
    return np.floor(q / step) * step

def _round_price(p, tick):
    if tick <= 0:
        return p
    return np.round(np.round(p / tick) * tick, 8)

def _zones_from_distance(levels: np.ndarray, spot: float, band_low: float, band_high: float,
                         zone_near: float, zone_mid: float) -> List[str]:
    # distance ratio from spot → band_low (สำหรับฝั่งลง) และ spot → band_high (สำหรับฝั่งขึ้น)
    denom_down = max(spot - band_low, 1e-12)
    denom_up = max(band_high - spot, 1e-12)
    
    zones = []
    for level in levels:
        if level <= spot:
            # ฝั่งลง: ใช้ distance จาก spot → band_low
            d = np.clip((spot - level) / denom_down, 0.0, 1.0)
        else:
            # ฝั่งขึ้น: ใช้ distance จาก spot → band_high
            d = np.clip((level - spot) / denom_up, 0.0, 1.0)
        
        if d <= zone_near:
            zones.append("near")
        elif d <= zone_mid:
            zones.append("mid")
        else:
            zones.append("far")
    return zones

def _levels_equal_prob(spot: float, band_low: float, band_high: float, K: int,
                       mc_mins_samples: np.ndarray) -> np.ndarray:
    """
    สร้างเลเวลทั้งสองฝั่งจำนวน K จุดจาก quantiles ของ 'ราคาต่ำสุดรายเส้นทาง'
    แล้ว clip ให้อยู่ใน [band_low, band_high], เรียงจากล่างขึ้นบน
    """
    mc_mins_samples = np.asarray(mc_mins_samples, dtype=float)
    if mc_mins_samples.size < 100:
        raise ValueError("mc_mins_samples is too small; need >=100 samples.")
    # ใช้ quantiles สม่ำเสมอ แล้วตัดที่ band_low <= x <= band_high
    qs = np.linspace(0.05, 0.95, K)  # คุณปรับได้
    raw = np.quantile(mc_mins_samples, qs)
    raw = np.clip(raw, band_low, band_high)  # เปลี่ยนจาก spot เป็น band_high
    levels = np.unique(np.sort(raw))
    # ถ้าซ้ำ เยอะจนได้น้อยกว่า K ก็เติมแบบ linear ระหว่าง band_low→band_high ให้ครบ
    if levels.size < K:
        lin = np.linspace(band_low, band_high, K)  # เปลี่ยนจาก spot เป็น band_high
        mix = np.unique(np.sort(np.concatenate([levels, lin])))
        # keep K ตัวท้าย (ใกล้ band_high มากขึ้น) เพื่อให้จำนวนครบ
        if mix.size >= K:
            levels = mix[-K:]
        else:
            levels = mix
    return levels

def _levels_equal_step(spot: float, band_low: float, band_high: float, K: int) -> np.ndarray:
    levels = np.linspace(band_low, band_high, K)  # สร้าง grid ทั้งสองฝั่ง
    return levels

def _make_weights(levels: np.ndarray, spot: float, band_low: float, band_high: float,
                  budget_usd: float, w_min: float, w_max: float, alpha: float,
                  scheme: WeightScheme) -> np.ndarray:
    """
    ระยะ d ต่อเลเวล:
      - ถ้าราคาต่ำกว่า spot:  d = (spot - level) / (spot - band_low)
      - ถ้าราคาสูงกว่า spot:  d = (level - spot) / (band_high - spot)
    แล้วแปลงเป็นน้ำหนักตาม scheme:
      near_heavier: r = (1 - d)^alpha   (ใกล้ spot หนัก)
      far_heavier : r = d^alpha         (ไกล spot หนัก)
    """
    levels = np.asarray(levels, dtype=float)
    denom_down = max(spot - band_low, 1e-12)
    denom_up   = max(band_high - spot, 1e-12)

    d = np.empty_like(levels, dtype=float)
    below = levels <= spot
    above = ~below
    d[below] = np.clip((spot - levels[below]) / denom_down, 0.0, 1.0)
    d[above] = np.clip((levels[above] - spot) / denom_up, 0.0, 1.0)

    if scheme == "near_heavier":
        r = np.power(1.0 - d, alpha)
    else:  # "far_heavier"
        r = np.power(d, alpha)

    if np.all(r <= 1e-12):
        r = np.ones_like(r)

    w = r / r.sum() * budget_usd

    def clamp_and_renorm(wv):
        wv = np.clip(wv, w_min, w_max)
        tot = wv.sum()
        if tot > 1e-9:
            wv = wv * (budget_usd / tot)
            wv = np.clip(wv, w_min, w_max)
        return wv

    for _ in range(3):
        w = clamp_and_renorm(w)
    return w


def build_grid(
    *,
    spot: float,
    band_low: float,
    band_high: float,
    budget_usd: float,
    K: int = 12,
    method: Method = "equal_prob",
    mc_mins_samples: Optional[np.ndarray] = None,
    # shape of weighting
    alpha: float = 0.7,
    # per-level dollar bounds
    w_min: Optional[float] = None,
    w_max: float = 50.0,
    # trading params
    fee_rate: float = 0.0004,
    tp_pct: float = 0.01,
    # zone thresholds (fraction of (spot - band_low))
    zone_near: float = 0.05,
    zone_mid: float = 0.15,
    # NEW: weighting scheme
    weight_scheme: WeightScheme = "near_heavier",
    # exchange rounding (ถ้าไม่มีให้เป็น 0)
    price_tick: float = 0.0,
    qty_step: float = 0.0,
) -> Dict:
    """
    คืน dict:
      - orders: List[Order as dict]
      - zone_counts
      - totals
      - spot / band_low / band_high
    """
    if band_low >= spot:
        raise ValueError("band_low must be < spot")

    if method == "equal_prob":
        if mc_mins_samples is None:
            raise ValueError("mc_mins_samples must be provided for equal_prob method.")
        levels = _levels_equal_prob(spot, band_low, band_high, K, mc_mins_samples)
    else:
        levels = _levels_equal_step(spot, band_low, band_high, K)

    zones = _zones_from_distance(levels, spot, band_low, band_high, zone_near, zone_mid)

    # >>> บังคับให้ w_min มาจากตลาดจริงเท่านั้น
    if w_min is None:
        raise ValueError("w_min must be provided by caller (derive from exchange minNotional/minQty).")

    # สร้างน้ำหนัก “กลับด้านได้” ตาม weight_scheme
    weights = _make_weights(
        levels=levels, spot=spot, band_low=band_low, band_high=band_high,
        budget_usd=budget_usd, w_min=w_min, w_max=w_max,
        alpha=alpha, scheme=weight_scheme
    )

    # คำนวณออเดอร์
    orders: List[Order] = []
    total_usd = 0.0
    gross_p = tp_pct
    net_pct_est = gross_p - 2 * fee_rate  # buy+sell fees, ไม่รวม slippage

    for z, lvl, usd in zip(zones, levels, weights):
        buy_p = _round_price(float(lvl), price_tick)
        usd_alloc = float(usd)
        # coin size (approx) ก่อน rounding
        size = usd_alloc / buy_p if buy_p > 0 else 0.0
        size = _round_qty(size, qty_step)

        # คำนวณ TP price: ฝั่งลงใช้ +tp_pct, ฝั่งขึ้นใช้ -tp_pct (ขายต่ำกว่าราคาซื้อ)
        tp_price = _round_price(buy_p * (1.0 + tp_pct), price_tick)


        # ถ้า rounding ทำให้ไม่เหลือ size → ข้ามเลเวลนี้
        if size <= 0.0:
            continue

        total_usd += size * buy_p
        orders.append(Order(
            zone=z,
            buy_price=buy_p,
            usd_alloc=round(size * buy_p, 6),
            coin_size=round(size, 8),
            tp_price=tp_price,
            net_pct_est=net_pct_est
        ))

    zone_counts = {
        "near": sum(1 for o in orders if o.zone == "near"),
        "mid":  sum(1 for o in orders if o.zone == "mid"),
        "far":  sum(1 for o in orders if o.zone == "far"),
        "Total_levels": len(orders)
    }

    out = {
        "spot": round(spot, 6),
        "band_low": round(band_low, 6),
        "band_high": round(band_high, 6),
        "orders": [o.__dict__ for o in orders],
        "zone_counts": zone_counts,
        "totals": {
            "total_usd": round(total_usd, 2),
            "est_net_pct_per_fill": round(net_pct_est, 4),
        }
    }
    return out
