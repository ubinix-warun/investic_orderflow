# visualizer.py
import os
import time
import numpy as np
import pandas as pd
import altair as alt
import streamlit as st
from glob import glob
from collections import deque

# ========== Page setup ==========
DEFAULT_LOG_DIR = "logs"
MACRO_FILE = "macro_montecarlo.csv"  # ตำแหน่งไฟล์ที่บอทใช้กำหนดสัญลักษณ์

st.set_page_config(page_title="CVD_z & TS_z Monitor", layout="wide")
st.title("CVD_z & TS_z — Streamlit Monitor")

# ========== Helpers ==========
def _normalize_symbol(raw: str) -> str:
    """แปลงสตริงให้เป็นรูป BASE/QUOTE (ดีฟอลต์ USDT)"""
    s = (raw or "").strip().upper().replace("\\", "/").replace("-", "/")
    if not s:
        return "XRP/USDT"
    if "/" in s:
        base, quote = s.split("/", 1)
        base = base.strip()
        quote = (quote or "USDT").strip() or "USDT"
        return f"{base}/{quote}"
    if s.endswith("USDT"):
        base = s[:-4]
        return f"{base}/USDT"
    return f"{s}/USDT"

@st.cache_data(ttl=5)
def detect_symbol_from_macro(path: str = MACRO_FILE) -> str:
    """อ่านคอลัมน์ 'symbol' แถวท้ายจาก macro_montecarlo.csv แล้ว normalize"""
    try:
        df = pd.read_csv(path)
        if "symbol" not in df.columns or df["symbol"].dropna().empty:
            return "XRP/USDT"
        return _normalize_symbol(str(df["symbol"].dropna().astype(str).iloc[-1]))
    except Exception:
        # เผื่อไฟล์ยังไม่ถูกสร้างตอนเปิดแดชบอร์ด
        return "XRP/USDT"

def sf(x):
    try:
        if x is None:
            return np.nan
        if isinstance(x, str) and x.strip().lower() in ("nan", ""):
            return np.nan
        return float(x)
    except Exception:
        return np.nan

def load_csv(path: str, minutes_window: int) -> pd.DataFrame:
    """อ่านไฟล์ตัดสินใจราย 5s แล้วคัดเฉพาะช่วงเวลาที่ต้องการ"""
    if not os.path.exists(path):
        return pd.DataFrame()

    try:
        df = pd.read_csv(path)
    except Exception as e:
        st.error(f"อ่านไฟล์ไม่สำเร็จ: {e}")
        return pd.DataFrame()

    # เวลา → UTC tz-aware
    if "bar_time_utc" in df.columns:
        df["time"] = pd.to_datetime(df["bar_time_utc"], utc=True, errors="coerce")
    elif "bar_ts_ms" in df.columns:
        df["time"] = pd.to_datetime(df["bar_ts_ms"], unit="ms", utc=True, errors="coerce")
    else:
        for cand in ("time", "timestamp", "ts", "ts_ms"):
            if cand in df.columns:
                if pd.api.types.is_integer_dtype(df[cand]):
                    df["time"] = pd.to_datetime(df[cand], unit="ms", utc=True, errors="coerce")
                else:
                    df["time"] = pd.to_datetime(df[cand], utc=True, errors="coerce")
                break

    df = df.dropna(subset=["time"]).sort_values("time")
    if not df.empty:
        cutoff = df["time"].max() - pd.Timedelta(minutes=minutes_window)
        df = df[df["time"] >= cutoff]

    # cast คอลัมน์สำคัญเป็นตัวเลข
    num_cols = [
        "mid","cvd","cvd_z","ts_buy","ts_z","confirm_count",
        "buy_signal_raw","buy_signal_confirmed","within_0p5pct"
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = df[c].apply(sf)
    return df

# ===== อ่านสัญลักษณ์จาก macro_montecarlo.csv (แบบเดียวกับบอท) =====
symbol = detect_symbol_from_macro(MACRO_FILE)
symbol_safe = symbol.replace("/", "").lower()
csv_path = os.path.join(DEFAULT_LOG_DIR, f"{symbol_safe}_5s_decisions.csv")

# ========== Sidebar ==========
st.sidebar.header("⚙️ Settings")
st.sidebar.markdown(f"**Symbol:** `{symbol}` *(อ่านจาก {MACRO_FILE})*")
minutes_window = st.sidebar.slider("Window (minutes)", 5, 240, 60, step=5)
refresh_sec = st.sidebar.slider("Auto-refresh (sec)", 1, 15, 3, step=1)
log_dir = st.sidebar.text_input("Log folder", DEFAULT_LOG_DIR)

# ========== Load data ==========
df = load_csv(csv_path, minutes_window)

# ========== Layout ==========
col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("Mid price (line) & Buy signal (points)")
    if not df.empty and "time" in df and "mid" in df:
        base = alt.Chart(df).encode(x=alt.X("time:T", title="UTC time"))
        line_mid = base.mark_line().encode(y=alt.Y("mid:Q", title="mid"))
        pts_buy = base.mark_circle(size=50, color="orange").encode(
            y="mid:Q",
           opacity=alt.condition(alt.datum.buy_signal_confirmed == 1, alt.value(1.0), alt.value(0.0)),
            tooltip=["time:T","mid:Q","buy_signal_raw:Q","buy_signal_confirmed:Q","grid_candidate:Q","reason:N"]
        )
        st.altair_chart((line_mid + pts_buy).interactive(), use_container_width=True)
    else:
        st.info(f"ยังไม่มีข้อมูล mid/สัญญาณในไฟล์: {csv_path}")

    st.subheader("CVD_z & TS_z")
    if not df.empty and "cvd_z" in df and "ts_z" in df:
        base2 = alt.Chart(df).encode(x=alt.X("time:T", title="UTC time"))
        cvd_line = base2.mark_line().encode(y=alt.Y("cvd_z:Q", title="cvd_z"))
        ts_line  = base2.mark_line(color="red").encode(y=alt.Y("ts_z:Q", title="ts_z"))
        th_line1 = base2.mark_rule(color="#aaa").encode(y=alt.Y(value=1.5))
        th_line2 = base2.mark_rule(color="#aaa").encode(y=alt.Y(value=-1.5))
        st.altair_chart((cvd_line + ts_line + th_line1 + th_line2).interactive(), use_container_width=True)
    else:
        st.info("ยังไม่มีข้อมูล cvd_z/ts_z")

with col2:
    st.subheader("Last N bars (table)")
    if not df.empty:
        show_cols = [
            "time","mid","cvd","cvd_z","ts_buy","ts_z",
            "confirm_count","buy_signal_raw","buy_signal_confirmed",
            "grid_candidate","action","reason"
        ]
        table_df = df[[c for c in show_cols if c in df.columns]].tail(200).copy()
        try:
            st.dataframe(table_df, width="stretch", height=360)
        except TypeError:
            st.dataframe(table_df, use_container_width=True, height=360)
    else:
        st.info(f"ยังไม่พบไฟล์หรือไม่มีข้อมูล: {csv_path}")

# ========== Errors (latest) -- errors-only panel ==========
# อ่านเฉพาะบรรทัดที่ขึ้นต้นด้วย [ERR] จากไฟล์ .log/.txt ในโฟลเดอร์ที่กำหนด
ERROR_SCAN_DIR   = log_dir if log_dir else DEFAULT_LOG_DIR
ERROR_MAX_BYTES  = 300_000    # tail ~300KB/ไฟล์
ERROR_SHOW_MAX   = 300        # แสดงสูงสุด 300 บรรทัด
ERROR_PREFIX     = "[ERR]"

def _find_latest_logs(dirpath: str) -> list[str]:
    try:
        files = []
        for patt in ("*.log", "*.txt"):
            files.extend(glob(os.path.join(dirpath, patt)))
        files = [p for p in files if os.path.isfile(p)]
        files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
        return files[:5]
    except Exception:
        return []

def _tail_text(path: str, max_bytes: int = ERROR_MAX_BYTES) -> str:
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - max_bytes), os.SEEK_SET)
            chunk = f.read()
        return chunk.decode("utf-8", errors="ignore")
    except Exception:
        return ""

def _extract_errors(text: str) -> list[str]:
    return [ln.strip() for ln in text.splitlines() if ln.strip().startswith(ERROR_PREFIX)]

@st.cache_data(ttl=2)
def _collect_errors_panel(log_dir: str) -> list[str]:
    errors = []
    for p in _find_latest_logs(log_dir):
        txt = _tail_text(p, ERROR_MAX_BYTES)
        if txt:
            errors.extend(_extract_errors(txt))
    seen = set(); dq = deque(maxlen=ERROR_SHOW_MAX)
    for ln in errors:
        if ln in seen:
            continue
        seen.add(ln); dq.append(ln)
    return list(dq)[-ERROR_SHOW_MAX:]

st.markdown("### 🚨 Errors (latest)")
try:
    errs = _collect_errors_panel(ERROR_SCAN_DIR)
    if errs:
        for ln in errs[::-1]:
            st.code(ln, language=None)
    else:
        st.info(f"ยังไม่พบบรรทัดที่ขึ้นต้นด้วย [ERR] ในโฟลเดอร์ `{ERROR_SCAN_DIR}/`")
except Exception as _e:
    st.warning(f"แสดง error ไม่สำเร็จ: {_e}")

# ========== Auto-refresh ==========
time.sleep(refresh_sec)
try:
    st.rerun()
except Exception:
    st.experimental_rerun()
