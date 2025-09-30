# visualizer.py
import os
import time
import numpy as np
import pandas as pd
import altair as alt
import streamlit as st

# ========== Page setup ==========
# DEFAULT_SYMBOL = "XRP/USDT"
DEFAULT_SYMBOL = "JTO/USDT"
DEFAULT_LOG_DIR = "logs"

st.set_page_config(page_title="CVD_z & TS_z Monitor", layout="wide")
st.title("CVD_z & TS_z — Streamlit Monitor")

# ========== Sidebar ==========
symbol = st.sidebar.text_input("Symbol (Base/Quote)", value=DEFAULT_SYMBOL)
symbol_safe = symbol.replace("/", "").lower()
default_csv = os.path.join(DEFAULT_LOG_DIR, f"{symbol_safe}_5s_decisions.csv")

csv_path = st.sidebar.text_input("CSV path", value=default_csv)
refresh_sec = st.sidebar.slider("Auto-refresh every (sec)", 1, 10, 2)
win_minutes = st.sidebar.slider("Window (minutes)", 5, 240, 60)
cvd_z_th = st.sidebar.number_input("CVD_z threshold line", value=0.0, step=0.1, format="%.2f")
ts_z_th  = st.sidebar.number_input("TS_z threshold line",  value=0.0, step=0.1, format="%.2f")

# ========== Helpers ==========
def _coerce_float(x):
    try:
        if pd.isna(x):
            return np.nan
        if isinstance(x, str) and x.strip().lower() in ("nan", ""):
            return np.nan
        return float(x)
    except Exception:
        return np.nan

def load_csv(path: str, minutes_window: int) -> pd.DataFrame:
    """อ่านไฟล์, ทำเวลาเป็น tz-aware UTC, คัดเฉพาะหน้าต่าง minutes_window นาทีล่าสุด"""
    if not os.path.exists(path):
        return pd.DataFrame()

    try:
        df = pd.read_csv(path)
    except Exception as e:
        st.error(f"อ่านไฟล์ไม่ได้: {e}")
        return pd.DataFrame()

    # เวลา → UTC tz-aware
    if "bar_time_utc" in df.columns:
        df["time"] = pd.to_datetime(df["bar_time_utc"], utc=True, errors="coerce")
    elif "bar_ts_ms" in df.columns:
        df["time"] = pd.to_datetime(df["bar_ts_ms"], unit="ms", utc=True, errors="coerce")
    else:
        # เผื่อชื่ออื่น ๆ
        for cand in ("time", "timestamp", "ts", "ts_ms"):
            if cand in df.columns:
                if pd.api.types.is_numeric_dtype(df[cand]):
                    df["time"] = pd.to_datetime(df[cand], unit="ms", utc=True, errors="coerce")
                else:
                    df["time"] = pd.to_datetime(df[cand], utc=True, errors="coerce")
                break
        else:
            return pd.DataFrame()

    # ให้แน่ใจว่าเป็น tz-aware(UTC)
    if getattr(df["time"].dt, "tz", None) is None:
        df["time"] = df["time"].dt.tz_localize("UTC")
    else:
        df["time"] = df["time"].dt.tz_convert("UTC")

    # คอลัมน์ที่ต้องใช้
    needed = [
        "dq","mid","cvd_z","ts_z","confirm_count",
        "buy_signal_confirmed","grid_candidate","action","reason"
    ]
    for c in needed:
        if c not in df.columns:
            df[c] = np.nan

    # types
    df["cvd_z"] = df["cvd_z"].apply(_coerce_float)
    df["ts_z"]  = df["ts_z"].apply(_coerce_float)
    df["mid"]   = df["mid"].apply(_coerce_float)
    df["confirm_count"] = pd.to_numeric(df["confirm_count"], errors="coerce").fillna(0).astype(int)
    df["buy_signal_confirmed"] = pd.to_numeric(df["buy_signal_confirmed"], errors="coerce").fillna(0).astype(int)
    df["action"] = df["action"].astype(str).fillna("")

    # ตัดหน้าต่างล่าสุด
    now_utc = pd.Timestamp.now(tz="UTC")
    cutoff = now_utc - pd.Timedelta(minutes=minutes_window)
    df = df[(df["time"] >= cutoff) & df["time"].notna()].copy()

    df.sort_values("time", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df

def safe_altair_chart(chart):
    """รองรับทั้งเวอร์ชันใหม่/เก่าของ Streamlit"""
    try:
        # เวอร์ชันใหม่ (แนะนำ)
        st.altair_chart(chart, width="stretch")
    except TypeError:
        # เวอร์ชันเก่า
        st.altair_chart(chart, use_container_width=True)

# ========== Load data ==========
df = load_csv(csv_path, win_minutes)

# ========== Metrics ==========
def show_metrics(df: pd.DataFrame):
    if df.empty:
        st.warning("No data yet.")
        return
    last = df.iloc[-1]
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("CVD_z (last)", f"{last['cvd_z']:.2f}" if np.isfinite(last['cvd_z']) else "nan")
    c2.metric("TS_z (last)",  f"{last['ts_z']:.2f}"  if np.isfinite(last['ts_z'])  else "nan")
    c3.metric("Action", str(last.get("action", "")))
    c4.metric("Grid",   str(last.get("grid_candidate", "")))
    mid_val = last.get("mid", np.nan)
    c5.metric("Mid Price", f"{mid_val:.6f}" if np.isfinite(mid_val) else "nan")

show_metrics(df)

# ========== Charts (เฉพาะ cvd_z & ts_z — ไม่มีกราฟ mid price) ==========
def chart_z(df: pd.DataFrame, z_col: str, th: float, title: str):
    base = alt.Chart(df).encode(
        x=alt.X("time:T", title=None)
    )
    line = base.mark_line().encode(
        y=alt.Y(f"{z_col}:Q", title=title),
        tooltip=[
            "time:T",
            alt.Tooltip(f"{z_col}:Q", format=".2f"),
            "action:N", "dq:N", "confirm_count:Q"
        ],
    )
    th_df = pd.DataFrame({"th":[th]})
    rule = alt.Chart(th_df).mark_rule(strokeDash=[4,3]).encode(y="th:Q")

    # ไฮไลต์จุดที่ action == PLACE_BUY
    buys = base.transform_filter(alt.datum.action == "PLACE_BUY") \
               .mark_point(size=70, filled=True, shape="triangle-up") \
               .encode(y=f"{z_col}:Q")

    return (line + rule + buys).properties(height=280)

if not df.empty:
    col1, col2 = st.columns(2)
    with col1:
        safe_altair_chart(chart_z(df, "cvd_z", cvd_z_th, "CVD z-score"))
    with col2:
        safe_altair_chart(chart_z(df, "ts_z",  ts_z_th,  "Trade Size z-score"))

# ========== Table ==========
if not df.empty:
    table_df = (
        df[["time","dq","mid","cvd_z","ts_z","confirm_count",
            "buy_signal_confirmed","grid_candidate","action","reason"]]
        .rename(columns={"time":"UTC time"})
    )
    try:
        st.dataframe(table_df, width="stretch", height=360)
    except TypeError:
        st.dataframe(table_df, use_container_width=True, height=360)
else:
    st.info(f"ยังไม่พบไฟล์หรือไม่มีข้อมูล: {csv_path}")

# ========== Auto-refresh ==========
time.sleep(refresh_sec)
try:
    st.rerun()
except Exception:
    st.experimental_rerun()
