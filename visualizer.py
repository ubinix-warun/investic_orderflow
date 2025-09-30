# visualizer.py — Grid Trading Bot Monitor (Dash, dark theme, GMT+7, 100-point sliding window)
import os, io, glob, warnings
from collections import deque
from typing import Tuple, List

import numpy as np
import pandas as pd
from dash import Dash, html, dcc, Output, Input, State, no_update
import plotly.graph_objects as go

# ================= Config =================
LOG_DIR              = "logs"                 # โฟลเดอร์ log
MACRO_FILE           = "macro_montecarlo.csv" # อ่าน symbol จากไฟล์นี้
WINDOW_MIN           = 60                     # โหลดข้อมูลย้อนหลัง (นาที) เพื่อคัดเฉพาะช่วงใกล้ ๆ
REFRESH_SEC          = 5                      # รีเฟรชทุกกี่วินาที
CSV_TAIL_MAX_BYTES   = 5_000_000              # อ่านท้ายไฟล์ได้สูงสุด
ERROR_MAX_BYTES      = 300_000
ERROR_SHOW_MAX       = 300
ERROR_PREFIX         = "[ERR]"
DISPLAY_POINTS       = 100                    # << แสดงผล “เลื่อนหน้าต่างล่าสุด” แค่ 100 datapoints
QUANT_ALPHA          = 0.35                   # soft of y-range (0..1) ยิ่งต่ำยิ่งนิ่ง
TZ_NAME              = "Asia/Bangkok"         # GMT+7

DEFAULT_SYMBOL = "XRP/USDT" # pair เริ่มต้น ถ้าไม่เจอใน macro_montecarlo.csv
# DEFAULT_SYMBOL = "JTO/USDT"

# ================= Helpers =================
def _normalize_symbol(raw: str) -> str:
    s = (raw or "").strip().upper().replace("\\", "/").replace("-", "/")
    if not s: return DEFAULT_SYMBOL
    if "/" in s:
        base, quote = s.split("/", 1)
        return f"{base.strip()}/{(quote or 'USDT').strip() or 'USDT'}"
    if s.endswith("USDT"):
        return f"{s[:-4]}/USDT"
    return f"{s}/USDT"

def detect_symbol_from_macro(path: str = MACRO_FILE) -> str:
    try:
        df = pd.read_csv(path)
        s = df.get("symbol")
        if s is None or s.dropna().empty:
            return DEFAULT_SYMBOL
        return _normalize_symbol(str(s.dropna().astype(str).iloc[-1]))
    except Exception:
        return DEFAULT_SYMBOL

def _tail_text(path: str, max_bytes: int) -> str:
    with open(path, "rb") as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()
        f.seek(max(0, size - max_bytes), os.SEEK_SET)
        return f.read().decode("utf-8", errors="ignore")

def _read_first_line(path: str) -> str:
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return (f.readline() or "").strip("\r\n")

def _as_float(x):
    try:
        if x is None: return np.nan
        if isinstance(x, str) and x.strip().lower() in ("", "nan", "none"): return np.nan
        return float(x)
    except Exception:
        return np.nan

def load_decisions(csv_path: str, minutes_window: int) -> pd.DataFrame:
    if not os.path.exists(csv_path):
        return pd.DataFrame()
    header = _read_first_line(csv_path)
    tail_txt = _tail_text(csv_path, CSV_TAIL_MAX_BYTES)
    buf = tail_txt.lstrip()
    if header and not buf.startswith(header):
        buf = header + "\n" + tail_txt

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            df = pd.read_csv(io.StringIO(buf), sep=None, engine="python", on_bad_lines="skip")
        except Exception:
            df = pd.read_csv(csv_path, sep=None, engine="python", on_bad_lines="skip")

    # time (UTC → aware)
    if "bar_ts_ms" in df.columns:
        ts_nums = pd.to_numeric(df["bar_ts_ms"], errors="coerce")
        df["time"] = pd.to_datetime(ts_nums, unit="ms", utc=True, errors="coerce")
    elif "bar_time_utc" in df.columns:
        mask = df["bar_time_utc"].astype(str).str.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")
        if mask.all():
            df["time"] = pd.to_datetime(df["bar_time_utc"], format="%Y-%m-%d %H:%M:%S", utc=True, errors="coerce")
        else:
            df["time"] = pd.to_datetime(df["bar_time_utc"], utc=True, errors="coerce")
    else:
        for cand in ("time","timestamp","ts","ts_ms"):
            if cand in df.columns:
                nums = pd.to_numeric(df[cand], errors="coerce")
                df["time"] = pd.to_datetime(nums, unit="ms", utc=True, errors="coerce")
                break

    if "time" not in df.columns:
        return pd.DataFrame()

    df = df.dropna(subset=["time"]).sort_values("time")

    if not df.empty:
        cutoff = df["time"].max() - pd.Timedelta(minutes=minutes_window)
        df = df[df["time"] >= cutoff]

    # numeric cast & derived cols
    for c in ["mid","cvd","cvd_z","ts_buy","ts_z","confirm_count",
              "buy_signal_raw","buy_signal_confirmed","within_0p5pct",
              "grid_candidate","bars_total","active_levels"]:
        if c in df.columns:
            df[c] = df[c].apply(_as_float)

    if "cvd_z" in df.columns:
        df["cvd_z_clip"] = df["cvd_z"].clip(-3,3)
    if "ts_z" in df.columns:
        df["ts_z_clip"] = df["ts_z"].clip(-3,3)
    if "buy_signal_confirmed" in df.columns:
        df["buy_confirmed"] = (df["buy_signal_confirmed"] == 1).astype(int)

    # เวลาแสดงผล = GMT+7
    df["time_gmt7"] = df["time"].dt.tz_convert(TZ_NAME)
    return df

def latest_ts_ns(df: pd.DataFrame) -> int:
    if df is None or df.empty or "time" not in df.columns:
        return 0
    return int(df["time"].max().value)

def smooth_domain(prev: Tuple[float,float] | None, newd: Tuple[float,float]) -> Tuple[float,float]:
    if prev is None: return newd
    return ((1-QUANT_ALPHA)*prev[0] + QUANT_ALPHA*newd[0],
            (1-QUANT_ALPHA)*prev[1] + QUANT_ALPHA*newd[1])

def find_latest_logfiles(log_dir: str) -> List[str]:
    files = []
    for patt in ("*.log","*.txt"):
        files.extend(glob.glob(os.path.join(log_dir, patt)))
    files = [p for p in files if os.path.isfile(p)]
    files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return files[:5]

def collect_errors_panel(log_dir: str) -> List[str]:
    out = []
    for p in find_latest_logfiles(log_dir):
        try:
            with open(p, "rb") as f:
                f.seek(0, os.SEEK_END)
                size = f.tell()
                f.seek(max(0, size-ERROR_MAX_BYTES), os.SEEK_SET)
                txt = f.read().decode("utf-8", errors="ignore")
        except Exception:
            continue
        for ln in txt.splitlines():
            s = ln.strip()
            if s.startswith(ERROR_PREFIX):
                out.append(s)
    seen = set(); dq = deque(maxlen=ERROR_SHOW_MAX)
    for ln in out:
        if ln in seen: continue
        seen.add(ln); dq.append(ln)
    return list(dq)[-ERROR_SHOW_MAX:]

# ================= Bootstrap & paths =================
SYMBOL = detect_symbol_from_macro(MACRO_FILE)
SYMBOL_SAFE = SYMBOL.replace("/","").lower()
DECISIONS_CSV = os.path.join(LOG_DIR, f"{SYMBOL_SAFE}_5s_decisions.csv")

# ================= Dash App =================
# ... ด้านบนเหมือนเดิม ...

app = Dash(__name__)
app.title = "Grid Trading Bot Monitor"

DARK_BG = "#0d1117"
PLOT_BG = "#0d1117"
FG      = "#e6edf3"
GRID    = "#1f2328"
ACCENT  = "#fbbf24"  # marker buy confirmed

# ========= REPLACE app.layout WITH THIS =========
app.layout = html.Div(                           # คอนเทนเนอร์ “เต็มหน้า” พื้นหลังดำ
    style={"backgroundColor": DARK_BG, "minHeight": "100vh", "width": "100vw"},
    children=[
        html.Div(                                # คอนเทนเนอร์เนื้อหา (จำกัดความกว้าง)
            style={"color": FG, "maxWidth": "1320px", "margin": "0 auto", "padding": "16px"},
            children=[
                html.H1("Grid Trading Bot Monitor", style={"marginBottom":"6px"}),
                html.Div(
                    f"Symbol: {SYMBOL}  •  Window: {WINDOW_MIN} min  •  Refresh: {REFRESH_SEC}s  •  Logs: {LOG_DIR}  •  Timezone: GMT+7",
                    style={"opacity":0.85, "fontSize":"0.9rem", "marginBottom":"12px"},
                ),

                dcc.Store(id="y-domain-store", data=None),
                dcc.Store(id="last-ts-store", data=0),
                dcc.Interval(id="timer", interval=int(REFRESH_SEC*1000), n_intervals=0),

                html.H2("Mid price", style={"marginTop":"8px"}),
                dcc.Graph(id="mid-graph", config={"displayModeBar": False}, style={"height":"360px"}),

                html.H2("Cumulative Volume Delta & Trade Size Impact (z)"),
                dcc.Graph(id="z-graph", config={"displayModeBar": False}, style={"height":"320px"}),

                html.Details([
                    html.Summary("Raw data (last 200 rows)"),
                    html.Div(id="tail-data", style={"marginTop":"8px"})
                ], open=False, style={"marginTop":"8px"}),

                html.H2("🚨 Errors (latest)", style={"marginTop":"12px"}),
                html.Pre(
                    id="errors-box",
                    style={
                        "whiteSpace":"pre-wrap","background":"#111","padding":"12px",
                        "borderRadius":"8px","border":f"1px solid {GRID}","color":FG
                    }
                ),
            ],
        )
    ],
)
# ================================================


# ================= Callback =================
@app.callback(
    Output("mid-graph","figure"),
    Output("z-graph","figure"),
    Output("y-domain-store","data"),
    Output("last-ts-store","data"),
    Output("errors-box","children"),
    Output("tail-data","children"),
    Input("timer","n_intervals"),
    State("y-domain-store","data"),
    State("last-ts-store","data"),
    prevent_initial_call=False
)
def update_every(_tick, y_state, last_ts_ns):
    df = load_decisions(DECISIONS_CSV, WINDOW_MIN)
    cur_ts = latest_ts_ns(df)

    # ไม่มีแถวใหม่ → อัปเดตเฉพาะ errors/tail
    if cur_ts <= (last_ts_ns or 0):
        errs = collect_errors_panel(LOG_DIR)
        return (no_update, no_update, y_state, last_ts_ns,
                "\n".join(errs) if errs else f"ยังไม่พบ {ERROR_PREFIX} ใน {LOG_DIR}/",
                no_update)

    # ตัดเหลือแค่หน้าต่างล่าสุด 100 จุด
    df_win = df.tail(DISPLAY_POINTS).copy()

    # -------- Mid chart --------
    if not df_win.empty and "mid" in df_win.columns:
        # ช่วง y-domain จากหน้าต่างแสดงผล
        q1 = float(df_win["mid"].quantile(0.01))
        q99 = float(df_win["mid"].quantile(0.99))
        pad = (q99 - q1) * 0.08 if q99 > q1 else 0.01
        y_new = (q1 - pad, q99 + pad)
        y_domain = smooth_domain(y_state, y_new)

        fig_mid = go.Figure()
        fig_mid.add_trace(go.Scatter(
            x=df_win["time_gmt7"], y=df_win["mid"], mode="lines",
            name="mid", line=dict(width=2, color="#7FDBFF")
        ))
        if "buy_confirmed" in df_win.columns:
            buy_df = df_win[df_win["buy_confirmed"] == 1]
            if not buy_df.empty:
                fig_mid.add_trace(go.Scatter(
                    x=buy_df["time_gmt7"], y=buy_df["mid"],
                    mode="markers", name="buy confirmed",
                    marker=dict(symbol="triangle-up", color=ACCENT, size=10),
                    hovertemplate="GMT+7=%{x}<br>mid=%{y:.6f}<extra>^</extra>"
                ))

        fig_mid.update_layout(
            template="plotly_dark",
            paper_bgcolor=DARK_BG, plot_bgcolor=PLOT_BG, font=dict(color=FG),
            margin=dict(l=40, r=12, t=10, b=36), uirevision="keep",
            xaxis=dict(title="time (GMT+7)", gridcolor=GRID, zeroline=False,
                       tickformat="%H:%M:%S<br>%b %d"),
            yaxis=dict(title="mid", range=list(y_domain), gridcolor=GRID, zeroline=False),
            legend=dict(bgcolor="rgba(0,0,0,0)")
        )
    else:
        y_domain = y_state
        fig_mid = go.Figure().update_layout(
            template="plotly_dark", paper_bgcolor=DARK_BG, plot_bgcolor=PLOT_BG,
            font=dict(color=FG), uirevision="keep"
        )

    # -------- Z chart --------
    if not df_win.empty and "cvd_z_clip" in df_win.columns and "ts_z_clip" in df_win.columns:
        fig_z = go.Figure()
        fig_z.add_trace(go.Scatter(
            x=df_win["time_gmt7"], y=df_win["cvd_z_clip"], mode="lines",
            name="CVD_z", line=dict(width=2, color="#7FDBFF")
        ))
        fig_z.add_trace(go.Scatter(
            x=df_win["time_gmt7"], y=df_win["ts_z_clip"], mode="lines",
            name="TS_z", line=dict(width=1.5, color="tomato")
        ))
        fig_z.update_layout(
            template="plotly_dark",
            paper_bgcolor=DARK_BG, plot_bgcolor=PLOT_BG, font=dict(color=FG),
            margin=dict(l=40, r=12, t=10, b=36), uirevision="keep",
            xaxis=dict(title="time (GMT+7)", gridcolor=GRID, zeroline=False,
                       tickformat="%H:%M:%S<br>%b %d"),
            yaxis=dict(title="z", range=[-3,3], gridcolor=GRID, zeroline=False),
            legend=dict(bgcolor="rgba(0,0,0,0)")
        )
    else:
        fig_z = go.Figure().update_layout(
            template="plotly_dark", paper_bgcolor=DARK_BG, plot_bgcolor=PLOT_BG,
            font=dict(color=FG), uirevision="keep"
        )

    # -------- Errors --------
    errs = collect_errors_panel(LOG_DIR)
    err_text = "\n".join(errs) if errs else f"ยังไม่พบ {ERROR_PREFIX} ใน {LOG_DIR}/"

    # -------- Tail (HTML) — เวลาเป็น GMT+7 --------
    tail_html = ""
    if not df.empty:
        show_cols = [c for c in ["time_gmt7","mid","cvd","cvd_z","ts_buy","ts_z",
                                 "confirm_count","buy_signal_raw","buy_signal_confirmed",
                                 "grid_candidate","action","reason"] if c in df.columns]
        tail = df[show_cols].tail(200).copy()
        tail.rename(columns={"time_gmt7":"time(GMT+7)"}, inplace=True)
        tail["time(GMT+7)"] = pd.to_datetime(tail["time(GMT+7)"]).dt.strftime("%Y-%m-%d %H:%M:%S")
        tail_html = (
            "<style>"
            "table{border-collapse:collapse;width:100%;}"
            "th,td{border:1px solid #222;padding:4px 6px;color:#e6edf3;background:#0f141a;}"
            "th{background:#111827;font-weight:600}"
            "</style>" + tail.to_html(index=False, border=0, classes="table")
        )

    return (fig_mid, fig_z, y_domain, cur_ts, err_text,
            dcc.Markdown(tail_html, dangerously_allow_html=True))

# ================= Run =================
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8501)
