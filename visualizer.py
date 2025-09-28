# visualizer.py
import os
from glob import glob
from collections import deque
import streamlit as st
from streamlit.components.v1 import html

# --------------------
# Config (ปรับค่าตามต้องการ)
# --------------------
DEFAULT_LOG_DIR   = "logs"     # โฟลเดอร์ที่มีไฟล์ .log/.txt
ERROR_MAX_BYTES   = 300_000    # อ่านเฉพาะท้ายไฟล์ ~300KB/ไฟล์
ERROR_SHOW_MAX    = 300        # แสดง error ล่าสุดไม่เกิน N บรรทัด
DEFAULT_REFRESHMS = 3000       # ms

ERROR_PREFIX = "[ERR]"         # เอาเฉพาะ error ตามที่ขอ

st.set_page_config(page_title="Grid Dashboard + Errors", page_icon="🚨", layout="wide")

# --------------------
# Helpers
# --------------------
def _find_latest_logs(dirpath: str) -> list[str]:
    try:
        files = []
        for patt in ("*.log", "*.txt"):
            files.extend(glob(os.path.join(dirpath, patt)))
        files = [p for p in files if os.path.isfile(p)]
        files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
        return files[:5]  # พอให้ครอบคลุมหลายไฟล์ล่าสุด
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
    out = []
    for ln in text.splitlines():
        s = ln.strip()
        if s.startswith(ERROR_PREFIX):
            out.append(s)
    return out

def _collect_errors(log_dir: str) -> list[str]:
    errors: list[str] = []
    for p in _find_latest_logs(log_dir):
        txt = _tail_text(p, ERROR_MAX_BYTES)
        if not txt:
            continue
        errors.extend(_extract_errors(txt))
    # dedupe แบบรักษาลำดับ
    seen = set()
    dq = deque(maxlen=ERROR_SHOW_MAX)
    for ln in errors:
        if ln in seen:
            continue
        seen.add(ln)
        dq.append(ln)
    return list(dq)[-ERROR_SHOW_MAX:]

# Cache บาง ๆ ลด IO (TTL 2s)
@st.cache_data(ttl=2)
def get_errors_cached(log_dir: str) -> list[str]:
    return _collect_errors(log_dir)

# --------------------
# Sidebar controls (เบาที่สุด)
# --------------------
with st.sidebar:
    st.header("⚙️ Settings")
    log_dir = st.text_input("Log folder", DEFAULT_LOG_DIR)
    refresh_ms = st.slider("Auto-refresh (ms)", 1000, 10000, DEFAULT_REFRESHMS, 500)

# --------------------
# (ที่นี่คือพื้นที่กราฟ/แดชบอร์ดเดิมของคุณ)
# วางโค้ดกราฟเดิมต่อไปตามปกติ...
# --------------------

# --------------------
# Errors panel
# --------------------
st.markdown("### 🚨 Errors (latest)")
try:
    errs = get_errors_cached(log_dir)
    if errs:
        # แสดงล่าสุดไว้บน
        for ln in errs[::-1]:
            st.code(ln, language=None)
    else:
        st.info(f"ยังไม่พบบรรทัดที่ขึ้นต้นด้วย [ERR] ในโฟลเดอร์ `{log_dir}/`")
except Exception as e:
    st.warning(f"แสดง error ไม่สำเร็จ: {e}")

# --------------------
# Auto refresh (เสถียร/เรียบง่าย)
# --------------------
html(f"""
<script>
setTimeout(function() {{
    window.location.reload();
}}, {int(refresh_ms)});
</script>
""", height=0)
