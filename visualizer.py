# visualizer.py
import streamlit as st
import subprocess, threading, queue, sys, os, time, re
from collections import deque
from datetime import datetime

# =========================
# Settings (ปรับได้จาก UI)
# =========================
DEFAULT_BOT_PATH = "grid_bot.py"
DEFAULT_REFRESH_MS = 3000
DEFAULT_MAX_KEEP = 5000
DEFAULT_LOG_DIR = "logs"
ERRORS_ONLY_LOG = os.path.join(DEFAULT_LOG_DIR, "errors_only.log")

# =========================
# Utilities
# =========================
def ensure_dirs():
    os.makedirs(DEFAULT_LOG_DIR, exist_ok=True)

def tail_write(path: str, lines: list[str]):
    if not lines:
        return
    ensure_dirs()
    with open(path, "a", encoding="utf-8") as f:
        for ln in lines:
            f.write(ln if ln.endswith("\n") else ln + "\n")

def make_proc(cmd):
    # ใช้ interpreter ตัวเดียวกับที่รัน Streamlit
    return subprocess.Popen(
        [sys.executable, "-u", cmd],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=1,
        universal_newlines=True,
        cwd=os.getcwd(),
        env=os.environ.copy()
    )

def reader_thread(stream, q: queue.Queue, tag: str):
    # อ่านบรรทัดต่อบรรทัดแล้วโยนเข้าคิว (ไม่บล็อค UI)
    for line in iter(stream.readline, ""):
        q.put((tag, line))
    stream.close()

def init_session_state():
    ss = st.session_state
    if "bot_proc" not in ss:
        ss.bot_proc = None
    if "stdout_thr" not in ss:
        ss.stdout_thr = None
    if "stderr_thr" not in ss:
        ss.stderr_thr = None
    if "log_q" not in ss:
        ss.log_q = queue.Queue()
    if "errors" not in ss:
        ss.errors = deque(maxlen=DEFAULT_MAX_KEEP)
    if "last_flush_ts" not in ss:
        ss.last_flush_ts = 0.0
    if "attach_bang" not in ss:
        ss.attach_bang = False

def is_running():
    return (st.session_state.bot_proc is not None) and (st.session_state.bot_proc.poll() is None)

def start_bot(bot_path: str):
    if is_running():
        return
    if not os.path.exists(bot_path):
        st.error(f"ไม่พบไฟล์บอท: {bot_path}")
        return
    ensure_dirs()
    proc = make_proc(bot_path)
    st.session_state.bot_proc = proc

    # สร้างเธรดอ่าน stdout/stderr
    out_thr = threading.Thread(target=reader_thread, args=(proc.stdout, st.session_state.log_q, "STDOUT"), daemon=True)
    err_thr = threading.Thread(target=reader_thread, args=(proc.stderr, st.session_state.log_q, "STDERR"), daemon=True)
    out_thr.start(); err_thr.start()
    st.session_state.stdout_thr = out_thr
    st.session_state.stderr_thr = err_thr

def stop_bot():
    proc = st.session_state.bot_proc
    if proc is None:
        return
    try:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
    except Exception:
        pass
    st.session_state.bot_proc = None
    st.session_state.stdout_thr = None
    st.session_state.stderr_thr = None

ERR_RE = re.compile(r"^\[(ERR)\]\s*(.*)")
BANG_RE = re.compile(r"^\[\!\]\s*(.*)")  # optional important warning

def pump_queue(include_bang: bool) -> int:
    """ดึงข้อความจากคิว → เก็บเฉพาะ error (และ [!] ถ้าเลือก) → เขียนลงไฟล์ errors_only.log"""
    collected = []
    pushed = 0
    while True:
        try:
            tag, line = st.session_state.log_q.get_nowait()
        except queue.Empty:
            break
        line_stripped = line.rstrip("\n")
        m_err = ERR_RE.match(line_stripped)
        if m_err:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            pretty = f"[{ts}] [ERR] {m_err.group(2)}"
            st.session_state.errors.append(pretty)
            collected.append(pretty)
            pushed += 1
            continue
        if include_bang:
            m_bang = BANG_RE.match(line_stripped)
            if m_bang:
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                pretty = f"[{ts}] [!] {m_bang.group(1)}"
                st.session_state.errors.append(pretty)
                collected.append(pretty)
                pushed += 1
    if collected:
        tail_write(ERRORS_ONLY_LOG, collected)
    return pushed

def clear_errors():
    st.session_state.errors.clear()
    # ล้างไฟล์ด้วย
    try:
        if os.path.exists(ERRORS_ONLY_LOG):
            os.remove(ERRORS_ONLY_LOG)
    except Exception:
        pass

# =========================
# Streamlit UI
# =========================
st.set_page_config(page_title="Grid Bot Errors", page_icon="🚨", layout="wide")
init_session_state()

st.title("🚨 Grid Bot — Errors Monitor (errors only)")

with st.sidebar:
    st.header("⚙️ Settings")
    bot_path = st.text_input("Bot script path", DEFAULT_BOT_PATH)
    refresh_ms = st.slider("Auto-refresh (ms)", min_value=1000, max_value=10000, value=DEFAULT_REFRESH_MS, step=500)
    include_bang = st.toggle("Include important [!]", value=st.session_state.attach_bang, help="รวมบรรทัดที่ขึ้นต้นด้วย [!]")
    st.session_state.attach_bang = include_bang

    colb1, colb2 = st.columns(2)
    with colb1:
        if st.button("▶️ Start bot", use_container_width=True, disabled=is_running()):
            start_bot(bot_path)
    with colb2:
        if st.button("⏹ Stop bot", use_container_width=True, disabled=not is_running()):
            stop_bot()

    st.divider()
    if st.button("🧹 Clear errors", use_container_width=True):
        clear_errors()
    # ปุ่มดาวน์โหลด errors
    errors_txt = "\n".join(st.session_state.errors)
    st.download_button("📥 Download errors.txt", errors_txt, file_name="errors.txt", mime="text/plain", use_container_width=True)

# ดึงข้อความใหม่จากคิว (เฉพาะ error)
new_cnt = pump_queue(include_bang)

# แสดงสถานะบอท
st.markdown(
    f"**Bot status:** {'🟢 Running' if is_running() else '🔴 Stopped'}  "
    f"| **Errors shown:** {len(st.session_state.errors):,}  "
    f"{'| newly captured: ' + str(new_cnt) if new_cnt else ''}"
)

# พื้นที่แสดง errors — ล่าสุดไว้บน
with st.container(border=True):
    st.subheader("Errors")
    if st.session_state.errors:
        # แสดง N รายการล่าสุด (กลับด้าน)
        for ln in list(st.session_state.errors)[-1000:][::-1]:
            st.code(ln, language=None)
    else:
        st.info("ยังไม่พบบรรทัดที่ขึ้นต้นด้วย [ERR] (หรือ [!] ถ้าเปิดตัวเลือก)")

# Auto-refresh
st.experimental_set_query_params(refresh=str(time.time()))  # กัน cache บางกรณี
st.autorefresh = st.experimental_rerun  # just alias for readability
# ใช้ st_autorefresh เพื่อเรียกสคริปต์ใหม่อัตโนมัติ
st.experimental_memo.clear() if False else None  # placeholder no-op
st.experimental_singleton.clear() if False else None  # placeholder no-op
st.stop() if False else None  # placeholder no-op

# ออโต้รีเฟรชแบบเบา ๆ
st.runtime.legacy_caching.clear_cache() if False else None  # no-op
st.experimental_set_query_params(_=str(int(time.time()*1000)))  # tick param
st.experimental_rerun() if st.session_state.get("_autorefresh_tick") and False else None
# ใช้กลไกของ Streamlit: autorefresh component
st.empty()
st.experimental_data_editor if False else None  # keep linter quiet
# ใช้ built-in autorefresh
st_autorefresh = st.sidebar.empty()
st_autorefresh.html(f"""<script>
setTimeout(function() {{ window.location.reload(); }}, {int(refresh_ms)});
</script>""", height=0)
