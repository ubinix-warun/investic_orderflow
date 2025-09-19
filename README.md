# investic Orderflow

บอทเทรดสปอตแบบ “โครงสร้างจากอดีต + สัญญาณจากปัจจุบัน”  
- **โครงสร้าง (Structure):** สร้างแผน `grid_plan.csv` จากผล Monte Carlo (macro_montecarlo.csv) เพื่อกำหนดแนวซื้อ (grid levels) และไซส์ต่อออเดอร์  
- **สัญญาณ (Signal):** วัดความ “ผิดปกติ” ระยะสั้นด้วย MAD-z (median absolute deviation z-score) ของ **CVD** และ **Trade Size** จากบาร์ 5 วินาที  
- **Execution:** เมื่อสัญญาณยืนยันและราคาอยู่ใกล้เลเวล → **BUY ด้วย MARKET 100%** แล้วตั้ง **LIMIT TP** ทันที  
- **Safety:** กันซ้ำด้วย pre-lock เลเวลจากออเดอร์ค้าง, cooldown, max open orders, และบังคับ minNotional/minQty ให้ผ่านกติกา exchange  
- **Logging & Viz:** บันทึกทุกบาร์ลง CSV และมี **Streamlit** สำหรับดูค่า `cvd_z` / `ts_z` แบบเรียลไทม์

---

## สารบัญ
- [สถาปัตยกรรมโดยสรุป](#สถาปัตยกรรมโดยสรุป)
- [การติดตั้ง](#การติดตั้ง)
- [เตรียมข้อมูล & สร้างแผนกริด](#เตรียมข้อมูล--สร้างแผนกริด)
- [รันทดลอง / รันจริง](#รันทดลอง--รันจริง)
- [การแสดงผล (Streamlit)](#การแสดงผล-streamlit)
- [โครงสร้างไฟล์และสคีมา CSV](#โครงสร้างไฟล์และสคีมา-csv)
- [อธิบายสัญญาณและเหตุผลที่ใช้](#อธิบายสัญญาณและเหตุผลที่ใช้)
- [กันยิงซ้ำยังไง](#กันยิงซ้ำยังไง)
- [Troubleshooting](#troubleshooting)
- [วิธีป้อน API ให้เครื่อง (mac/windows)](#วิธีป้อน-api-ให้เครื่อง)

---

## สถาปัตยกรรมโดยสรุป
```
Monte Carlo (macro_montecarlo.csv)
│
├─> Grid Allocator → grid_plan.csv (buy_price, coin_size, tp_price)
│
Live data (ccxt: orderbook + trades) → 5s Aggregator
├─> CVD & TradeSize series → MAD-zscore (window=W)
└─> Candidate level (ใกล้กริดภายใน GRID_TOL; มี fallback ลงล่าง)
│
Logic check: signal_confirmed + cooldown + under_limits + level_free
│
BUY = MARKET (taker 100%) → ตั้ง LIMIT TP ทันที
│
Log → logs/<symbol>_5s_decisions.csv
```


---

## การติดตั้ง

```bash
# 1) clone โปรเจกต์
git clone https://github.com/yourname/investic_orderflow.git
cd investic_orderflow

# 2) สร้างและเปิดใช้งาน virtual env
python3 -m venv .venv
source .venv/bin/activate        # Windows: .\.venv\Scripts\Activate.ps1

# 3) ติดตั้งแพ็กเกจ
pip install -r requirement.txt


เตรียมข้อมูล & สร้างแผนกริด

รัน Monte Carlo ของคุณให้ได้ไฟล์ macro_montecarlo.csv
อย่างน้อยควรมีคอลัมน์:

spot, band_low, band_high (สรุป)

path_id, min_price (รายเส้นทาง)

ใช้สคริปต์ allocator เพื่อสร้าง grid_plan.csv

รันทดลอง / รันจริง

ตั้งค่า API key (ดูวิธีด้านล่างสุด)

เปิดไฟล์บอท (เช่น 03_grid_bot.py ที่เป็น MARKET buy + LIMIT TP) แล้วตั้งค่าในส่วน CONFIG:

SYMBOL (เช่น "XRP/USDT")

DRY_RUN=True เพื่อทดสอบ (ไม่ส่งคำสั่งจริง)

LOG_CSV=None หรือปล่อย default เพื่อบันทึก CSV

สำคัญ: GRID_CSV="grid_plan.csv" ต้องมีไฟล์จริงก่อนรัน

รันบอท: python 03_grid_bot.py

ตัวอย่าง:
[05:19:35] mid=3.10767  cvd_z=-1.24  ts_z=-0.69  conf=0  grid=3.095  act=HOLD  dq=ok  bars=84  active_lv=0

mid ราคาเฉลี่ย bid/ask
cvd_z, ts_z = MAD-zscore ของ CVD และ Trade Size
conf = จำนวนแท่งที่สัญญาณ “ติด” ต่อเนื่อง (ผ่าน threshold)
grid = เลเวลกริดที่เลือก (หรือเลื่อนลงต่ำกว่าถ้าถูกล็อก)
act = PLACE_BUY หรือ HOLD
dq = คุณภาพข้อมูล (ok | partial | stale)
active_lv = จำนวนเลเวลที่ล็อกอยู่แล้ว (กันซ้ำ)

การแสดงผล (Streamlit)
มี visualizer.py สำหรับดูค่า cvd_z / ts_z และตารางบาร์ล่าสุดจากไฟล์ CSV

โครงสร้าง:
.
├── 03_grid_bot.py           # บอท (MARKET buy + LIMIT TP)
├── visualizer.py            # Streamlit dashboard
├── grid_plan.csv            # แผนกริดจาก Monte Carlo/Allocator
├── macro_montecarlo.csv     # ไฟล์ MC ที่ใช้วางแผน
└── logs/
    └── xrpusdt_5s_decisions.csv  # ล็อกการตัดสินใจ/สัญญาณ/สถานะ

อธิบายสัญญาณและเหตุผลที่ใช้

CVD (Cumulative Volume Delta): สะสม (BidVol − AskVol) แบบถ่วงด้วยอิมบาลานซ์จาก orderbook เพื่อดูแรงกดซื้อ/ขายรวม
MAD-zscore: ใช้ มัธยฐาน และ ค่าเบี่ยงเบนสัมบูรณ์จากมัธยฐาน (MAD) → แข็งแรงต่อ outlier มากกว่า z-score ปกติ
z = 0.6745 * (x_now − median) / MAD (ถ้า MAD = 0 ให้กันด้วย epsilon)
TradeSize proxy: ประมาณแรงซื้อฝั่งลึก (เช่น depth bid 5 levels ปรับด้วยอิมบาลานซ์) → คำนวณ MAD-z เช่นเดียวกัน
เข้าเทรดเมื่อ: cvd_z ≥ CVD_Z_TH และ ts_z ≥ TS_Z_TH และ อยู่ใกล้กริด (±GRID_TOL) พร้อมผ่านเงื่อนไขคูลดาวน์/จำนวนดีล

Troubleshooting

Filter failure: NOTIONAL
→ เพิ่มขนาดคำสั่ง (ดู MIN_NOTIONAL_OVERRIDE), ปรับ coin_size ใน grid_plan.csv, หรือเพิ่ม SLIP_PCT เพื่อกันราคากระโดดตอนส่ง MARKET

dq=partial / stale
→ ok: มี orderbook + trades ครบ, partial: มี orderbook แต่แทบไม่มีเทรด, stale: ขาดข้อมูลหลัก ๆ
→ ควรตรวจเน็ตเวิร์ค / rate limit / สภาพคล่องสกุลนั้น ๆ

cvd_z = NaN / ts_z = NaN ช่วงแรก
→ ต้องรอสะสมครบ WINDOW บาร์ก่อน (เช่น WINDOW=50 → ~250 วินาที)

ซ้ำเลเวล
→ ตรวจ LOCK_TOL ให้สมเหตุสมผล (เช่น 0.0015 = 0.15%) และยืนยันว่า prelock_existing() เรียกก่อนลูปรัน
→ ใช้ clientOrderId ที่เข้มงวดและตรวจ active_levels ทุกครั้งก่อนส่งคำสั่ง

วิธีป้อน API ให้เครื่อง
MAC นำไปใส่ในโปรแกรม Terminal

export BINANCE_KEY="ใส่คีย์ของคุณที่นี่"
export BINANCE_SECRET="ใส่ซีเคร็ตของคุณที่นี่"
# วิธีเช็ค:
echo "$BINANCE_KEY"
echo "$BINANCE_SECRET"
# วิธีลบ:
unset BINANCE_KEY
unset BINANCE_SECRET


Windows: นำไปใส่ใน Command Line (PowerShell)
$env:BINANCE_KEY = "ใส่คีย์ของคุณที่นี่"
$env:BINANCE_SECRET = "ใส่ซีเคร็ตของคุณที่นี่"
# วิธีเช็ค:
echo $env:BINANCE_KEY
echo $env:BINANCE_SECRET
# วิธีลบ:
Remove-Item Env:BINANCE_KEY
Remove-Item Env:BINANCE_SECRET
