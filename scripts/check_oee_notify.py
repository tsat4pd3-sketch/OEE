#!/usr/bin/env python3
"""
OEE / Output Telegram Notifier
Fetches Google Sheets CSV → calculates 90-day median per product line
→ alerts if latest shift is below median by > THRESHOLD %
"""
import csv, io, os, sys, json, statistics, urllib.request, urllib.parse
from datetime import datetime, timedelta, timezone

# ── Config ──────────────────────────────────────────────────────────────
PD3_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vToehKwHXa32rnaE008gbSZ795A_2tpe4mgEsPECNX9-o5gv7aFfNWBZnxoVdvrqmylqv7bjg26PXHY"
    "/pub?output=csv&gid=2011515171"
)
PD4_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vSs034o0M970g62tL1jkU4CU6HpAbVkER87uw3OzI97ueA6xXdBdwXd1Gcd0GrgaKOuIrTl-F75q3nz"
    "/pub?gid=122174819&single=true&output=csv"
)

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")

LOOKBACK_DAYS = 90   # median window
THRESHOLD_PCT = 5    # alert if below median by more than 5%
BKK_OFFSET    = 7    # UTC+7

PRODUCTS = [
    {"id": "gor", "label": "GOR",    "src": "PD4", "keywords": ["GOR"]},
    {"id": "lwr", "label": "LWRBAR", "src": "PD4", "keywords": ["LWRBAR", "LWR BAR", "LWR_BAR"]},
    {"id": "p60", "label": "APRON 60", "src": "PD3", "keywords": ["060", "RH 060", "RH060"]},
    {"id": "p61", "label": "APRON 61", "src": "PD3", "keywords": ["061", "LH 061", "LH061"]},
]
EXCLUDES = ["LASER", "BENDING", "BEND"]

# ── Helpers ─────────────────────────────────────────────────────────────
def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read().decode("utf-8-sig")

def n(v):
    try:
        return float(str(v).replace(",", "").strip()) if v else 0.0
    except ValueError:
        return 0.0

def parse_thai_date_dmy(s):
    """Parse DD/MM/YYYY (Buddhist Era aware)"""
    if not s:
        return None
    s = s.strip().strip('"').split(" ")[0]
    p = s.split("/")
    if len(p) != 3:
        return None
    try:
        d, m, y = int(p[0]), int(p[1]), int(p[2])
        if y > 2400:
            y -= 543
        if y < 2000 or y > 2100:
            return None
        return datetime(y, m, d)
    except (ValueError, TypeError):
        return None

def match_product(part, prod):
    p = part.upper().strip()
    if any(ex in p for ex in EXCLUDES):
        return False
    return any(k.upper() in p for k in prod["keywords"])

def csv_rows(text):
    return list(csv.reader(io.StringIO(text.replace("\r\n", "\n"))))

def median_pos(values):
    vals = [v for v in values if v > 0]
    if not vals:
        return 0.0
    return statistics.median(vals)

# ── Parsers ──────────────────────────────────────────────────────────────
def parse_pd3(text):
    rows_raw = csv_rows(text)
    if len(rows_raw) < 2:
        return []
    headers = [h.strip() for h in rows_raw[0]]
    idx = {h: i for i, h in enumerate(headers)}

    def g(row, col):
        i = idx.get(col)
        return str(row[i]).strip() if i is not None and i < len(row) else ""

    def gi(row, i):
        return str(row[i]).strip() if i < len(row) else ""

    pd3_prods = [p for p in PRODUCTS if p["src"] == "PD3"]
    rows = []
    for row_raw in rows_raw[1:]:
        if len(row_raw) < 10:
            continue
        line = g(row_raw, "เลือกไลน์การผลิต")
        if not line:
            continue
        prod_id = next(
            (p["id"] for p in pd3_prods if match_product(line, p)), None
        )
        if not prod_id:
            continue
        date = parse_thai_date_dmy(gi(row_raw, 43))
        if not date:
            continue
        shift = g(row_raw, "กะทำงาน") or "_"
        rows.append({
            "prod": prod_id, "date": date,
            "shift": shift,
            "oee":   n(g(row_raw, "OEE")),
            "total": n(g(row_raw, "ยอดผลิตรวมทั้งวัน")),
            "defect":n(g(row_raw, "Defect")),
        })

    # deduplicate date|prod|shift
    seen = {}
    for r in rows:
        k = f"{r['date'].date()}|{r['prod']}|{r['shift']}"
        seen[k] = r
    return list(seen.values())


def parse_pd4(text):
    rows_raw = csv_rows(text)
    if len(rows_raw) < 2:
        return []

    pd4_prods = [p for p in PRODUCTS if p["src"] == "PD4"]

    def g(row, i):
        return str(row[i]).strip() if i < len(row) else ""

    rows = []
    for row_raw in rows_raw[1:]:
        if len(row_raw) < 10:
            continue
        part = g(row_raw, 1)
        if not part:
            continue
        prod_id = next(
            (p["id"] for p in pd4_prods if match_product(part, p)), None
        )
        if not prod_id:
            continue
        date = parse_thai_date_dmy(g(row_raw, 59)) or parse_thai_date_dmy(g(row_raw, 0))
        if not date:
            continue
        shift = g(row_raw, 5) or "_"
        # defect: 3 groups at col 7+i*4 (flag), 9+i*4 (qty)
        defect_total = sum(
            n(g(row_raw, 9 + i * 4))
            for i in range(3)
            if g(row_raw, 7 + i * 4).upper() in ("TRUE", "1")
        )
        rows.append({
            "prod": prod_id, "date": date,
            "shift": shift,
            "oee":   n(g(row_raw, 83)),
            "total": n(g(row_raw, 6)),
            "defect": defect_total,
        })

    seen = {}
    for r in rows:
        k = f"{r['date'].date()}|{r['prod']}|{r['shift']}"
        seen[k] = r
    return list(seen.values())


# ── Telegram ─────────────────────────────────────────────────────────────
def send_telegram(msg):
    if not BOT_TOKEN or not CHAT_ID:
        print("[WARN] TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set — skipping send")
        print(f"[MSG]\n{msg}")
        return False
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = json.dumps({
        "chat_id": CHAT_ID,
        "text": msg,
        "parse_mode": "HTML",
    }).encode("utf-8")
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=15) as r:
        result = json.loads(r.read())
    return result.get("ok", False)


# ── Main ─────────────────────────────────────────────────────────────────
def main():
    now_bkk = datetime.now(timezone.utc) + timedelta(hours=BKK_OFFSET)
    print(f"[{now_bkk.strftime('%Y-%m-%d %H:%M')} BKK] Starting OEE check")

    all_rows = []
    for src, url, parser in [("PD3", PD3_URL, parse_pd3), ("PD4", PD4_URL, parse_pd4)]:
        try:
            text = fetch(url)
            rows = parser(text)
            print(f"  {src}: {len(rows)} rows parsed")
            all_rows.extend(rows)
        except Exception as e:
            print(f"  {src}: ERROR — {e}")

    if not all_rows:
        print("No data — exiting")
        sys.exit(0)

    # Latest date with data
    latest_date = max(r["date"].date() for r in all_rows)
    print(f"Latest data date: {latest_date}")

    # 90-day historical window (exclude latest date → avoid partial day skewing median)
    cutoff = latest_date - timedelta(days=LOOKBACK_DAYS)
    hist   = [r for r in all_rows if cutoff <= r["date"].date() < latest_date]
    latest = [r for r in all_rows if r["date"].date() == latest_date]

    if not latest:
        print(f"No rows for latest date {latest_date} — nothing to compare")
        sys.exit(0)

    # Median per product over last 90 days
    medians = {}
    for p in PRODUCTS:
        pid = p["id"]
        pr = [r for r in hist if r["prod"] == pid]
        medians[pid] = {
            "oee":   median_pos([r["oee"]   for r in pr]),
            "total": median_pos([r["total"] for r in pr]),
            "n":     len(pr),
        }

    # Aggregate latest by product (sum shifts of same day if multiple)
    latest_by_prod = {}
    for r in latest:
        pid = r["prod"]
        if pid not in latest_by_prod:
            latest_by_prod[pid] = {"oee_list": [], "total": 0}
        if r["oee"] > 0:
            latest_by_prod[pid]["oee_list"].append(r["oee"])
        latest_by_prod[pid]["total"] += r["total"]

    # Flatten: avg OEE across shifts for that day
    for pid, v in latest_by_prod.items():
        v["oee"] = statistics.mean(v["oee_list"]) if v["oee_list"] else 0

    # Compare
    alerts = []
    ok_parts = []

    for p in PRODUCTS:
        pid = p["id"]
        if pid not in latest_by_prod:
            continue
        cur = latest_by_prod[pid]
        med = medians[pid]

        issues = []
        if med["oee"] > 0 and cur["oee"] > 0:
            diff = (cur["oee"] - med["oee"]) / med["oee"] * 100
            if diff < -THRESHOLD_PCT:
                issues.append(
                    f'  📊 OEE: <b>{cur["oee"]:.1f}%</b>  '
                    f'(median {med["oee"]:.1f}%)  '
                    f'<b>▼ {abs(diff):.1f}%</b>'
                )

        if med["total"] > 0 and cur["total"] > 0:
            diff = (cur["total"] - med["total"]) / med["total"] * 100
            if diff < -THRESHOLD_PCT:
                issues.append(
                    f'  🏭 Output: <b>{int(cur["total"])} ชิ้น</b>  '
                    f'(median {int(med["total"])})  '
                    f'<b>▼ {abs(diff):.1f}%</b>'
                )

        label = f'{p["label"]} ({p["src"]})'
        if issues:
            alerts.append(f'🔴 <b>{label}</b>\n' + "\n".join(issues))
        else:
            ok_parts.append(p["label"])

    date_str = latest_date.strftime("%d/%m/%Y")
    hour = now_bkk.hour
    shift_label = "กะดึก 🌙" if (0 <= hour < 13) else "กะกลางวัน ☀️"

    if alerts:
        alert_block = "\n\n".join(alerts)
        ok_block = (f'\n\n✅ <b>ปกติ:</b> {", ".join(ok_parts)}') if ok_parts else ""
        msg = (
            f'⚠️ <b>OEE Alert — {date_str}  {shift_label}</b>\n\n'
            f'📉 ต่ำกว่า median 3 เดือน (>5%):\n\n'
            f'{alert_block}{ok_block}\n\n'
            f'<i>🔗 Dashboard: https://tsat4pd3-sketch.github.io/OEE/</i>'
        )
    else:
        summary_lines = []
        for p in PRODUCTS:
            pid = p["id"]
            if pid not in latest_by_prod:
                continue
            cur = latest_by_prod[pid]
            med = medians[pid]
            oee_str = f'{cur["oee"]:.1f}%' if cur["oee"] > 0 else "—"
            med_str = f'{med["oee"]:.1f}%' if med["oee"] > 0 else "—"
            summary_lines.append(f'  • {p["label"]}: {oee_str} (median {med_str})')
        summary = "\n".join(summary_lines)
        msg = (
            f'✅ <b>OEE Check — {date_str}  {shift_label}</b>\n\n'
            f'ทุกไลน์อยู่ในเกณฑ์ปกติ 👍\n\n'
            f'{summary}\n\n'
            f'<i>🔗 https://tsat4pd3-sketch.github.io/OEE/</i>'
        )

    print(msg)
    ok = send_telegram(msg)
    print(f"Telegram send: {'✅ OK' if ok else '❌ FAILED'}")
    sys.exit(0 if ok or not BOT_TOKEN else 1)


if __name__ == "__main__":
    main()
