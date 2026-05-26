# Landing Page Skill — Thai Summit OEE Dashboard

**Use this file** when building any new dashboard page in this project.
Replicate every pattern below exactly. Do not improvise.

---

## 1. HTML Boilerplate

```html
<!DOCTYPE html>
<html lang="th" data-theme="dark">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>PAGE TITLE · Thai Summit</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800&family=Sarabun:wght@400;500;600;700&display=swap" rel="stylesheet">
  <style>
    /* ALL CSS INLINE — no external stylesheet */
  </style>
</head>
<body>
  <!-- sidebar-edge (mobile swipe zone) -->
  <div id="sidebar-edge"></div>

  <!-- sidebar toggle button -->
  <button id="sidebar-toggle" onclick="toggleSidebar()" title="Toggle Sidebar">&#9664;</button>

  <!-- mobile overlay -->
  <div id="sidebar-overlay" onclick="toggleSidebar()"></div>

  <!-- SIDEBAR -->
  <aside id="sidebar">
    <!-- logo + badge -->
    <!-- nav links -->
    <!-- filter controls -->
  </aside>

  <!-- MAIN CONTENT -->
  <main id="main">
    <!-- marquee bar (optional) -->
    <!-- page content cards -->
  </main>

  <script>/* ALL JS INLINE */</script>
</body>
</html>
```

---

## 2. CSS Variables (copy verbatim)

```css
:root {
  --bg:#f0f2f5; --bg2:#ffffff; --bg3:#f7f8fa; --card:#ffffff;
  --border:#e2e8f0; --border2:#cbd5e1;
  --accent:#e31937; --accent2:#b91c2e;
  --text:#0f172a; --text2:#475569; --muted:#94a3b8;
  --green:#16a34a; --amber:#d97706; --red:#dc2626;
  --font-body:'Sarabun',sans-serif;
  --font-display:'Inter',sans-serif;
  --sidebar:252px; --radius:4px; --radius-sm:3px;
}
[data-theme="dark"] {
  --bg:#070d1a; --bg2:#0d1525; --bg3:#111b2e; --card:#0d1525;
  --border:#1c2640; --border2:#243050;
  --accent:#e31937; --accent2:#ff2d4a;
  --text:#dce4f0; --text2:#8899bb; --muted:#526080;
  --green:#22c55e; --amber:#f59e0b; --red:#ef4444;
}
*{box-sizing:border-box;margin:0;padding:0}
body{
  font-family:var(--font-body); background:var(--bg); color:var(--text);
  display:flex; min-height:100vh; font-size:14px;
}
```

---

## 3. Sidebar Layout

### CSS
```css
#sidebar {
  width:var(--sidebar); background:var(--bg2); border-right:1px solid var(--border);
  display:flex; flex-direction:column; padding:0; position:fixed; top:0; left:0;
  height:100vh; z-index:200; overflow-y:auto; overflow-x:hidden;
  transition:transform .28s cubic-bezier(.4,0,.2,1);
}
#sidebar-toggle {
  position:fixed; top:16px; left:calc(var(--sidebar) - 1px); z-index:201;
  background:var(--bg2); border:1px solid var(--border); border-left:none;
  color:var(--muted); cursor:pointer; padding:6px 5px; border-radius:0 4px 4px 0;
  font-size:10px; line-height:1; transition:all .2s;
}
body.sidebar-collapsed #sidebar-toggle { left:0px }
body.sidebar-collapsed #sidebar { transform:translateX(calc(-1 * var(--sidebar))); position:fixed }
#main { margin-left:var(--sidebar); flex:1; padding:24px; transition:margin-left .28s }
body.sidebar-collapsed #main { margin-left:0 }

/* Mobile */
@media(max-width:768px){
  #sidebar { transform:translateX(calc(-1 * var(--sidebar))); position:fixed }
  body.sidebar-open #sidebar { transform:translateX(0) }
  body.sidebar-open #sidebar-overlay { display:block }
  body.sidebar-open #sidebar-toggle { left:calc(var(--sidebar) - 1px) }
  body:not(.sidebar-open) #sidebar-toggle { left:0px }
  #main { margin-left:0 !important; padding:16px }
  #sidebar-overlay {
    display:none; position:fixed; inset:0; background:#00000066; z-index:199;
  }
}
```

### HTML — Sidebar interior
```html
<aside id="sidebar">
  <!-- Logo block -->
  <div style="padding:16px 18px 10px;border-bottom:1px solid var(--border)">
    <div style="display:flex;align-items:center;gap:8px;margin-bottom:4px">
      <img src="TS_logo.png" style="height:28px">
      <div style="font-size:9px;font-weight:700;letter-spacing:1.5px;color:var(--muted);line-height:1.3">
        MAINTENANCE<br>PAGE SUBTITLE
      </div>
    </div>
    <div style="font-size:16px;font-weight:800;color:var(--text);font-family:var(--font-display)">
      PAGE NAME
    </div>
    <!-- live badge -->
    <span id="mo-badge" class="badge bx" style="margin-top:6px">...</span>
  </div>

  <!-- Nav links -->
  <div class="nav-links" style="padding:10px 10px 0">
    <a class="nav-btn" href="index.html"><span class="ni">🏠</span>Home</a>
    <a class="nav-btn" href="overview.html"><span class="ni">📊</span>Combined Overview</a>
    <a class="nav-btn" href="summary.html"><span class="ni">📈</span>Summary Dashboard</a>
    <a class="nav-btn" href="oee_pd3.html"><span class="ni">⚙️</span>PD3 Overview</a>
    <a class="nav-btn" href="oee_pd4.html"><span class="ni">⚙️</span>PD4 Overview</a>
    <a class="nav-btn" href="oee_deep_pd3.html"><span class="ni">🔬</span>Deep Analysis PD3</a>
    <a class="nav-btn" href="oee_deep_pd4.html"><span class="ni">🔬</span>Deep Analysis PD4</a>
    <a class="nav-btn active" href="THIS_PAGE.html"><span class="ni">🔧</span>THIS PAGE</a>
    <a class="nav-btn" href="line_status.html"><span class="ni">🟢</span>Line Status</a>
    <a class="nav-btn" href="part_images.html"><span class="ni">🖼️</span>Part Images</a>
    <a class="nav-btn" href="masterplan.html"><span class="ni">📋</span>Masterplan</a>
  </div>

  <!-- Date range filter -->
  <div style="padding:14px 14px 0;border-top:1px solid var(--border);margin-top:10px">
    <div style="font-size:10px;font-weight:700;color:var(--muted);letter-spacing:.8px;margin-bottom:8px">ช่วงวันที่</div>
    <div style="display:flex;flex-direction:column;gap:6px">
      <input type="date" id="date-from" onchange="renderPage()" style="...date-input-style...">
      <input type="date" id="date-to"   onchange="renderPage()" style="...date-input-style...">
    </div>
  </div>

  <!-- Toggle filters -->
  <div style="padding:14px;border-top:1px solid var(--border);margin-top:10px">
    <div style="font-size:10px;font-weight:700;color:var(--muted);letter-spacing:.8px;margin-bottom:8px">ตัวกรองข้อมูล</div>
    <!-- Toggle row template -->
    <label class="tog-row">
      <div>
        <span class="tog-lbl">LABEL</span>
        <span class="tog-sub">SUBLABEL</span>
      </div>
      <input type="checkbox" id="tog-ID" onchange="renderPage()">
    </label>
  </div>

  <!-- Dropdown filter -->
  <div style="padding:0 14px 14px">
    <div style="font-size:10px;font-weight:700;color:var(--muted);letter-spacing:.8px;margin-bottom:6px">DROPDOWN LABEL</div>
    <select id="sel-ID" onchange="renderPage()" style="width:100%;padding:7px 10px;background:var(--bg3);border:1px solid var(--border);border-radius:var(--radius-sm);color:var(--text);font-family:var(--font-body);font-size:13px;cursor:pointer;outline:none">
      <option value="">ทั้งหมด</option>
    </select>
  </div>

  <!-- Reload button -->
  <div style="padding:14px;border-top:1px solid var(--border);margin-top:auto">
    <button onclick="loadData()" style="width:100%;padding:9px;background:var(--accent);color:#fff;border:none;border-radius:var(--radius-sm);font-weight:700;cursor:pointer;font-size:13px">
      🔄 โหลดข้อมูลใหม่
    </button>
  </div>
</aside>
```

### CSS — Nav, Toggle, Badge
```css
.nav-links { display:flex; flex-direction:column; gap:2px }
.nav-btn {
  display:flex; align-items:center; gap:9px; padding:9px 10px;
  border-radius:var(--radius-sm); color:var(--text2); text-decoration:none;
  font-size:13px; font-weight:500; transition:all .15s;
}
.nav-btn:hover { background:var(--bg3); color:var(--text) }
.nav-btn.active { background:var(--accent); color:#fff; font-weight:700 }
.ni { font-size:15px; width:20px; text-align:center }

.tog-row {
  display:flex; align-items:center; justify-content:space-between;
  padding:8px 0; border-bottom:1px solid var(--border); cursor:pointer;
}
.tog-row:last-child { border-bottom:none }
.tog-lbl { font-size:12px; font-weight:600; color:var(--text) }
.tog-sub { font-size:10px; color:var(--muted); display:block; margin-top:1px }
input[type=checkbox] { width:32px; height:18px; cursor:pointer; accent-color:var(--accent) }

.badge {
  display:inline-flex; align-items:center; gap:5px;
  padding:3px 8px; border-radius:20px; font-size:10px; font-weight:700;
  letter-spacing:.4px; font-family:var(--font-display);
}
.bx  { background:#1a2a1a; color:#22c55e; border:1px solid #22c55e44 }
.bam { background:#2a1a00; color:#f59e0b; border:1px solid #f59e0b44 }
.err { background:#2a0a0a; color:#ef4444; border:1px solid #ef444444 }
```

---

## 4. Sidebar JS (toggleSidebar)

```js
function toggleSidebar() {
  const isMobile = window.innerWidth < 769;
  if (isMobile) {
    document.body.classList.toggle('sidebar-open');
  } else {
    document.body.classList.toggle('sidebar-collapsed');
    const btn = document.getElementById('sidebar-toggle');
    btn.innerHTML = document.body.classList.contains('sidebar-collapsed') ? '&#9654;' : '&#9664;';
  }
}
window.addEventListener('resize', () => {
  if (window.innerWidth >= 769) {
    document.body.classList.remove('sidebar-open');
  }
});
```

---

## 5. KPI Cards (6-card row)

### CSS
```css
.kpi-row { display:grid; grid-template-columns:repeat(3,1fr); gap:14px; margin-bottom:24px }
@media(max-width:600px){ .kpi-row { grid-template-columns:repeat(2,1fr) } }
.kpi {
  background:var(--card); border:1px solid var(--border);
  border-radius:var(--radius); padding:16px 18px 14px; position:relative;
  overflow:hidden; transition:box-shadow .2s;
}
.kpi::after {
  content:''; position:absolute; bottom:0; left:0; right:0; height:3px;
}
.k1::after{background:var(--accent)}
.k2::after{background:#22c55e}
.k3::after{background:#f59e0b}
.k4::after{background:#8b5cf6}
.k5::after{background:#06b6d4}
.k6::after{background:#3b82f6}
.kpi-lbl { font-size:10px; font-weight:700; color:var(--muted); letter-spacing:.8px; text-transform:uppercase; margin-bottom:6px }
.kpi-val { font-size:28px; font-weight:800; color:var(--text); font-family:var(--font-display); line-height:1 }
.kpi-sub { font-size:11px; color:var(--muted); margin-top:5px }
```

### HTML
```html
<div class="kpi-row">
  <div class="kpi k1">
    <div class="kpi-lbl">LABEL 1</div>
    <div class="kpi-val" id="kv-1">—</div>
    <div class="kpi-sub" id="ks-1">subtext</div>
  </div>
  <!-- repeat k2..k6 -->
</div>
```

---

## 6. Marquee Bar (live scrolling stats)

```html
<div id="marquee-bar" style="display:none;border-top:1px solid var(--border);border-bottom:1px solid var(--border);padding:10px 0;overflow:hidden;background:var(--bg2);margin-bottom:22px">
  <div style="display:flex;gap:0;width:max-content;animation:marquee 28s linear infinite" id="mq-track"></div>
</div>
```
```css
@keyframes marquee{0%{transform:translateX(0)}100%{transform:translateX(-50%)}}
.mq-item { display:flex; align-items:center; gap:8px; padding:0 22px; font-size:12px; white-space:nowrap }
.mq-dot  { width:6px; height:6px; border-radius:50%; flex-shrink:0 }
.mq-val  { font-weight:700; font-family:var(--font-display); color:var(--text) }
```
```js
function updateMarquee(items) {
  // items = [{ label, val }, ...]
  const track = document.getElementById('mq-track');
  const bar   = document.getElementById('marquee-bar');
  if (!items.length) return;
  const html = [...items, ...items].map(it =>
    `<div class="mq-item"><div class="mq-dot" style="background:var(--accent)"></div><span>${it.label}</span><span class="mq-val">${it.val}</span></div>`
  ).join('');
  track.innerHTML = html;
  bar.style.display = 'block';
}
```

---

## 7. Card Container

```css
.card {
  background:var(--card); border:1px solid var(--border);
  border-radius:var(--radius); padding:20px; margin-bottom:20px;
}
.card-hdr {
  display:flex; align-items:center; justify-content:space-between;
  margin-bottom:16px; padding-bottom:12px; border-bottom:1px solid var(--border);
}
.card-hdr h3 { font-size:14px; font-weight:700; color:var(--text); font-family:var(--font-display) }
```

---

## 8. Data Table

```css
.tbl-wrap { overflow-x:auto; max-height:360px; overflow-y:auto }
.dt-tbl   { width:100%; border-collapse:collapse; font-size:13px }
.dt-tbl th {
  background:var(--bg3); color:var(--muted); font-weight:700;
  padding:10px 13px; text-align:left; font-size:12px;
  position:sticky; top:0; letter-spacing:.3px; border-bottom:2px solid var(--border2);
}
.dt-tbl th.num { text-align:right }   /* IMPORTANT: override specificity */
.dt-tbl td     { padding:10px 13px; border-bottom:1px solid var(--border); color:var(--text2) }
.dt-tbl tr:hover td { background:#f8faff }
[data-theme="dark"] .dt-tbl th        { background:var(--bg2) }
[data-theme="dark"] .dt-tbl tr:hover td { background:#ffffff04 }
.num { text-align:right; font-variant-numeric:tabular-nums }
```

---

## 9. GSheet CSV Pattern

```js
const DATA_URL = "https://docs.google.com/spreadsheets/d/e/PUBLISHED_ID/pub?gid=SHEET_GID&single=true&output=csv";

// CSV parser (handles quoted commas)
function csvLineParse(line) {
  const result = []; let cur = ''; let inQ = false;
  for (let i = 0; i < line.length; i++) {
    if (line[i] === '"') { inQ = !inQ; }
    else if (line[i] === ',' && !inQ) { result.push(cur.trim()); cur = ''; }
    else cur += line[i];
  }
  result.push(cur.trim()); return result;
}

async function loadData() {
  try {
    const res  = await fetch(DATA_URL + '&t=' + Date.now(), { cache: 'no-store' });
    const text = await res.text();
    const lines = text.split('\n').filter(l => l.trim());
    const hdr = csvLineParse(lines[0]);
    const idx = {}; hdr.forEach((h, i) => idx[h.trim().replace(/\r/g, '')] = i);
    const g = (row, name) => { const i = idx[name]; return (i !== undefined && row[i] !== undefined) ? String(row[i]).trim().replace(/\r/g, '') : ''; };
    const findCol = sub => Object.keys(idx).find(k => k.includes(sub)) || null;

    // Resolve column keys
    const dateColKey = findCol('วันเวลา') || findCol('Date');
    // ... resolve other keys

    const rows = [];
    for (let r = 1; r < lines.length; r++) {
      const row = csvLineParse(lines[r]);
      if (row.length < 4) continue;
      // parse & push rows
    }
    return rows;
  } catch (e) {
    console.error('loadData failed', e);
    return [];
  }
}
```

### Thai Buddhist Era date parser
```js
function parseThaiDate(str) {
  if (!str) return null;
  const s = str.trim().replace(/\//g, '-');
  const m = s.match(/(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})/);
  if (!m) { const d = new Date(s); return isNaN(d) ? null : d; }
  let [, dd, mo, yy] = m;
  let y = parseInt(yy);
  if (y < 100) y += 2000;
  if (y > 2400) y -= 543; // Buddhist Era → CE
  const d = new Date(y, +mo - 1, +dd);
  return isNaN(d.getTime()) ? null : d;
}
```

---

## 10. Column Mapping Convention

| Field | findCol() search | Fallback |
|---|---|---|
| ประเภทงานซ่อม | `"Problem Type"` or `"2.ประเภทงานซ่อม"` | — |
| ลักษณะปัญหา | `"Problem Characteristic"` or `"1.ลักษณะปัญหา"` | — |
| วันเปิด MO | `"วันเวลาที่แจ้ง"` or `"เวลาเปิด MO"` | — |
| วันปิด MO | `"วันเวลาที่ซ่อมเสร็จ"` or `"เวลาซ่อมเสร็จ"` | — |

- Skip rows where `Problem Type` includes `"reject"` (case-insensitive)
- `isBreakdown = probType.toLowerCase().includes('breakdown')`
- Unclosed MO downtime cap: `ttr * 60000` ms or `8 * 60 * 60 * 1000` ms (whichever smaller)

---

## 11. GSheet URL

```
https://docs.google.com/spreadsheets/d/e/2PACX-1vSVDNHyOb2LrXosMZG09quorOkfI9aLs8Hg_6ek8glxEQRQYL6F1rWl_cZ12A_f7SC2yDkyWroXHtM1/pub?gid=785568377&single=true&output=csv
```

---

## 12. Rules

1. **ทุก CSS และ JS อยู่ inline ในไฟล์เดียว** — ไม่มี external file
2. **ไม่ใช้ framework** — vanilla JS เท่านั้น
3. **Dark mode default** — `<html data-theme="dark">`
4. **Thai font** — Sarabun สำหรับ body, Inter สำหรับ numbers/display
5. **Sidebar collapsible** — desktop: toggle collapsed, mobile: slide-in overlay
6. **Date input** — always ISO format `YYYY-MM-DD`; default range = last 30 days
7. **findCol()** — partial match column names เสมอ (ชีทอาจมี prefix ตัวเลข)
8. **Skip MO** — Reject MO only; import all other types
9. **KPI** — Breakdown Maintenance only (`isBreakdown`); fallback ใช้ทุก MO ถ้า isBreakdown=0
10. **Problem Char grouping** — ใช้ `classifyProbChar(probChar, symptom, knownChars)` function จาก jig_mtn.html
