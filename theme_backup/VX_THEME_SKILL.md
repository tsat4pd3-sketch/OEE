# Thai Summit VX — Design System Skill

> Based on VX Presentation Standard Template (R1)  
> Brand: Thai Summit Group · "Zero defect is possible"

---

## 1. Color System

### CSS Variables (copy into `:root` and `[data-theme="dark"]`)

```css
:root {
  /* ── Light Mode — VX White/Green ── */
  --bg:       #f5f7f5;
  --bg2:      #ffffff;
  --bg3:      #eef3ee;
  --card:     #ffffff;
  --border:   #d0ddd0;
  --border2:  #b8ccb8;

  /* Primary accent: Dark Forest Green */
  --accent:       #0d3d14;
  --accent-dim:   rgba(13, 61, 20, 0.07);

  /* Secondary accent: Brand Orange */
  --accent2:      #e87c1e;
  --accent2-dim:  rgba(232, 124, 30, 0.08);

  /* Status colors */
  --green:   #1a7a1a;
  --amber:   #cc7700;
  --red:     #c0392b;
  --purple:  #5b4db8;
  --cyan:    #1a6d2e;

  /* Text */
  --text:    #0a1f0c;
  --text2:   #2d4a30;
  --muted:   #6a8a6d;
  --muted2:  #9ab09c;

  /* Data source colors */
  --pd3:        #1a6d2e;
  --pd3-bg:     rgba(13, 61, 20, 0.06);
  --pd3-border: rgba(13, 61, 20, 0.18);
  --pd4:        #e87c1e;
  --pd4-bg:     rgba(232, 124, 30, 0.07);
  --pd4-border: rgba(232, 124, 30, 0.22);

  /* Layout */
  --sidebar:    252px;
  --radius:     4px;
  --radius-sm:  3px;

  /* Typography */
  --font-body:    'Sarabun', 'Tahoma', sans-serif;
  --font-display: 'Tahoma', 'Sarabun', sans-serif;
}

[data-theme="dark"] {
  /* ── Dark Mode — VX Dark Forest Green ── */
  --bg:       #060f07;
  --bg2:      #091209;
  --bg3:      #0f1a10;
  --card:     #0c1a0d;
  --border:   #1a2e1c;
  --border2:  #243626;

  /* Primary accent: Bright Green */
  --accent:     #3dd65c;
  --accent-dim: rgba(61, 214, 92, 0.09);

  /* Secondary: Warm Orange */
  --accent2:      #f59a3f;
  --accent2-dim:  rgba(245, 154, 63, 0.09);

  /* Status */
  --green:  #3dd65c;
  --amber:  #f59a3f;
  --red:    #e05c4a;

  /* Text */
  --text:    #e8f5e9;
  --text2:   #a5c9a8;
  --muted:   #4a6e4d;
  --muted2:  #2d4430;

  /* Data source */
  --pd3:        #4dcc6a;
  --pd3-bg:     rgba(77, 204, 106, 0.08);
  --pd3-border: rgba(77, 204, 106, 0.22);
  --pd4:        #f59a3f;
  --pd4-bg:     rgba(245, 154, 63, 0.08);
  --pd4-border: rgba(245, 154, 63, 0.22);
}
```

---

## 2. Typography

| Role | Font | Weight | Notes |
|---|---|---|---|
| Display / Heading | **Tahoma** | 700 Bold | ตาม VX template standard |
| Body / Thai text | **Sarabun** | 400 / 500 | Google Fonts |
| Label / Badge | Tahoma | 700 Bold | letter-spacing: 1–3px |
| Data value | Tahoma | 700 | tabular figures |

```html
<!-- Google Fonts — Sarabun only (Tahoma is system font) -->
<link href="https://fonts.googleapis.com/css2?family=Sarabun:wght@300;400;500;600;700&display=swap" rel="stylesheet">
```

### Type Scale
```css
/* Page title */     font-size: 26–31px; font-weight: 700; letter-spacing: -0.3px;
/* Section header */ font-size: 12px;    font-weight: 700; letter-spacing: 2.5px; text-transform: uppercase;
/* Card title */     font-size: 13px;    font-weight: 700; letter-spacing: 1.5px; text-transform: uppercase;
/* Body */           font-size: 14–15px; font-weight: 400;
/* Label/Badge */    font-size: 10–11px; font-weight: 700; letter-spacing: 1–2px;
/* KPI value */      font-size: 36–40px; font-weight: 700; letter-spacing: -1px;
/* Brand tag */      font-size: 8–9px;   font-weight: 700; letter-spacing: 2–3px; text-transform: uppercase;
```

---

## 3. Brand Logo Mark (SVG)

ใช้ได้ทั้ง inline SVG — รูปสามเหลี่ยม Thai Summit:

```html
<!-- Small (sidebar / header) — 28×28px container -->
<div style="width:28px;height:28px;background:var(--accent);border-radius:3px;
            display:flex;align-items:center;justify-content:center;flex-shrink:0">
  <svg width="14" height="13" viewBox="0 0 14 13" fill="none">
    <path d="M7 1L13 12H1L7 1Z" fill="rgba(255,255,255,0.9)"/>
    <path d="M7 5L10 12H4L7 5Z" fill="var(--accent)"/>
  </svg>
</div>

<!-- Medium (topbar / card header) — 34×34px container -->
<div style="width:34px;height:34px;background:var(--accent);border-radius:4px;
            display:flex;align-items:center;justify-content:center;flex-shrink:0">
  <svg width="20" height="18" viewBox="0 0 20 18" fill="none">
    <path d="M10 1L19 17H1L10 1Z" fill="rgba(255,255,255,0.95)"/>
    <path d="M10 7L15 17H5L10 7Z" fill="var(--accent)"/>
  </svg>
</div>
```

### Brand Header Block (sidebar top)
```html
<div style="display:flex;align-items:center;gap:8px;margin-bottom:10px">
  <!-- logo mark here -->
  <div>
    <div style="font-size:8px;letter-spacing:2px;color:var(--accent);
                text-transform:uppercase;font-weight:700;font-family:var(--font-display)">
      Thai Summit
    </div>
    <div style="font-size:7px;letter-spacing:1.5px;color:var(--muted);
                text-transform:uppercase;font-family:var(--font-display)">
      VX System
    </div>
  </div>
</div>
```

---

## 4. Chart Color Palette

### Line / Bar Charts
```js
// PD3 / Dataset A → Green
const COLOR_A = '#4dcc6a';
const COLOR_A_BG = 'rgba(77, 204, 106, 0.08)';

// PD4 / Dataset B → Orange
const COLOR_B = '#f59a3f';
const COLOR_B_BG = 'rgba(245, 154, 63, 0.08)';

// Target line → Orange (ตาม VX bar chart standard)
const COLOR_TARGET = 'rgba(232, 124, 30, 0.6)';

// Multi-series palette (ตาม VX: light→dark green + orange)
const PALETTE = [
  '#3dd65c',  // bright green
  '#e87c1e',  // brand orange
  '#2eb84b',  // mid green
  '#f59a3f',  // warm orange
  '#1a6d2e',  // dark green
  '#a3d977',  // light green
  '#66c77a',  // soft green
  '#ff6b35',  // deep orange
];
```

### Production Line Colors
```js
const LINE_COLORS = {
  'GOR':    '#e87c1e',  // Orange
  'LWRBAR': '#3dd65c',  // Green
  '060':    '#2eb84b',  // Mid green
  '061':    '#a3d977',  // Light green
};
```

---

## 5. Component Snippets

### Brand Topbar
```html
<div style="display:flex;align-items:flex-start;justify-content:space-between;
            padding-bottom:18px;border-bottom:2px solid var(--accent);margin-bottom:20px">
  <div style="flex:1">
    <div style="font-size:9px;letter-spacing:3px;color:var(--accent);
                text-transform:uppercase;font-family:var(--font-display);
                font-weight:700;margin-bottom:3px">
      Thai Summit Group · VX Production System
    </div>
    <h2 style="font-family:var(--font-display);font-size:26px;font-weight:700;
               color:var(--text);letter-spacing:-.3px">
      Page Title
    </h2>
  </div>
</div>
```

### Brand Footer
```html
<div style="margin-top:40px;padding:20px 0;border-top:1px solid var(--border);
            display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px">
  <span style="font-family:Tahoma,sans-serif;font-size:10px;letter-spacing:2px;
               color:var(--muted);text-transform:uppercase">
    Thai Summit Group — VX Production Intelligence
  </span>
  <span style="font-family:Tahoma,sans-serif;font-size:10px;color:var(--muted2);letter-spacing:1px">
    "Zero defect is possible"
  </span>
</div>
```

### KPI Card
```html
<div class="kpi">
  <div style="font-size:11px;color:var(--muted);font-weight:700;
              letter-spacing:1.5px;margin-bottom:10px;text-transform:uppercase;
              font-family:var(--font-display)">OEE</div>
  <div style="font-family:var(--font-display);font-size:40px;font-weight:700;
              line-height:1;color:var(--text);letter-spacing:-1px">82.4%</div>
  <div style="font-size:13px;color:var(--muted);margin-top:6px">Combined</div>
</div>
```

### Status Badge
```html
<!-- Good (green) -->
<span style="padding:2px 8px;border-radius:2px;font-size:11px;font-weight:700;
             background:rgba(61,214,92,0.1);color:var(--green);
             border:1px solid rgba(61,214,92,0.3);
             font-family:var(--font-display);letter-spacing:.5px">
  ✓ OK
</span>

<!-- Warn (orange) -->
<span style="padding:2px 8px;border-radius:2px;font-size:11px;font-weight:700;
             background:rgba(245,154,63,0.1);color:var(--amber);
             border:1px solid rgba(245,154,63,0.3);
             font-family:var(--font-display);letter-spacing:.5px">
  ⚠ WARN
</span>

<!-- Bad (red) -->
<span style="padding:2px 8px;border-radius:2px;font-size:11px;font-weight:700;
             background:rgba(192,57,43,0.1);color:var(--red);
             border:1px solid rgba(192,57,43,0.3);
             font-family:var(--font-display);letter-spacing:.5px">
  ✕ LOW
</span>
```

### Section Header
```html
<div style="display:flex;align-items:center;gap:12px;margin-bottom:16px">
  <h3 style="font-family:var(--font-display);font-size:12px;font-weight:700;
             color:var(--muted);text-transform:uppercase;letter-spacing:2.5px">
    Section Name
  </h3>
  <span style="font-size:11px;font-weight:700;padding:2px 8px;border-radius:2px;
               background:var(--accent-dim);color:var(--accent);
               border:1px solid rgba(13,61,20,0.2);letter-spacing:.8px;text-transform:uppercase">
    Tag
  </span>
  <div style="flex:1;height:1px;background:var(--border)"></div>
</div>
```

### Theme Toggle Button
```html
<button onclick="toggleTheme()"
  style="width:100%;padding:9px 12px;background:transparent;
         border:1px solid var(--border);border-radius:var(--radius-sm);
         color:var(--muted);font-family:var(--font-display);font-size:11px;
         font-weight:700;cursor:pointer;transition:all .2s;
         display:flex;align-items:center;justify-content:center;gap:8px;
         letter-spacing:1.5px;text-transform:uppercase">
  <span id="theme-icon">◐</span>
  <span id="theme-label">Dark Mode</span>
</button>
```

```js
function toggleTheme() {
  const next = document.documentElement.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
  document.documentElement.setAttribute('data-theme', next);
  localStorage.setItem('vx_theme', next);
  document.getElementById('theme-icon').textContent = next === 'dark' ? '◐' : '○';
  document.getElementById('theme-label').textContent = next === 'dark' ? 'Dark Mode' : 'Light Mode';
}
// On load
const saved = localStorage.getItem('vx_theme') || 'dark';
document.documentElement.setAttribute('data-theme', saved);
```

---

## 6. Dark Mode Loading Screen

```html
<div id="loading" style="position:fixed;inset:0;background:#060f07;z-index:999;
                         display:flex;align-items:center;justify-content:center;
                         flex-direction:column;gap:16px;
                         transition:opacity .8s,visibility .8s">

  <!-- Brand mark -->
  <div style="display:flex;align-items:center;gap:12px;margin-bottom:1.5rem">
    <div style="width:42px;height:42px;background:#0d3d14;border-radius:4px;
                display:flex;align-items:center;justify-content:center">
      <svg width="24" height="22" viewBox="0 0 24 22" fill="none">
        <path d="M12 1L23 21H1L12 1Z" fill="rgba(255,255,255,0.9)"/>
        <path d="M12 8L18 21H6L12 8Z" fill="#0d3d14"/>
      </svg>
    </div>
    <div style="text-align:left">
      <div style="font-family:Tahoma,sans-serif;font-size:13px;font-weight:700;
                  color:rgba(255,255,255,0.9);letter-spacing:1.5px;text-transform:uppercase">
        Thai Summit Group
      </div>
      <div style="font-family:Tahoma,sans-serif;font-size:10px;
                  color:rgba(255,255,255,0.4);letter-spacing:2px;text-transform:uppercase">
        VX Production System
      </div>
    </div>
  </div>

  <!-- Loading bar -->
  <div style="width:200px;height:2px;background:rgba(255,255,255,0.08);border-radius:1px">
    <div id="ld-bar" style="height:100%;width:0%;background:#3dd65c;
                            transition:width .4s cubic-bezier(.4,0,.2,1);border-radius:1px"></div>
  </div>

  <p style="font-family:Tahoma,sans-serif;font-size:11px;
            color:rgba(255,255,255,0.3);letter-spacing:2px;text-transform:uppercase">
    กำลังโหลดข้อมูล...
  </p>
</div>
```

---

## 7. Quick Reference — Color Hex

| Token | Light | Dark | Use |
|---|---|---|---|
| Primary Green | `#0d3d14` | `#3dd65c` | Accent, nav active, logo |
| Brand Orange | `#e87c1e` | `#f59a3f` | Charts, alerts, PD4 |
| Background | `#f5f7f5` | `#060f07` | Page bg |
| Card | `#ffffff` | `#0c1a0d` | Card surface |
| Border | `#d0ddd0` | `#1a2e1c` | Dividers |
| Text | `#0a1f0c` | `#e8f5e9` | Body text |
| Muted | `#6a8a6d` | `#4a6e4d` | Labels, captions |
| Good | `#1a7a1a` | `#3dd65c` | OEE ≥ 85% |
| Warn | `#cc7700` | `#f59a3f` | OEE 70–85% |
| Bad | `#c0392b` | `#e05c4a` | OEE < 70% |

---

## 8. HTML Boilerplate

```html
<!DOCTYPE html>
<html lang="th" data-theme="dark">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Page Title — Thai Summit VX</title>
<link href="https://fonts.googleapis.com/css2?family=Sarabun:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<style>
  /* paste CSS variables from Section 1 here */
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: var(--bg);
    color: var(--text);
    font-family: var(--font-body);
    min-height: 100vh;
  }
</style>
</head>
<body>
  <!-- content -->

  <script>
    // Theme restore
    const _t = localStorage.getItem('vx_theme') || 'dark';
    document.documentElement.setAttribute('data-theme', _t);
  </script>
</body>
</html>
```

---

*Thai Summit Group · VX Production Intelligence · "Zero defect is possible"*
