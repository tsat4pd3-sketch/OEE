# Tesla-inspired OEE Dashboard Template Guide

**Reference Page:** `overview.html` — Use as gold standard template for all OEE pages

---

## 🎨 Design System Variables

All pages must include these CSS variables in `:root` and `[data-theme="dark"]`:

```css
:root {
  /* Light mode defaults */
  --bg: #f2f2f2; --bg2: #ffffff; --bg3: #f7f7f7; --card: #ffffff;
  --border: #e0e0e0; --border2: #cccccc;
  --accent: #e31937; /* Tesla Red */
  --text: #121212; --text2: #404040; --muted: #7a7a7a;
  --pd3: #1a6dcc; --pd4: #cc8800;
  --font-body: 'Sarabun', sans-serif;
  --font-display: 'Inter', sans-serif;
}

[data-theme="dark"] {
  /* Dark mode — Tesla signature black */
  --bg: #050505; --bg2: #0a0a0a; --bg3: #111111; --card: #0d0d0d;
  --border: #1e1e1e; --border2: #2a2a2a;
  --accent: #e31937;
  --text: #ffffff; --text2: #a8a8a8; --muted: #555555;
  --pd3: #4d9fff; --pd4: #ffaa40;
  --green: #22c55e; --amber: #f59e0b;
}
```

---

## 🚀 Feature 1: Intro Splash Screen

**Purpose:** Auto-closing loading screen with animated letters & progress bar

### HTML Structure (Inside `<body>`)
```html
<div id="loading">
  <div class="intro-letters">
    <span class="intro-letter" id="il0">P</span>
    <span class="intro-letter" id="il1">D</span>
    <span class="intro-letter red" id="il2">3</span>
    <span class="intro-letter dim" id="il3">+</span>
    <span class="intro-letter" id="il4">P</span>
    <span class="intro-letter" id="il5">D</span>
    <span class="intro-letter red" id="il6">4</span>
  </div>
  <div class="intro-sub" id="intro-sub">Production Intelligence</div>
  <p class="intro-status" id="ld-msg">กำลังดึงข้อมูล...</p>
  <div class="ld-bar-wrap"><div class="ld-bar-fill" id="ld-bar"></div></div>
  <div class="src-badges">
    <span id="ld-pd3" style="...">PD3 ...</span>
    <span id="ld-pd4" style="...">PD4 ...</span>
  </div>
</div>
```

### CSS Styling
```css
#loading {
  position: fixed; inset: 0; background: #050505; z-index: 999;
  display: flex; align-items: center; justify-content: center;
  flex-direction: column;
  transition: opacity 0.9s cubic-bezier(0.4, 0, 0.2, 1);
}
#loading.hidden {
  opacity: 0; visibility: hidden; pointer-events: none;
}

.intro-letter {
  font-family: var(--font-display); font-size: clamp(3.5rem, 11vw, 8.5rem);
  font-weight: 800; transform: translateY(115%); opacity: 0;
  transition: transform 0.7s cubic-bezier(0.16, 1, 0.3, 1), opacity 0.5s ease;
  color: #ffffff;
}
.intro-letter.up { transform: translateY(0); opacity: 1; }
.intro-letter.red { color: #e31937; }
.intro-letter.dim { color: rgba(255, 255, 255, 0.22); }

.ld-bar-fill { height: 100%; width: 0%; background: #e31937; }
```

### JavaScript: Animate Letters & Auto-close
```javascript
/* Animate letters on page load */
(function(){
  const letters = [0,1,2,3,4,5,6];
  letters.forEach((i) => {
    setTimeout(() => {
      const el = document.getElementById('il' + i);
      if(el) el.classList.add('up');
    }, 120 + i * 100);
  });
  setTimeout(() => {
    const sub = document.getElementById('intro-sub');
    if(sub) sub.classList.add('up');
  }, 900);
})();

/* Auto-close after data loads (in your render/init function) */
if(typeof window.enterDashboard === 'function') {
  window.enterDashboard(); // After setBar(100) & render()
}

/* Define the close function once */
window.enterDashboard = function(){
  const el = document.getElementById('loading');
  if(el) el.classList.add('hidden');
  setTimeout(() => { if(el) el.style.display = 'none'; }, 950);
};
```

---

## 📊 Feature 2: Marquee KPI Bar

**Purpose:** Scrolling ticker showing live per-line OEE percentages

### HTML Structure
```html
<div id="marquee-bar" style="display:none;border-top:1px solid var(--border);border-bottom:1px solid var(--border);...">
  <div class="marquee-track" id="marquee-track">
    <!-- Populated by JS after render() -->
  </div>
</div>
```

### CSS Styling
```css
#marquee-bar {
  background: var(--bg);
  position: sticky; top: 0; z-index: 50;
  height: 36px; overflow: hidden;
}

.marquee-track {
  display: flex; gap: 20px;
  animation: marquee linear infinite;
}

@keyframes marquee {
  0% { transform: translateX(0); }
  100% { transform: translateX(-50%); }
}

.marquee-item {
  display: flex; gap: 8px; align-items: center; white-space: nowrap;
  flex-shrink: 0;
}

.m-line { color: var(--text2); font-weight: 600; }
.m-val {
  font-weight: 700; font-family: var(--font-display);
}
.m-val.good { color: #22c55e; }
.m-val.warn { color: #f59e0b; }
.m-val.bad { color: #e31937; }
```

### JavaScript: Populate Marquee
```javascript
window.updateMarquee = function(){
  if(typeof getFiltered !== 'function') return;
  const {all} = getFiltered(); // Your filtered data function
  
  const lineMap = {};
  all.forEach(d => {
    if(!d.part || !d.oee || d.oee <= 0) return;
    if(!lineMap[d.part]) lineMap[d.part] = {sum: 0, cnt: 0};
    lineMap[d.part].sum += d.oee;
    lineMap[d.part].cnt++;
  });
  
  const items = Object.entries(lineMap)
    .map(([line, v]) => ({ line, oee: (v.sum / v.cnt) }))
    .sort((a, b) => b.oee - a.oee);
  
  if(!items.length) return;
  
  const oc = v => v >= 85 ? 'good' : v >= 70 ? 'warn' : 'bad';
  const html = [...items, ...items] // Double for seamless loop
    .map(it => `
      <div class="marquee-item">
        <span class="m-sep">▸</span>
        <span class="m-line">${it.line}</span>
        <span class="m-val ${oc(it.oee)}">${it.oee.toFixed(1)}%</span>
      </div>
    `).join('');
  
  const track = document.getElementById('marquee-track');
  if(track) track.innerHTML = html;
  
  const bar = document.getElementById('marquee-bar');
  if(bar) bar.style.display = 'block';
  
  // Adjust animation speed
  const dur = Math.max(20, items.length * 4);
  if(track) track.style.animationDuration = dur + 's';
};

/* Hook into render */
const _rOrig = window.render;
if(typeof _rOrig === 'function'){
  window.render = function(){
    _rOrig.apply(this, arguments);
    setTimeout(window.updateMarquee, 80);
  };
}
```

---

## 🎯 Feature 3: Custom Cursor

**Purpose:** Tesla-style red cursor with lag effect

### HTML (Inside `<body>`)
```html
<div id="cursor" style="position:fixed;width:8px;height:8px;background:#e31937;border-radius:50%;pointer-events:none;z-index:9999;mix-blend-mode:difference"></div>
<div id="cursor-ring" style="position:fixed;width:20px;height:20px;border:2px solid #e31937;border-radius:50%;pointer-events:none;z-index:9998"></div>
```

### CSS (Optional enhancements)
```css
body.cur-hover #cursor {
  background: #ffffff;
  box-shadow: 0 0 8px rgba(227, 25, 55, 0.6);
}

body.cur-hover #cursor-ring {
  border-color: #ffffff;
  opacity: 0.5;
}
```

### JavaScript
```javascript
(function(){
  const dot = document.getElementById('cursor');
  const ring = document.getElementById('cursor-ring');
  if(!dot || !ring) return;
  
  // Only on true pointer devices
  if(!window.matchMedia('(hover:hover)').matches) return;
  
  let mx = 0, my = 0, rx = 0, ry = 0;
  
  document.addEventListener('mousemove', e => {
    mx = e.clientX;
    my = e.clientY;
    dot.style.left = mx + 'px';
    dot.style.top = my + 'px';
  });
  
  /* Lag effect using requestAnimationFrame */
  (function animRing(){
    rx += (mx - rx) * 0.11; // Easing factor
    ry += (my - ry) * 0.11;
    ring.style.left = rx + 'px';
    ring.style.top = ry + 'px';
    requestAnimationFrame(animRing);
  })();
  
  /* Add hover state to interactive elements */
  document.querySelectorAll('a, button, .card, .kpi, .src-pill, .tg-btn')
    .forEach(el => {
      el.addEventListener('mouseenter', () => 
        document.body.classList.add('cur-hover'));
      el.addEventListener('mouseleave', () => 
        document.body.classList.remove('cur-hover'));
    });
})();
```

---

## ✨ Feature 4: Parallax + Noise Texture

**Purpose:** Subtle scroll parallax + film grain overlay

### HTML (Inside `<body>`)
```html
<div id="noise-overlay" style="position:fixed;inset:0;pointer-events:none;opacity:0.028;z-index:1;background-image:url('data:image/svg+xml;utf8,<svg xmlns=%22http://www.w3.org/2000/svg%22 width=%22100%22 height=%22100%22><filter id=%22noise%22><feTurbulence type=%22fractalNoise%22 baseFrequency=%220.85%22 numOctaves=%224%22 result=%22noise%22/></filter><rect width=%22100%22 height=%22100%22 fill=%22%23000%22 filter=%22url(%23noise)%22/></svg>')"></div>
```

### CSS for Parallax
```css
#main-title {
  transition: transform 0.1s ease-out;
}

#range-lbl {
  transition: transform 0.1s ease-out;
}
```

### JavaScript: Parallax on Scroll
```javascript
(function(){
  const mainEl = document.getElementById('main');
  if(!mainEl) return;
  
  mainEl.addEventListener('scroll', () => {
    const y = mainEl.scrollTop;
    
    const title = document.getElementById('main-title');
    if(title) title.style.transform = `translateY(${y * 0.12}px)`;
    
    const range = document.getElementById('range-lbl');
    if(range) range.style.transform = `translateY(${y * 0.08}px)`;
  });
})();
```

---

## 🎬 Feature 5: KPI Count-up Animation

**Purpose:** Smooth number animation from 0 to final value

### HTML Structure (Add to KPI elements)
```html
<div class="kpi-val" data-end="85.3" data-suffix="%">0%</div>
```

### JavaScript Function
```javascript
window.animateKPIs = function(){
  document.querySelectorAll('.kpi-val[data-end]').forEach(el => {
    const end = parseFloat(el.dataset.end);
    const suffix = el.dataset.suffix || '';
    const decimals = (el.dataset.end.includes('.')) 
      ? el.dataset.end.split('.')[1].length : 0;
    
    if(isNaN(end)) return;
    
    el.classList.add('counting');
    const dur = 900; // 900ms animation
    const start = performance.now();
    
    function step(now){
      const t = Math.min((now - start) / dur, 1);
      // easeOutCubic easing
      const ease = 1 - Math.pow(1 - t, 3);
      el.textContent = (end * ease).toFixed(decimals) + suffix;
      
      if(t < 1) requestAnimationFrame(step);
      else { 
        el.textContent = end.toFixed(decimals) + suffix;
        el.classList.remove('counting');
      }
    }
    requestAnimationFrame(step);
  });
};

/* Call after setting all KPI data-end values */
if(typeof window.animateKPIs === 'function') {
  window.animateKPIs();
}
```

### For Large Numbers (with toLocaleString)
```javascript
window.animateKPIs = function(){
  document.querySelectorAll('.kpi-val[data-end]').forEach(el => {
    const end = parseFloat(el.dataset.end);
    const suffix = el.dataset.suffix || '';
    const decimals = (el.dataset.end.includes('.')) 
      ? el.dataset.end.split('.')[1].length : 0;
    if(isNaN(end)) return;
    
    el.classList.add('counting');
    const dur = 900;
    const start = performance.now();
    
    function step(now){
      const t = Math.min((now - start) / dur, 1);
      const ease = 1 - Math.pow(1 - t, 3);
      const val = (end * ease).toFixed(decimals);
      const formatted = parseInt(val) > 999 
        ? parseInt(val).toLocaleString('en-US') : val;
      el.textContent = formatted + suffix;
      
      if(t < 1) requestAnimationFrame(step);
      else {
        const finalVal = end.toFixed(decimals);
        const finalFormatted = parseInt(finalVal) > 999 
          ? parseInt(finalVal).toLocaleString('en-US') : finalVal;
        el.textContent = finalFormatted + suffix;
        el.classList.remove('counting');
      }
    }
    requestAnimationFrame(step);
  });
};
```

---

## 🔧 Quick Adaptation Checklist for New Pages

### Step 1: CSS Setup
- [ ] Copy all CSS variables (`:root` & `[data-theme="dark"]`)
- [ ] Add `[data-theme="dark"]` to `<html>` tag
- [ ] Include both font families from Google Fonts
- [ ] Use `color: var(--text)`, `background: var(--bg)`, `border: 1px solid var(--border)`

### Step 2: Intro Splash (Optional)
- [ ] Add `<div id="loading">` with letters/progress bar HTML
- [ ] Include intro animation IIFE script
- [ ] Define `window.enterDashboard()` function
- [ ] Call `enterDashboard()` after data loads

### Step 3: Marquee Bar (Optional)
- [ ] Add `<div id="marquee-bar">` with `#marquee-track` child
- [ ] Include `updateMarquee()` function
- [ ] Hook into render: `window.render = function(){ _origRender(); setTimeout(updateMarquee, 80); }`

### Step 4: Custom Cursor
- [ ] Add `#cursor` and `#cursor-ring` divs to body
- [ ] Include cursor animation IIFE script
- [ ] Add `cur-hover` CSS class for hover effects

### Step 5: Parallax + Noise
- [ ] Add `#noise-overlay` div with SVG
- [ ] Add `#main-title` and similar elements with scroll listeners
- [ ] Include parallax scroll IIFE script

### Step 6: KPI Animations
- [ ] Add `data-end` and `data-suffix` to KPI elements
- [ ] Define `window.animateKPIs()` function
- [ ] Call after setting KPI values: `if(typeof window.animateKPIs === 'function') window.animateKPIs()`

---

## 📋 Example: Minimal Page Setup

```html
<!DOCTYPE html>
<html lang="th" data-theme="dark">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>My OEE Dashboard</title>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=Sarabun:wght@300;400;500;600;700&display=swap" rel="stylesheet">
  <style>
    :root { /* CSS vars */ }
    [data-theme="dark"] { /* Dark mode vars */ }
    
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { background: var(--bg); color: var(--text); font-family: var(--font-body); }
    
    /* Feature CSS here: intro, marquee, noise, etc. */
  </style>
</head>
<body>
  <!-- Intro Splash -->
  <div id="loading"><!-- ... --></div>
  
  <!-- Cursor -->
  <div id="cursor"></div>
  <div id="cursor-ring"></div>
  
  <!-- Noise -->
  <div id="noise-overlay"></div>
  
  <!-- Your Content -->
  <main id="main">
    <!-- Marquee (if needed) -->
    <div id="marquee-bar"></div>
    
    <!-- Page Content -->
    <div id="main-title">My Dashboard</div>
  </main>
  
  <script>
    /* Feature scripts: intro, cursor, marquee, parallax, KPI animation */
  </script>
</body>
</html>
```

---

## 🎨 Color Reference

| Element | Dark Mode | Light Mode | Usage |
|---------|-----------|-----------|-------|
| Background | `#050505` | `#f2f2f2` | `--bg` |
| Card | `#0d0d0d` | `#ffffff` | `--card` |
| Border | `#1e1e1e` | `#e0e0e0` | `--border` |
| Text | `#ffffff` | `#121212` | `--text` |
| Accent (Red) | `#e31937` | `#e31937` | `--accent` |
| PD3 (Cyan) | `#4d9fff` | `#1a6dcc` | `--pd3` |
| PD4 (Orange) | `#ffaa40` | `#cc8800` | `--pd4` |
| Success | `#22c55e` | `#1a7a1a` | `--green` |

---

## 📞 Support Files

- **Gold Standard:** `/home/user/OEE/overview.html` — Reference implementation
- **Already Applied:** `oee_pd3.html`, `oee_pd4.html`, `oee_deep_pd3.html`, `oee_deep_pd4.html`, `summary.html`
- **Branch:** `claude/standardize-templates-symbols-uqt6g`

---

**Last Updated:** 2026-05-08  
**Status:** Ready for production use
