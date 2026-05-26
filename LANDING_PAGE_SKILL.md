# Landing Page Skill — Thai Summit VX

**Reference file:** `index.html`
Copy patterns below verbatim. Do not change color scheme, animation timing, or layout structure.

---

## 1. Color System

```css
:root {
  --g0:#060f07; --g1:#091209; --g2:#0c1a0d; --g3:#0f1e10;
  --accent:#3dd65c;   /* green — primary */
  --accent2:#e87c1e;  /* orange — secondary */
  --green-brand:#0d3d14; --green-mid:#1a6d2e;
  --border:rgba(61,214,92,0.12); --border2:rgba(61,214,92,0.06);
  --text:#e8f5e9; --text2:#a5c9a8; --muted:#4a6e4d;
  --ff:'Tahoma','Sarabun',sans-serif;   /* headings / numbers */
  --fs:'Sarabun',sans-serif;            /* body Thai */
}
```

---

## 2. Background Layers (copy all 4 in order)

```html
<div id="ambient"></div>   <!-- radial green + orange glow -->
<div id="grid"></div>      <!-- 80px dot-grid, masked to ellipse -->
<div id="noise"></div>     <!-- SVG fractalNoise texture -->
<div id="cur"></div>       <!-- custom cursor dot -->
<div id="cur-ring"></div>  <!-- cursor ring -->
```

```css
/* Ambient */
#ambient{position:fixed;inset:0;z-index:0;pointer-events:none;overflow:hidden}
#ambient::before{
  content:'';position:absolute;width:1100px;height:1100px;border-radius:50%;
  background:radial-gradient(circle,rgba(13,61,20,0.18) 0%,transparent 65%);
  top:50%;left:50%;transform:translate(-50%,-50%);
  animation:breathe 8s ease-in-out infinite alternate;
}
#ambient::after{
  content:'';position:absolute;width:500px;height:500px;border-radius:50%;
  background:radial-gradient(circle,rgba(232,124,30,0.08) 0%,transparent 70%);
  bottom:10%;right:10%;animation:drift2 20s ease-in-out infinite alternate;
}
@keyframes breathe{to{transform:translate(-50%,-50%) scale(1.15)}}
@keyframes drift2{to{transform:translate(-40px,-30px)}}

/* Grid */
#grid{
  position:fixed;inset:0;z-index:0;pointer-events:none;
  background-image:
    linear-gradient(rgba(61,214,92,0.028) 1px,transparent 1px),
    linear-gradient(90deg,rgba(61,214,92,0.028) 1px,transparent 1px);
  background-size:80px 80px;
  mask-image:radial-gradient(ellipse 70% 70% at 50% 50%,black 40%,transparent 100%);
}

/* Noise */
#noise{
  position:fixed;inset:0;z-index:1;pointer-events:none;opacity:.022;
  background-image:url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.85' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)'/%3E%3C/svg%3E");
  background-size:200px;
}
```

---

## 3. Custom Cursor

```css
#cur{
  position:fixed;top:0;left:0;z-index:9999;pointer-events:none;
  width:8px;height:8px;border-radius:50%;background:var(--accent);
  transform:translate(-50%,-50%);mix-blend-mode:screen;
  transition:width .18s,height .18s,background .2s;
}
#cur-ring{
  position:fixed;top:0;left:0;z-index:9998;pointer-events:none;
  width:30px;height:30px;border-radius:50%;
  border:1px solid rgba(61,214,92,0.4);transform:translate(-50%,-50%);
  transition:width .35s cubic-bezier(.4,0,.2,1),height .35s,opacity .25s,border-color .3s;
}
body.hov   #cur{width:44px;height:44px}
body.hov   #cur-ring{opacity:0}
body.click #cur{background:var(--accent2);width:14px;height:14px}
```

```js
// Cursor JS
const cur=document.getElementById('cur'),ring=document.getElementById('cur-ring');
let mx=0,my=0,rx=0,ry=0;
document.addEventListener('mousemove',e=>{
  mx=e.clientX;my=e.clientY;
  cur.style.cssText+=`;left:${mx}px;top:${my}px`;
});
(function loop(){
  rx+=(mx-rx)*.1;ry+=(my-ry)*.1;
  ring.style.left=rx+'px';ring.style.top=ry+'px';
  requestAnimationFrame(loop);
})();
document.querySelectorAll('a,.mod-card,.btn-p,.btn-g,.np').forEach(el=>{
  el.addEventListener('mouseenter',()=>document.body.classList.add('hov'));
  el.addEventListener('mouseleave',()=>document.body.classList.remove('hov'));
});
document.addEventListener('mousedown',()=>document.body.classList.add('click'));
document.addEventListener('mouseup',()=>document.body.classList.remove('click'));
// Set cursor:none on all interactive elements
```

---

## 4. Top Nav

```html
<nav id="nav">
  <a class="nav-brand" href="#">
    <img class="nav-logo-svg" src="TS_logo.png" alt="Thai Summit" style="object-fit:contain"/>
    <div>
      <div class="nav-brand-name">Thai Summit</div>
      <div class="nav-brand-sub">VX Production System</div>
    </div>
  </a>
  <div class="nav-r">
    <a class="np" href="overview.html">Overview</a>
    <a class="np" href="summary.html">Summary</a>
    <a class="np" href="jig_mtn.html">JIG MTN</a>
    <a class="np" href="line_status.html">Line Status</a>
    <a class="np cta" href="overview.html">Enter →</a>
  </div>
</nav>
```

```css
#nav{
  position:fixed;top:0;left:0;right:0;z-index:500;
  display:flex;align-items:center;justify-content:space-between;
  padding:0 48px;height:60px;
  transition:background .4s,backdrop-filter .4s,border-color .4s;
  border-bottom:1px solid transparent;
}
#nav.scrolled{background:rgba(6,15,7,0.9);backdrop-filter:blur(24px);border-bottom-color:var(--border2)}
.nav-brand{display:flex;align-items:center;gap:10px;text-decoration:none}
.nav-logo-svg{width:28px;height:28px}
.nav-brand-name{font-size:11px;font-weight:700;letter-spacing:2px;color:var(--text);text-transform:uppercase}
.nav-brand-sub{font-size:8px;letter-spacing:1.5px;color:var(--muted);text-transform:uppercase}
.np{
  padding:6px 14px;border-radius:2px;font-size:11px;font-weight:700;
  letter-spacing:1.5px;text-transform:uppercase;color:var(--text2);
  border:1px solid transparent;text-decoration:none;transition:all .2s;cursor:none;
}
.np:hover{color:var(--accent);border-color:var(--border)}
.np.cta{background:var(--accent);color:var(--g0);border-color:var(--accent)}
.np.cta:hover{background:transparent;color:var(--accent)}
```

```js
// Nav scroll glass effect
const nav=document.getElementById('nav');
window.addEventListener('scroll',()=>nav.classList.toggle('scrolled',scrollY>40),{passive:true});
```

---

## 5. Hero Section (2-column grid)

```html
<section id="hero">
  <!-- LEFT: text -->
  <div class="hero-left">
    <div class="hero-eyebrow">
      <div class="ey-line"></div>
      <div class="ey-text">YOUR EYEBROW TEXT</div>
      <div class="ey-line"></div>
    </div>
    <h1 class="hero-title">
      LINE 1<br>
      <span class="hl">HL</span>
      <span class="dim"> · </span>
      <span class="or">ORANGE</span>
    </h1>
    <p class="hero-sub">Body text Thai / English here</p>
    <div class="hero-cta">
      <a class="btn-p" href="TARGET.html"><span>Enter Dashboard</span><span>→</span></a>
      <a class="btn-g" href="OTHER.html">Secondary</a>
    </div>
    <div class="hero-stats">
      <div class="hs">
        <div class="hs-val">VALUE<span class="u">UNIT</span></div>
        <div class="hs-lbl">LABEL</div>
      </div>
      <div class="hs-div"></div>
      <!-- repeat hs blocks -->
    </div>
  </div>

  <!-- RIGHT: 3D Logo -->
  <div class="hero-right">
    <div class="orbit-ring or1"></div>
    <div class="orbit-ring or2"></div>
    <!-- floating particles (4 divs) -->
    <div id="logo-scene">
      <div id="logo-glow"></div>
      <div id="logo-3d">
        <img id="logo-img" src="TS_logo.png" alt="Thai Summit Logo"/>
      </div>
      <div id="logo-shadow"></div>
    </div>
  </div>
</section>
```

```css
#hero{
  position:relative;z-index:2;min-height:100vh;
  display:grid;grid-template-columns:1fr 1fr;
  align-items:center;padding:80px 48px;overflow:hidden;
}
.hero-left{display:flex;flex-direction:column;gap:0;padding-right:64px}
.hero-eyebrow{display:flex;align-items:center;gap:12px;margin-bottom:28px;
  opacity:0;transform:translateX(-24px);
  animation:slideR .8s cubic-bezier(.16,1,.3,1) .2s forwards}
.ey-line{width:32px;height:1px;background:var(--accent)}
.ey-text{font-size:10px;font-weight:700;letter-spacing:4px;color:var(--accent);text-transform:uppercase}
.hero-title{
  font-size:clamp(2.8rem,5.5vw,5.5rem);font-weight:700;line-height:.92;letter-spacing:-.03em;
  opacity:0;transform:translateX(-32px);
  animation:slideR .9s cubic-bezier(.16,1,.3,1) .35s forwards}
.hero-title .hl{color:var(--accent)}
.hero-title .or{color:var(--accent2)}
.hero-title .dim{color:rgba(232,245,233,0.18)}
.hero-sub{font-size:15px;color:var(--text2);margin-top:24px;line-height:1.75;font-weight:300;max-width:400px;
  opacity:0;transform:translateX(-24px);
  animation:slideR .8s cubic-bezier(.16,1,.3,1) .55s forwards}
.hero-cta{display:flex;align-items:center;gap:10px;margin-top:40px;
  opacity:0;transform:translateX(-24px);
  animation:slideR .8s cubic-bezier(.16,1,.3,1) .7s forwards}
.hero-stats{display:flex;gap:32px;margin-top:56px;
  opacity:0;transform:translateX(-24px);
  animation:slideR .8s cubic-bezier(.16,1,.3,1) .88s forwards}
.hs{display:flex;flex-direction:column;gap:4px}
.hs-val{font-size:26px;font-weight:700;color:var(--text);letter-spacing:-.5px}
.hs-val .u{font-size:14px;color:var(--accent);margin-left:2px}
.hs-lbl{font-size:9px;font-weight:700;color:var(--muted);letter-spacing:2px;text-transform:uppercase}
.hs-div{width:1px;background:var(--border2);align-self:stretch}
@keyframes slideR{to{opacity:1;transform:translateX(0)}}

/* Primary button */
.btn-p{
  display:inline-flex;align-items:center;gap:10px;
  padding:13px 28px;border-radius:2px;background:var(--accent);color:var(--g0);
  font-size:11px;font-weight:700;letter-spacing:2px;text-transform:uppercase;
  text-decoration:none;border:1px solid var(--accent);
  transition:background .25s,color .25s;cursor:none;position:relative;overflow:hidden;
}
.btn-p::after{content:'';position:absolute;inset:0;background:var(--g0);
  transform:translateX(-101%);transition:transform .3s cubic-bezier(.4,0,.2,1)}
.btn-p:hover::after{transform:translateX(0)}
.btn-p:hover{color:var(--accent)}
.btn-p span{position:relative;z-index:1}
/* Ghost button */
.btn-g{
  display:inline-flex;align-items:center;gap:8px;
  padding:13px 22px;border-radius:2px;background:transparent;
  font-size:11px;font-weight:700;letter-spacing:2px;text-transform:uppercase;
  text-decoration:none;color:var(--text2);border:1px solid var(--border);
  transition:all .25s;cursor:none;
}
.btn-g:hover{color:var(--accent);border-color:var(--accent)}
```

---

## 6. 3D Logo Scene

```css
.hero-right{display:flex;align-items:center;justify-content:center;
  position:relative;opacity:0;animation:fadeIn .6s ease .4s forwards}
@keyframes fadeIn{to{opacity:1}}

#logo-scene{perspective:900px;perspective-origin:50% 50%;
  width:360px;height:360px;display:flex;align-items:center;justify-content:center;position:relative}
#logo-3d{transform-style:preserve-3d;transform:rotateX(8deg) rotateY(-12deg);
  transition:transform .08s linear;position:relative;width:280px;height:280px}
#logo-glow{position:absolute;inset:-60px;border-radius:50%;
  background:radial-gradient(circle,rgba(61,214,92,0.18) 0%,rgba(232,124,30,0.08) 50%,transparent 70%);
  animation:glowPulse 4s ease-in-out infinite alternate;filter:blur(20px)}
@keyframes glowPulse{to{opacity:.6;transform:scale(1.15)}}
#logo-shadow{position:absolute;bottom:-50px;left:50%;transform:translateX(-50%);
  width:180px;height:20px;border-radius:50%;background:rgba(0,0,0,0.5);
  filter:blur(16px);animation:shadowPulse 4s ease-in-out infinite alternate}
@keyframes shadowPulse{to{width:160px;opacity:.7}}
#logo-img{width:100%;height:100%;object-fit:contain;
  filter:
    drop-shadow(-3px 3px 0 rgba(100,40,0,0.8))
    drop-shadow(-6px 6px 0 rgba(70,25,0,0.6))
    drop-shadow(-9px 9px 0 rgba(40,12,0,0.4))
    drop-shadow(-12px 12px 0 rgba(20,5,0,0.25))
    drop-shadow(0 0 48px rgba(232,124,30,0.3));}

/* Orbit rings */
.orbit-ring{position:absolute;border-radius:50%;border:1px solid;pointer-events:none}
.or1{width:300px;height:300px;top:50%;left:50%;
  transform:translate(-50%,-50%) rotateX(75deg);
  border-color:rgba(61,214,92,0.12);animation:orbitSpin1 18s linear infinite}
.or2{width:380px;height:380px;top:50%;left:50%;
  transform:translate(-50%,-50%) rotateX(75deg) rotateZ(45deg);
  border-color:rgba(232,124,30,0.08);animation:orbitSpin2 28s linear infinite reverse}
@keyframes orbitSpin1{to{transform:translate(-50%,-50%) rotateX(75deg) rotateZ(360deg)}}
@keyframes orbitSpin2{to{transform:translate(-50%,-50%) rotateX(75deg) rotateZ(-360deg)}}

/* Floating particles (add 4 divs with inline style) */
.particle{position:absolute;border-radius:50%;pointer-events:none;
  animation:float var(--pd,6s) ease-in-out infinite alternate}
@keyframes float{to{transform:translateY(var(--fy,-12px)) translateX(var(--fx,6px))}}
```

```js
// 3D logo mouse tracking
const scene=document.getElementById('logo-scene');
const logo3d=document.getElementById('logo-3d');
let targetRX=8,targetRY=-12,curRX=8,curRY=-12;
document.addEventListener('mousemove',e=>{
  const rect=scene.getBoundingClientRect();
  const dx=(e.clientX-(rect.left+rect.width/2))/window.innerWidth;
  const dy=(e.clientY-(rect.top+rect.height/2))/window.innerHeight;
  targetRY=dx*28-4; targetRX=-dy*20+6;
});
(function animLogo(){
  curRX+=(targetRX-curRX)*.06;curRY+=(targetRY-curRY)*.06;
  logo3d.style.transform=`rotateX(${curRX}deg) rotateY(${curRY}deg)`;
  const sh=document.getElementById('logo-shadow');
  sh.style.transform=`translateX(calc(-50% + ${curRY*0.6}px))`;
  sh.style.opacity=0.3+Math.abs(curRX)/60;
  requestAnimationFrame(animLogo);
})();
```

---

## 7. Module Cards Grid

```html
<section id="modules">
  <div class="sec-label reveal">
    <div class="sec-line"></div>
    <div class="sec-text">Dashboard Modules</div>
    <div class="sec-num">01 / 04</div>
  </div>
  <div class="mod-grid">
    <a class="mod-card mc-oee reveal reveal-d1" href="overview.html">
      <span class="mc-n">01</span>
      <span class="mc-icon">📊</span>
      <div class="mc-title">TITLE</div>
      <div class="mc-desc">Description text</div>
      <div class="mc-links">
        <a class="mc-link" href="PAGE.html" onclick="event.stopPropagation()">LABEL <span>→</span></a>
      </div>
    </a>
    <!-- repeat for mc-mtn, mc-ls, mc-plan -->
  </div>
</section>
```

```css
#modules{position:relative;z-index:2;padding:100px 48px;border-top:1px solid var(--border2)}
.sec-label{display:flex;align-items:center;gap:16px;margin-bottom:60px}
.sec-line{width:32px;height:1px;background:var(--accent)}
.sec-text{font-size:10px;font-weight:700;letter-spacing:4px;color:var(--accent);text-transform:uppercase}
.sec-num{font-size:10px;color:var(--muted);letter-spacing:2px;margin-left:auto}

.mod-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:1px;background:var(--border2);border:1px solid var(--border2)}
.mod-card{
  background:var(--g0);padding:40px 32px;
  position:relative;overflow:hidden;cursor:none;text-decoration:none;display:block;transition:background .3s;
}
.mod-card::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;
  background:var(--mc,var(--accent));transform:scaleX(0);transform-origin:left;
  transition:transform .4s cubic-bezier(.4,0,.2,1)}
.mod-card:hover::before{transform:scaleX(1)}
.mod-card:hover{background:var(--g2)}
.mod-card::after{content:'';position:absolute;width:180px;height:180px;border-radius:50%;
  background:radial-gradient(circle,var(--mc-glow,rgba(61,214,92,0.06)) 0%,transparent 70%);
  bottom:-60px;right:-40px;opacity:0;transition:opacity .4s}
.mod-card:hover::after{opacity:1}
.mc-n{font-size:9px;font-weight:700;letter-spacing:3px;color:var(--muted);text-transform:uppercase;margin-bottom:20px;display:block}
.mc-icon{font-size:28px;margin-bottom:20px;display:block}
.mc-title{font-size:16px;font-weight:700;color:var(--text);letter-spacing:-.2px;margin-bottom:10px}
.mc-desc{font-size:13px;color:var(--text2);line-height:1.7;font-weight:300}
.mc-links{margin-top:24px;display:flex;flex-direction:column;gap:4px}
.mc-link{font-size:10px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;
  color:var(--muted);text-decoration:none;padding:4px 0;
  border-bottom:1px solid transparent;transition:all .2s;cursor:none;
  display:inline-flex;align-items:center;gap:6px;width:fit-content}
.mc-link:hover{color:var(--mc,var(--accent));border-bottom-color:currentColor}

/* Card color variants */
.mc-oee{--mc:#3dd65c;--mc-glow:rgba(61,214,92,0.07)}
.mc-mtn{--mc:#e87c1e;--mc-glow:rgba(232,124,30,0.07)}
.mc-ls{--mc:#4dcc6a;--mc-glow:rgba(77,204,106,0.07)}
.mc-plan{--mc:#f59a3f;--mc-glow:rgba(245,154,63,0.07)}
```

---

## 8. Statement + Footer

```html
<!-- Statement -->
<section id="statement">
  <div class="stmt-pre reveal">Quality Declaration · Thai Summit Group</div>
  <div class="stmt-q reveal reveal-d1">"<em>Zero defect</em> is possible"</div>
  <div class="stmt-th reveal reveal-d2">"ปัญหาคุณภาพเป็นศูนย์คือสิ่งที่เป็นไปได้"</div>
</section>

<!-- Footer -->
<footer id="footer">
  <div class="ft-brand">
    <!-- SVG Thai Summit logo mark (inline) -->
    <div class="ft-text">Thai Summit Group · VX Production Intelligence</div>
  </div>
  <div class="ft-clock" id="ft-clock">—</div>
</footer>
```

```css
#statement{position:relative;z-index:2;padding:100px 48px;border-top:1px solid var(--border2);
  display:flex;flex-direction:column;align-items:center;text-align:center;gap:24px}
.stmt-q{font-size:clamp(2rem,4vw,3.8rem);font-weight:700;color:var(--text);
  letter-spacing:-.02em;max-width:720px;line-height:1.1}
.stmt-q em{font-style:normal;color:var(--accent)}
.stmt-th{font-size:15px;color:var(--muted);font-weight:300;font-style:italic}

#footer{position:relative;z-index:2;padding:28px 48px;border-top:1px solid var(--border2);
  display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px}
.ft-text{font-size:10px;letter-spacing:2px;color:var(--muted);text-transform:uppercase}
.ft-clock span{color:var(--accent)}
```

```js
// Live clock
function tick(){
  const d=new Date(),p=n=>String(n).padStart(2,'0');
  document.getElementById('ft-clock').innerHTML=
    `<span>${d.getFullYear()}.${p(d.getMonth()+1)}.${p(d.getDate())}</span> — ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}
tick();setInterval(tick,1000);
```

---

## 9. Scroll Reveal

```css
.reveal{opacity:0;transform:translateY(28px);
  transition:opacity .8s cubic-bezier(.16,1,.3,1),transform .8s cubic-bezier(.16,1,.3,1)}
.reveal.vis{opacity:1;transform:translateY(0)}
.reveal-d1{transition-delay:.1s}.reveal-d2{transition-delay:.2s}
.reveal-d3{transition-delay:.3s}.reveal-d4{transition-delay:.4s}
```

```js
const obs=new IntersectionObserver(
  es=>es.forEach(e=>{if(e.isIntersecting)e.target.classList.add('vis')}),
  {threshold:.12}
);
document.querySelectorAll('.reveal').forEach(el=>obs.observe(el));
```

---

## 10. Responsive

```css
@media(max-width:1100px){
  #hero{grid-template-columns:1fr;padding:100px 32px 60px;text-align:center}
  .hero-left{padding-right:0;align-items:center}
  .hero-right{margin-top:48px}
  .mod-grid{grid-template-columns:1fr 1fr}
}
@media(max-width:700px){
  #nav{padding:0 20px}.nav-r{display:none}
  #hero,#modules,#statement,#footer{padding-left:20px;padding-right:20px}
  .mod-grid{grid-template-columns:1fr}
  #logo-scene{width:260px;height:260px}
  #logo-3d{width:180px;height:200px}
}
@media(prefers-reduced-motion:reduce){
  *{animation:none!important;transition:none!important}
  .reveal{opacity:1;transform:none}
}
```

---

## 11. Rules

1. `body{cursor:none}` — custom cursor บน desktop เสมอ
2. ทุก `<a>` และ interactive element ต้องมี `cursor:none`
3. โลโก้ = `TS_logo.png` (อยู่ใน root) — ไม่ใช้ SVG แทน
4. 3D logo ใช้ CSS `drop-shadow` หลายชั้น — ไม่ใช้ canvas หรือ WebGL
5. Module card สีตาม `--mc` CSS variable — เพิ่ม variant ใหม่ด้วย pattern เดิม
6. Reveal animation ใช้ IntersectionObserver — ไม่ใช้ scroll event listener
7. ไม่มี external JS library ทั้งสิ้น — vanilla only
8. Font: `Sarabun` (Google Fonts) สำหรับ Thai, `Tahoma` fallback สำหรับ heading
9. ทุก section มี `position:relative;z-index:2` เพื่อ stack เหนือ background layers
10. `@media(prefers-reduced-motion)` ต้อง disable animation ทั้งหมด
