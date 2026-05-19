/**
 * part_img_widget.js
 * Shared widget: look up part images via registry aliases and render
 * thumbnail + lightbox modal into any page.
 *
 * Usage:
 *   <script src="part_img_widget.js"></script>
 *   PartImgWidget.init();
 *   PartImgWidget.attachThumb(containerEl, searchKey);
 */
(function(global) {
  const SUPABASE_URL = "https://ewhdfqwfwofivojtsizn.supabase.co";
  const SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV3aGRmcXdmd29maXZvanRzaXpuIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzY4ODA5NjYsImV4cCI6MjA5MjQ1Njk2Nn0.mGrLjRFmtNtpyAu3aBduKqixyb3AjQDCid06qpBzrxw";
  const BUCKET = "part-images";
  const HDR = { apikey: SUPABASE_KEY, Authorization: "Bearer " + SUPABASE_KEY };

  // Cache: key → images[]
  const _cache = {};
  // Registry cache: normalized alias → canonical_key
  let _registry = null;
  let _registryLoading = null;

  function publicUrl(path) {
    return `${SUPABASE_URL}/storage/v1/object/public/${BUCKET}/${path}`;
  }

  async function loadRegistry() {
    if (_registry) return _registry;
    if (_registryLoading) return _registryLoading;
    _registryLoading = fetch(`${SUPABASE_URL}/rest/v1/part_registry?select=canonical_key,aliases`, { headers: HDR })
      .then(r => r.json())
      .then(rows => {
        _registry = {};
        rows.forEach(row => {
          const ck = row.canonical_key;
          _registry[ck.toUpperCase()] = ck;
          (row.aliases || []).forEach(a => { _registry[a.toUpperCase()] = ck; });
        });
        return _registry;
      });
    return _registryLoading;
  }

  async function resolveCanonical(searchKey) {
    const reg = await loadRegistry();
    return reg[searchKey.toUpperCase()] || null;
  }

  async function fetchImages(canonicalKey) {
    if (_cache[canonicalKey] !== undefined) return _cache[canonicalKey];
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/part_images?part_no=eq.${encodeURIComponent(canonicalKey)}&order=uploaded_at.desc`,
      { headers: HDR }
    );
    const imgs = await res.json();
    _cache[canonicalKey] = imgs;
    return imgs;
  }

  // Look up images by any alias or canonical key (tries all keywords)
  async function getImages(searchKeys) {
    const keys = Array.isArray(searchKeys) ? searchKeys : [searchKeys];
    const reg = await loadRegistry();

    const canonicals = new Set();
    keys.forEach(k => {
      const c = reg[k.toUpperCase()];
      if (c) canonicals.add(c);
    });

    if (!canonicals.size) return [];

    const all = await Promise.all([...canonicals].map(fetchImages));
    return all.flat();
  }

  // ─── Modal ────────────────────────────────────────────────────
  let _modalEl = null;

  function ensureModal() {
    if (_modalEl) return;
    const div = document.createElement("div");
    div.id = "piw-modal";
    div.style.cssText = "display:none;position:fixed;inset:0;background:#000c;z-index:9999;align-items:center;justify-content:center;padding:16px";
    div.innerHTML = `
      <div style="background:#fff;border-radius:6px;max-width:92vw;max-height:92vh;overflow:hidden;display:flex;flex-direction:column;box-shadow:0 20px 60px #0005;min-width:300px">
        <div style="display:flex;align-items:center;justify-content:space-between;padding:14px 18px;border-bottom:1px solid #e0e0e0">
          <div>
            <div id="piw-modal-title" style="font-family:'Inter',sans-serif;font-size:15px;font-weight:700;color:#121212"></div>
            <div id="piw-modal-sub" style="font-size:12px;color:#7a7a7a;margin-top:2px"></div>
          </div>
          <button id="piw-modal-close" style="background:none;border:none;font-size:22px;cursor:pointer;color:#7a7a7a;padding:4px 8px;border-radius:4px;line-height:1">✕</button>
        </div>
        <div id="piw-modal-body" style="overflow-y:auto;padding:16px;flex:1">
          <div id="piw-modal-grid" style="display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:12px"></div>
        </div>
        <div id="piw-lb" style="display:none;position:fixed;inset:0;background:#000d;z-index:10000;align-items:center;justify-content:center;padding:20px">
          <div style="background:#111;border-radius:6px;overflow:hidden;max-width:90vw;max-height:90vh;display:flex;flex-direction:column">
            <img id="piw-lb-img" src="" style="max-width:85vw;max-height:75vh;object-fit:contain;display:block">
            <div style="display:flex;align-items:center;justify-content:space-between;padding:10px 14px;background:#1a1a1a">
              <span id="piw-lb-name" style="font-size:13px;color:#ccc"></span>
              <div style="display:flex;gap:8px">
                <a id="piw-lb-dl" href="" download style="padding:5px 12px;background:#333;color:#ccc;border-radius:4px;font-size:12px;text-decoration:none">ดาวน์โหลด</a>
                <button id="piw-lb-close" style="background:#333;border:none;color:#ccc;padding:5px 12px;border-radius:4px;font-size:12px;cursor:pointer">ปิด</button>
              </div>
            </div>
          </div>
        </div>
      </div>`;
    document.body.appendChild(div);
    _modalEl = div;

    div.addEventListener("click", e => { if (e.target === div) closeModal(); });
    div.querySelector("#piw-modal-close").addEventListener("click", closeModal);
    div.querySelector("#piw-lb-close").addEventListener("click", () => {
      div.querySelector("#piw-lb").style.display = "none";
    });
    div.querySelector("#piw-lb").addEventListener("click", e => {
      if (e.target === div.querySelector("#piw-lb")) div.querySelector("#piw-lb").style.display = "none";
    });
    document.addEventListener("keydown", e => {
      if (e.key === "Escape") {
        if (div.querySelector("#piw-lb").style.display !== "none") {
          div.querySelector("#piw-lb").style.display = "none";
        } else {
          closeModal();
        }
      }
    });
  }

  function openLightboxInModal(url, name) {
    const lb = _modalEl.querySelector("#piw-lb");
    _modalEl.querySelector("#piw-lb-img").src = url;
    _modalEl.querySelector("#piw-lb-name").textContent = name;
    _modalEl.querySelector("#piw-lb-dl").href = url;
    _modalEl.querySelector("#piw-lb-dl").download = name;
    lb.style.display = "flex";
  }

  function showModal(label, images) {
    ensureModal();
    _modalEl.querySelector("#piw-modal-title").textContent = "รูปชิ้นงาน: " + label;
    _modalEl.querySelector("#piw-modal-sub").textContent = images.length + " รูป";

    const grid = _modalEl.querySelector("#piw-modal-grid");
    if (!images.length) {
      grid.innerHTML = `<div style="grid-column:1/-1;text-align:center;padding:32px;color:#7a7a7a;font-size:14px">ยังไม่มีรูปสำหรับ ${label}</div>`;
    } else {
      grid.innerHTML = images.map(img => {
        const url = publicUrl(img.image_path);
        const dt = new Date(img.uploaded_at).toLocaleDateString("th-TH", { day: "numeric", month: "short" });
        return `<div style="border:1px solid #e0e0e0;border-radius:4px;overflow:hidden;cursor:pointer;transition:box-shadow .2s" onclick="PartImgWidget._openLb('${url}','${(img.file_name||'').replace(/'/g,"\\'")}')">
          <img src="${url}" style="width:100%;aspect-ratio:4/3;object-fit:cover;display:block;background:#f3f4f6" loading="lazy">
          <div style="padding:6px 8px">
            <div style="font-size:10px;color:#7a7a7a;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${img.file_name || ""}</div>
            <div style="font-size:10px;color:#aaa">${dt}${img.note ? " · " + img.note : ""}</div>
          </div>
        </div>`;
      }).join("");
    }

    _modalEl.style.display = "flex";
  }

  function closeModal() {
    if (_modalEl) _modalEl.style.display = "none";
  }

  // ─── Thumbnail ────────────────────────────────────────────────
  /**
   * Attach a thumbnail strip + camera button to a container element.
   * searchKeys: string or string[] — canonical key or any alias
   * label: display name shown in modal header
   */
  async function attachThumb(containerEl, searchKeys, label) {
    const imgs = await getImages(searchKeys);
    if (!imgs.length) {
      // Show a faint "📷 เพิ่มรูป" link if no images
      const btn = document.createElement("a");
      btn.href = "part_images.html";
      btn.title = "เพิ่มรูปชิ้นงาน";
      btn.style.cssText = "display:inline-flex;align-items:center;gap:4px;font-size:11px;color:#aaa;text-decoration:none;padding:2px 6px;border:1px dashed #ccc;border-radius:3px;margin-top:6px;transition:all .2s";
      btn.innerHTML = "📷 เพิ่มรูป";
      btn.addEventListener("mouseenter", () => { btn.style.borderColor = "#e31937"; btn.style.color = "#e31937"; });
      btn.addEventListener("mouseleave", () => { btn.style.borderColor = "#ccc"; btn.style.color = "#aaa"; });
      containerEl.appendChild(btn);
      return;
    }

    // Show up to 3 thumbnails + a count badge
    const wrap = document.createElement("div");
    wrap.style.cssText = "display:flex;align-items:center;gap:6px;margin-top:8px;cursor:pointer";
    wrap.title = "ดูรูปทั้งหมด " + imgs.length + " รูป";

    const first3 = imgs.slice(0, 3);
    first3.forEach(img => {
      const th = document.createElement("img");
      th.src = publicUrl(img.image_path);
      th.style.cssText = "width:44px;height:44px;object-fit:cover;border-radius:3px;border:1px solid #e0e0e0;flex-shrink:0";
      th.loading = "lazy";
      wrap.appendChild(th);
    });

    if (imgs.length > 3) {
      const more = document.createElement("div");
      more.style.cssText = "width:44px;height:44px;background:#f3f4f6;border:1px solid #e0e0e0;border-radius:3px;display:flex;align-items:center;justify-content:center;font-size:11px;color:#7a7a7a;font-weight:600;flex-shrink:0";
      more.textContent = "+" + (imgs.length - 3);
      wrap.appendChild(more);
    }

    wrap.addEventListener("click", () => showModal(label || (Array.isArray(searchKeys) ? searchKeys[0] : searchKeys), imgs));
    containerEl.appendChild(wrap);
  }

  // ─── Public API ───────────────────────────────────────────────
  const API = {
    init() { ensureModal(); loadRegistry(); },
    attachThumb,
    getImages,
    resolveCanonical,
    showModal,
    reloadRegistry() { _registry = null; _registryLoading = null; return loadRegistry(); },
    _openLb: openLightboxInModal,
  };

  global.PartImgWidget = API;
})(window);
