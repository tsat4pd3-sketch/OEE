/**
 * admin_auth.js
 * Login modal for OEE Admin pages using existing Supabase users.
 *
 * Usage:
 *   <script src="admin_auth.js"></script>
 *   AdminAuth.require(onReady)   // blocks page until logged in, then calls onReady()
 *   AdminAuth.signOut()
 *   AdminAuth.isAuthed()
 *   AdminAuth.getHeaders()       // fetch headers with Bearer token
 */
(function(global) {
  const SUPABASE_URL = "https://ewhdfqwfwofivojtsizn.supabase.co";
  const SUPABASE_ANON = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV3aGRmcXdmd29maXZvanRzaXpuIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzY4ODA5NjYsImV4cCI6MjA5MjQ1Njk2Nn0.mGrLjRFmtNtpyAu3aBduKqixyb3AjQDCid06qpBzrxw";
  const SESSION_KEY = "oee_admin_session";

  let _session = null;
  let _onReady = null;

  // ── Supabase Auth ──────────────────────────────────────────────
  async function signIn(email, password) {
    const res = await fetch(`${SUPABASE_URL}/auth/v1/token?grant_type=password`, {
      method: "POST",
      headers: { "apikey": SUPABASE_ANON, "Content-Type": "application/json" },
      body: JSON.stringify({ email, password })
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error_description || data.msg || "อีเมลหรือรหัสผ่านไม่ถูกต้อง");
    return data;
  }

  async function refreshSession(refresh_token) {
    const res = await fetch(`${SUPABASE_URL}/auth/v1/token?grant_type=refresh_token`, {
      method: "POST",
      headers: { "apikey": SUPABASE_ANON, "Content-Type": "application/json" },
      body: JSON.stringify({ refresh_token })
    });
    if (!res.ok) return null;
    return res.json();
  }

  // ── Session persistence ────────────────────────────────────────
  function saveSession(data) {
    _session = {
      access_token: data.access_token,
      refresh_token: data.refresh_token,
      expires_at: Date.now() + (data.expires_in || 3600) * 1000
    };
    localStorage.setItem(SESSION_KEY, JSON.stringify(_session));
  }

  function loadSession() {
    try { return JSON.parse(localStorage.getItem(SESSION_KEY)); } catch { return null; }
  }

  function clearSession() {
    _session = null;
    localStorage.removeItem(SESSION_KEY);
  }

  async function restoreSession() {
    const saved = loadSession();
    if (!saved) return false;
    if (saved.expires_at - 60000 > Date.now()) { _session = saved; return true; }
    const data = await refreshSession(saved.refresh_token);
    if (!data?.access_token) { clearSession(); return false; }
    saveSession(data);
    return true;
  }

  // ── Modal UI ───────────────────────────────────────────────────
  function buildModal() {
    const el = document.createElement("div");
    el.id = "aa-overlay";
    el.style.cssText = "position:fixed;inset:0;z-index:99999;display:flex;align-items:center;justify-content:center;background:rgba(5,5,5,0.92);backdrop-filter:blur(6px)";

    el.innerHTML = `
      <div style="
        background:#0d0d0d;border:1px solid #2a2a2a;border-radius:10px;
        padding:36px 32px;width:340px;text-align:center;
        box-shadow:0 24px 64px rgba(0,0,0,0.8);
        font-family:'Inter','Sarabun',sans-serif;
      ">
        <div style="font-size:32px;margin-bottom:8px">🔐</div>
        <div style="font-size:17px;font-weight:700;color:#f0f0f0;margin-bottom:4px">OEE Admin</div>
        <div style="font-size:12px;color:#555;margin-bottom:24px">ลงชื่อเข้าใช้ด้วย Supabase account</div>

        <input id="aa-email" type="email" placeholder="อีเมล"
          style="width:100%;box-sizing:border-box;background:#1a1a1a;border:1px solid #333;border-radius:6px;color:#f0f0f0;font-size:14px;text-align:left;padding:11px 14px;outline:none;margin-bottom:10px;transition:border-color .2s;">

        <input id="aa-pw" type="password" placeholder="รหัสผ่าน"
          style="width:100%;box-sizing:border-box;background:#1a1a1a;border:1px solid #333;border-radius:6px;color:#f0f0f0;font-size:14px;text-align:left;padding:11px 14px;outline:none;margin-bottom:16px;transition:border-color .2s;">

        <button id="aa-btn" style="
          width:100%;padding:12px;border:none;border-radius:6px;
          background:#e31937;color:#fff;font-size:14px;font-weight:700;
          cursor:pointer;transition:background .2s;letter-spacing:.5px;
        ">เข้าสู่ระบบ</button>

        <div id="aa-err" style="margin-top:12px;font-size:12px;color:#e31937;min-height:18px;"></div>
      </div>`;

    document.body.appendChild(el);

    const emailEl = el.querySelector("#aa-email");
    const pwEl    = el.querySelector("#aa-pw");
    const btnEl   = el.querySelector("#aa-btn");
    const errEl   = el.querySelector("#aa-err");

    function setError(msg) {
      errEl.textContent = msg;
      emailEl.style.borderColor = msg ? "#e31937" : "#333";
      pwEl.style.borderColor    = msg ? "#e31937" : "#333";
    }

    function setLoading(v) {
      btnEl.disabled = v;
      btnEl.textContent = v ? "กำลังเข้าสู่ระบบ..." : "เข้าสู่ระบบ";
    }

    async function submit() {
      const email = emailEl.value.trim();
      const pw    = pwEl.value;
      if (!email) { setError("กรุณากรอกอีเมล"); return; }
      if (!pw)    { setError("กรุณากรอกรหัสผ่าน"); return; }
      setError(""); setLoading(true);
      try {
        const data = await signIn(email, pw);
        saveSession(data);
        el.remove();
        if (_onReady) _onReady();
      } catch(e) {
        setError(e.message);
        setLoading(false);
      }
    }

    btnEl.addEventListener("click", submit);
    pwEl.addEventListener("keydown", e => { if (e.key === "Enter") submit(); });
    emailEl.addEventListener("keydown", e => { if (e.key === "Enter") pwEl.focus(); });

    setTimeout(() => emailEl.focus(), 100);
  }

  // ── Public API ─────────────────────────────────────────────────
  async function require(onReady) {
    _onReady = onReady;
    const ok = await restoreSession();
    if (ok) { onReady(); return; }
    buildModal();
  }

  function isAuthed() {
    return !!_session && _session.expires_at > Date.now();
  }

  function getHeaders() {
    const token = _session?.access_token || SUPABASE_ANON;
    return { "apikey": SUPABASE_ANON, "Authorization": "Bearer " + token };
  }

  function signOut() {
    clearSession();
    window.location.reload();
  }

  global.AdminAuth = { require, isAuthed, getHeaders, signOut };
})(window);
