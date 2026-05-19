/**
 * admin_auth.js
 * PIN-based admin session for OEE internal dashboard.
 *
 * Flow:
 *  - First visit: "Setup PIN" → creates Supabase account
 *  - Subsequent visits: "Enter PIN" → restores session
 *  - Session persists in localStorage (Supabase handles expiry/refresh)
 *
 * Usage:
 *   <script src="admin_auth.js"></script>
 *   AdminAuth.require(onReady)   // blocks page, calls onReady() when authed
 *   AdminAuth.signOut()
 *   AdminAuth.isAuthed()         // sync check
 *   AdminAuth.getHeaders()       // fetch headers with Bearer token
 */
(function(global) {
  const SUPABASE_URL = "https://ewhdfqwfwofivojtsizn.supabase.co";
  const SUPABASE_ANON = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImV3aGRmcXdmd29maXZvanRzaXpuIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzY4ODA5NjYsImV4cCI6MjA5MjQ1Njk2Nn0.mGrLjRFmtNtpyAu3aBduKqixyb3AjQDCid06qpBzrxw";
  const ADMIN_EMAIL = "admin@oee-dashboard.local";
  const SESSION_KEY = "oee_admin_session";

  let _session = null;   // { access_token, refresh_token, expires_at }
  let _onReady = null;

  // ── Supabase Auth REST helpers ─────────────────────────────────
  async function signIn(pin) {
    const res = await fetch(`${SUPABASE_URL}/auth/v1/token?grant_type=password`, {
      method: "POST",
      headers: { "apikey": SUPABASE_ANON, "Content-Type": "application/json" },
      body: JSON.stringify({ email: ADMIN_EMAIL, password: pin })
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error_description || data.msg || "PIN ไม่ถูกต้อง");
    return data;
  }

  async function signUp(pin) {
    const res = await fetch(`${SUPABASE_URL}/auth/v1/signup`, {
      method: "POST",
      headers: { "apikey": SUPABASE_ANON, "Content-Type": "application/json" },
      body: JSON.stringify({ email: ADMIN_EMAIL, password: pin })
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error_description || data.msg || "ตั้ง PIN ไม่สำเร็จ");
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
    // If still valid (with 60s buffer), use it
    if (saved.expires_at - 60000 > Date.now()) {
      _session = saved;
      return true;
    }
    // Try refresh
    const data = await refreshSession(saved.refresh_token);
    if (!data?.access_token) { clearSession(); return false; }
    saveSession(data);
    return true;
  }

  // ── Modal UI ───────────────────────────────────────────────────
  function buildModal() {
    const el = document.createElement("div");
    el.id = "aa-overlay";
    el.style.cssText = [
      "position:fixed;inset:0;z-index:99999",
      "display:flex;align-items:center;justify-content:center",
      "background:rgba(5,5,5,0.92);backdrop-filter:blur(6px)"
    ].join(";");

    el.innerHTML = `
      <div id="aa-box" style="
        background:#0d0d0d;border:1px solid #2a2a2a;border-radius:10px;
        padding:36px 32px;width:320px;text-align:center;
        box-shadow:0 24px 64px rgba(0,0,0,0.8);
        font-family:'Inter','Sarabun',sans-serif;
      ">
        <div style="font-size:32px;margin-bottom:8px">🔐</div>
        <div id="aa-title" style="font-size:17px;font-weight:700;color:#f0f0f0;margin-bottom:4px">OEE Admin</div>
        <div id="aa-sub" style="font-size:12px;color:#666;margin-bottom:24px">กรอก PIN เพื่อเข้าใช้งาน</div>

        <input id="aa-pin" type="password" inputmode="numeric" maxlength="8"
          placeholder="PIN"
          style="
            width:100%;box-sizing:border-box;
            background:#1a1a1a;border:1px solid #333;border-radius:6px;
            color:#f0f0f0;font-size:22px;letter-spacing:6px;text-align:center;
            padding:12px 16px;outline:none;margin-bottom:16px;
            transition:border-color .2s;
          ">
        <div id="aa-confirm-wrap" style="display:none;margin-bottom:16px">
          <input id="aa-pin2" type="password" inputmode="numeric" maxlength="8"
            placeholder="ยืนยัน PIN"
            style="
              width:100%;box-sizing:border-box;
              background:#1a1a1a;border:1px solid #333;border-radius:6px;
              color:#f0f0f0;font-size:22px;letter-spacing:6px;text-align:center;
              padding:12px 16px;outline:none;
              transition:border-color .2s;
            ">
        </div>

        <button id="aa-btn" style="
          width:100%;padding:12px;border:none;border-radius:6px;
          background:#e31937;color:#fff;font-size:14px;font-weight:700;
          cursor:pointer;transition:background .2s;letter-spacing:.5px;
        ">เข้าสู่ระบบ</button>

        <div id="aa-err" style="
          margin-top:12px;font-size:12px;color:#e31937;
          min-height:18px;
        "></div>
        <div id="aa-setup-link" style="margin-top:16px">
          <button id="aa-toggle" style="
            background:none;border:none;color:#555;font-size:11px;
            cursor:pointer;text-decoration:underline;padding:0;
          ">ยังไม่เคยตั้ง PIN? ตั้งค่าครั้งแรก</button>
        </div>
      </div>`;
    document.body.appendChild(el);

    const pinEl   = el.querySelector("#aa-pin");
    const pin2El  = el.querySelector("#aa-pin2");
    const btnEl   = el.querySelector("#aa-btn");
    const errEl   = el.querySelector("#aa-err");
    const subEl   = el.querySelector("#aa-sub");
    const cfWrap  = el.querySelector("#aa-confirm-wrap");
    const toggleEl= el.querySelector("#aa-toggle");

    let isSetup = false;

    function setError(msg) {
      errEl.textContent = msg;
      pinEl.style.borderColor = msg ? "#e31937" : "#333";
      if (pin2El) pin2El.style.borderColor = msg ? "#e31937" : "#333";
    }

    function setLoading(v) {
      btnEl.disabled = v;
      btnEl.textContent = v ? "กำลังดำเนินการ..." : (isSetup ? "ตั้ง PIN" : "เข้าสู่ระบบ");
    }

    toggleEl.addEventListener("click", () => {
      isSetup = !isSetup;
      cfWrap.style.display = isSetup ? "block" : "none";
      subEl.textContent = isSetup ? "ตั้ง PIN สำหรับใช้ครั้งแรก" : "กรอก PIN เพื่อเข้าใช้งาน";
      btnEl.textContent = isSetup ? "ตั้ง PIN" : "เข้าสู่ระบบ";
      toggleEl.textContent = isSetup ? "มี PIN อยู่แล้ว ลงชื่อเข้าใช้" : "ยังไม่เคยตั้ง PIN? ตั้งค่าครั้งแรก";
      setError("");
      pinEl.value = "";
      pin2El.value = "";
      pinEl.focus();
    });

    async function submit() {
      const pin = pinEl.value.trim();
      if (pin.length < 4) { setError("PIN ต้องมีอย่างน้อย 4 ตัว"); return; }
      if (isSetup) {
        if (pin !== pin2El.value.trim()) { setError("PIN ไม่ตรงกัน"); return; }
      }
      setError(""); setLoading(true);
      try {
        let data;
        if (isSetup) {
          data = await signUp(pin);
          // signUp may return session directly if email confirmation is disabled
          if (!data.access_token) {
            // Try signing in immediately after signup
            data = await signIn(pin);
          }
        } else {
          data = await signIn(pin);
        }
        saveSession(data);
        el.remove();
        if (_onReady) _onReady();
      } catch(e) {
        setError(e.message);
        setLoading(false);
      }
    }

    btnEl.addEventListener("click", submit);
    pinEl.addEventListener("keydown", e => { if (e.key === "Enter") submit(); });
    pin2El.addEventListener("keydown", e => { if (e.key === "Enter") submit(); });

    setTimeout(() => pinEl.focus(), 100);
  }

  // ── Public API ─────────────────────────────────────────────────
  async function require(onReady) {
    _onReady = onReady;
    const ok = await restoreSession();
    if (ok) { onReady(); return; }
    buildModal();
  }

  function isAuthed() {
    if (!_session) return false;
    return _session.expires_at > Date.now();
  }

  function getHeaders() {
    if (!_session) return { "apikey": SUPABASE_ANON, "Authorization": "Bearer " + SUPABASE_ANON };
    return {
      "apikey": SUPABASE_ANON,
      "Authorization": "Bearer " + _session.access_token
    };
  }

  function signOut() {
    clearSession();
    window.location.reload();
  }

  global.AdminAuth = { require, isAuthed, getHeaders, signOut };
})(window);
