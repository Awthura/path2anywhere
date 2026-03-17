// auth.js — Path2Anywhere shared auth + freemium usage module
// Requires: @supabase/supabase-js v2 loaded via CDN before this script
(function () {
  const SUPABASE_URL      = '__SUPABASE_URL__';
  const SUPABASE_ANON_KEY = '__SUPABASE_ANON_KEY__';
  const ANON_USAGE_KEY   = 'p2a_daily_usage';
  const ANON_LIMIT       = 5;
  const FREE_LIMIT       = 20;

  const sb = window.supabase.createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

  let _session     = null;
  let _plan        = 'free';
  let _initialized = false;

  // ── Init ────────────────────────────────────────────────────────────────────
  async function init() {
    // Handle OAuth callback errors — clean the URL and surface a friendly message
    const urlParams = new URLSearchParams(window.location.search);
    const oauthError = urlParams.get('error_code');
    if (oauthError) {
      // Strip the error query params and hash fragment from the URL
      window.history.replaceState({}, '', window.location.pathname);
      // Show a toast after the modal is injected (deferred below)
      setTimeout(() => _showToast('Sign-in failed. Please try again.'), 100);
    }

    const { data: { session } } = await sb.auth.getSession();
    _session = session;
    if (session) await _loadPlan();

    sb.auth.onAuthStateChange(async (event, session) => {
      const wasSignedIn = !!_session;
      _session = session;
      _plan    = 'free';
      if (session) await _loadPlan();
      _updateHeaderUI();

      // Clean up OAuth hash tokens from the URL
      if (event === 'SIGNED_IN' && window.location.hash.includes('access_token')) {
        window.history.replaceState({}, '', window.location.pathname);
      }

      // Show welcome toast on new sign-ins (not on page-load session restore)
      if (_initialized && event === 'SIGNED_IN' && !wasSignedIn && session) {
        const name = session.user.user_metadata?.full_name || session.user.email || '';
        _showToast(`Signed in as ${name}`, 'success');
        closeAuthModal();
      }
      _initialized = true;
    });

    _injectAuthModal();
    _injectPaywallModal();
    _injectHeaderSlot();
    _updateHeaderUI();
  }

  function _showToast(message, type = 'error') {
    const existing = document.getElementById('p2a-toast');
    if (existing) existing.remove();
    const toast = document.createElement('div');
    toast.id = 'p2a-toast';
    toast.textContent = message;
    const isSuccess = type === 'success';
    toast.style.cssText = `position:fixed;bottom:24px;left:50%;transform:translateX(-50%);background:${isSuccess ? '#0f2e1c' : '#3b1c1c'};border:1px solid ${isSuccess ? '#166534' : '#7f1d1d'};color:${isSuccess ? '#86efac' : '#fca5a5'};padding:10px 20px;border-radius:8px;font-size:14px;z-index:99999;box-shadow:0 4px 12px rgba(0,0,0,.4);white-space:nowrap`;
    document.body.appendChild(toast);
    setTimeout(() => toast.remove(), 4000);
  }

  async function _loadPlan() {
    const { data } = await sb
      .from('user_subscriptions')
      .select('plan, valid_until')
      .eq('user_id', _session.user.id)
      .single();

    if (data && data.plan === 'pro') {
      const stillValid = !data.valid_until || new Date(data.valid_until) > new Date();
      _plan = stillValid ? 'pro' : 'free';
    } else {
      _plan = 'free';
    }
  }

  // ── Usage checking ───────────────────────────────────────────────────────────
  async function checkAndIncrementUsage() {
    // Pro users: always allowed
    if (_session && _plan === 'pro') return true;

    // Signed-in free users: Supabase server-side count
    if (_session) {
      const today = new Date().toISOString().slice(0, 10); // YYYY-MM-DD

      const { data } = await sb
        .from('user_usage')
        .select('count')
        .eq('user_id', _session.user.id)
        .eq('date', today)
        .single();

      const currentCount = data?.count || 0;

      if (currentCount >= FREE_LIMIT) {
        _showPaywall('free');
        return false;
      }

      await sb.from('user_usage').upsert(
        { user_id: _session.user.id, date: today, count: currentCount + 1 },
        { onConflict: 'user_id,date' }
      );

      return true;
    }

    // Anonymous: localStorage, 5/day
    const stored = (() => {
      try { return JSON.parse(localStorage.getItem(ANON_USAGE_KEY) || '{}'); }
      catch { return {}; }
    })();
    const today = new Date().toDateString();
    const count = stored.date === today ? (stored.count || 0) : 0;

    if (count >= ANON_LIMIT) {
      _showPaywall('anon');
      return false;
    }

    localStorage.setItem(ANON_USAGE_KEY, JSON.stringify({ date: today, count: count + 1 }));
    return true;
  }

  // ── Auth actions ─────────────────────────────────────────────────────────────
  async function _signInWithEmail(email, password) {
    const { error } = await sb.auth.signInWithPassword({ email, password });
    return error;
  }

  async function _signUpWithEmail(email, password) {
    const { data, error } = await sb.auth.signUp({ email, password });
    return { error, session: data?.session };
  }

  async function _signInWithGoogle() {
    await sb.auth.signInWithOAuth({
      provider: 'google',
      options: { redirectTo: 'https://path2anywhere.com' },
    });
  }

  async function signOut() {
    await sb.auth.signOut();
    window.location.reload();
  }

  // ── Auth modal ───────────────────────────────────────────────────────────────
  function openAuthModal(defaultTab = 'signin') {
    document.getElementById('p2a-auth-modal')?.classList.add('visible');
    _switchTab(defaultTab);
  }

  function closeAuthModal() {
    document.getElementById('p2a-auth-modal')?.classList.remove('visible');
    _clearAuthError();
  }

  function _switchTab(tab) {
    document.querySelectorAll('.p2a-tab').forEach(t =>
      t.classList.toggle('active', t.dataset.tab === tab)
    );
    document.getElementById('p2a-tab-signin').style.display = tab === 'signin' ? 'block' : 'none';
    document.getElementById('p2a-tab-signup').style.display = tab === 'signup'  ? 'block' : 'none';
  }

  function _setAuthError(msg) {
    const el = document.getElementById('p2a-auth-error');
    if (el) { el.textContent = msg; el.style.display = 'block'; }
  }

  function _clearAuthError() {
    const el = document.getElementById('p2a-auth-error');
    if (el) { el.textContent = ''; el.style.display = 'none'; }
  }

  async function _handleSignIn() {
    _clearAuthError();
    const email    = document.getElementById('p2a-signin-email').value.trim();
    const password = document.getElementById('p2a-signin-password').value;
    if (!email || !password) return _setAuthError('Please enter your email and password.');
    const btn = document.getElementById('p2a-signin-btn');
    btn.disabled = true; btn.textContent = 'Signing in…';
    const error = await _signInWithEmail(email, password);
    btn.disabled = false; btn.textContent = 'Continue with email';
    if (error) return _setAuthError(error.message);
    closeAuthModal();
  }

  async function _handleSignUp() {
    _clearAuthError();
    const email    = document.getElementById('p2a-signup-email').value.trim();
    const password = document.getElementById('p2a-signup-password').value;
    if (!email || !password) return _setAuthError('Please enter your email and password.');
    if (password.length < 8) return _setAuthError('Password must be at least 8 characters.');
    const btn = document.getElementById('p2a-signup-btn');
    btn.disabled = true; btn.textContent = 'Creating account…';
    const { error, session } = await _signUpWithEmail(email, password);
    btn.disabled = false; btn.textContent = 'Create account';
    if (error) return _setAuthError(error.message);

    if (session) {
      // Email confirmation disabled — user is signed in immediately
      closeAuthModal();
    } else {
      // Email confirmation enabled — show check-your-email screen
      document.getElementById('p2a-auth-form-area').innerHTML = `
        <div style="text-align:center;padding:16px 0">
          <div style="font-size:32px;margin-bottom:12px">✉️</div>
          <p style="color:#e2e8f0;font-weight:600;margin-bottom:8px">Check your email</p>
          <p style="color:#94a3b8;font-size:14px">We sent a confirmation link to <strong>${email}</strong>. Click it to activate your account.</p>
        </div>`;
    }
  }

  function _injectAuthModal() {
    if (document.getElementById('p2a-auth-modal')) return;

    const style = document.createElement('style');
    style.textContent = `
      #p2a-auth-modal{display:none;position:fixed;inset:0;background:rgba(0,0,0,.75);z-index:10000;align-items:center;justify-content:center}
      #p2a-auth-modal.visible{display:flex}
      #p2a-auth-box{background:#1a1d27;border:1px solid #2a2d3a;border-radius:14px;padding:32px 28px;max-width:400px;width:90%}
      #p2a-auth-box h2{font-size:20px;font-weight:700;color:#e2e8f0;margin:0 0 20px}
      .p2a-tabs{display:flex;gap:4px;margin-bottom:20px;background:#0f1117;border-radius:8px;padding:4px}
      .p2a-tab{flex:1;padding:7px;border:none;border-radius:6px;background:transparent;color:#64748b;font-size:14px;font-weight:500;cursor:pointer}
      .p2a-tab.active{background:#2a2d3a;color:#e2e8f0}
      .p2a-input{width:100%;background:#0f1117;border:1px solid #2a2d3a;border-radius:8px;padding:10px 12px;color:#e2e8f0;font-size:14px;margin-bottom:10px;box-sizing:border-box}
      .p2a-input:focus{outline:none;border-color:#4f8ef7}
      .p2a-btn-primary{width:100%;background:#4f8ef7;color:#fff;border:none;border-radius:8px;padding:11px;font-size:15px;font-weight:600;cursor:pointer;margin-bottom:12px}
      .p2a-btn-primary:hover{background:#3a7de0}
      .p2a-btn-primary:disabled{opacity:.6;cursor:default}
      .p2a-divider{display:flex;align-items:center;gap:10px;margin-bottom:12px;color:#64748b;font-size:13px}
      .p2a-divider::before,.p2a-divider::after{content:'';flex:1;height:1px;background:#2a2d3a}
      .p2a-btn-google{width:100%;background:#fff;color:#333;border:1px solid #dadce0;border-radius:8px;padding:10px;font-size:14px;font-weight:500;cursor:pointer;display:flex;align-items:center;justify-content:center;gap:10px;margin-bottom:16px}
      .p2a-btn-google:hover{background:#f8f8f8}
      .p2a-security-note{font-size:11px;color:#64748b;text-align:center;line-height:1.5;margin:0}
      #p2a-auth-error{display:none;background:#3b1c1c;border:1px solid #7f1d1d;border-radius:6px;padding:8px 12px;color:#fca5a5;font-size:13px;margin-bottom:12px}
      .p2a-modal-close{float:right;background:none;border:none;color:#64748b;font-size:20px;cursor:pointer;padding:0;line-height:1;margin-top:-4px}
      .p2a-modal-close:hover{color:#e2e8f0}
      #p2a-auth-header{margin-left:auto;display:flex;align-items:center}
      .p2a-signin-link{background:none;border:1px solid #2a2d3a;border-radius:6px;color:#e2e8f0;font-size:13px;padding:5px 14px;cursor:pointer}
      .p2a-signin-link:hover{border-color:#4f8ef7;color:#4f8ef7}
      .p2a-user-menu{display:flex;align-items:center;gap:10px}
      .p2a-avatar{width:28px;height:28px;border-radius:50%;background:#4f8ef7;color:#fff;font-size:13px;font-weight:600;display:flex;align-items:center;justify-content:center;cursor:default}
      .p2a-signout-btn{background:none;border:none;color:#64748b;font-size:13px;cursor:pointer;padding:0}
      .p2a-signout-btn:hover{color:#e2e8f0}
    `;
    document.head.appendChild(style);

    const modal = document.createElement('div');
    modal.id = 'p2a-auth-modal';
    modal.innerHTML = `
      <div id="p2a-auth-box">
        <button class="p2a-modal-close" onclick="P2AAuth.closeAuthModal()">×</button>
        <h2>Sign in to Path2Anywhere</h2>
        <div class="p2a-tabs">
          <button class="p2a-tab active" data-tab="signin" onclick="P2AAuth._switchTab('signin')">Sign in</button>
          <button class="p2a-tab" data-tab="signup" onclick="P2AAuth._switchTab('signup')">Create account</button>
        </div>
        <div id="p2a-auth-error"></div>
        <div id="p2a-auth-form-area">
          <div id="p2a-tab-signin">
            <input id="p2a-signin-email" class="p2a-input" type="email" placeholder="Email" autocomplete="email" />
            <input id="p2a-signin-password" class="p2a-input" type="password" placeholder="Password" autocomplete="current-password" />
            <button id="p2a-signin-btn" class="p2a-btn-primary" onclick="P2AAuth._handleSignIn()">Continue with email</button>
          </div>
          <div id="p2a-tab-signup" style="display:none">
            <input id="p2a-signup-email" class="p2a-input" type="email" placeholder="Email" autocomplete="email" />
            <input id="p2a-signup-password" class="p2a-input" type="password" placeholder="Password (min 8 characters)" autocomplete="new-password" />
            <button id="p2a-signup-btn" class="p2a-btn-primary" onclick="P2AAuth._handleSignUp()">Create account</button>
          </div>
          <div class="p2a-divider">or</div>
          <button class="p2a-btn-google" onclick="P2AAuth._signInWithGoogle()">
            <svg width="18" height="18" viewBox="0 0 18 18"><path fill="#4285F4" d="M17.64 9.2c0-.637-.057-1.251-.164-1.84H9v3.481h4.844c-.209 1.125-.843 2.078-1.796 2.717v2.258h2.908c1.702-1.567 2.684-3.874 2.684-6.615z"/><path fill="#34A853" d="M9 18c2.43 0 4.467-.806 5.956-2.184l-2.908-2.258c-.806.54-1.837.86-3.048.86-2.344 0-4.328-1.584-5.036-3.711H.957v2.332A8.997 8.997 0 0 0 9 18z"/><path fill="#FBBC05" d="M3.964 10.707A5.41 5.41 0 0 1 3.682 9c0-.593.102-1.17.282-1.707V4.961H.957A8.996 8.996 0 0 0 0 9c0 1.452.348 2.827.957 4.039l3.007-2.332z"/><path fill="#EA4335" d="M9 3.58c1.321 0 2.508.454 3.44 1.345l2.582-2.58C13.463.891 11.426 0 9 0A8.997 8.997 0 0 0 .957 4.961L3.964 6.293C4.672 4.166 6.656 3.58 9 3.58z"/></svg>
            Continue with Google
          </button>
          <p class="p2a-security-note">Passwords encrypted with bcrypt — never stored in plain text.</p>
        </div>
      </div>`;
    document.body.appendChild(modal);
    modal.addEventListener('click', e => { if (e.target === modal) closeAuthModal(); });
  }

  // ── Paywall modal ────────────────────────────────────────────────────────────
  function _showPaywall(tier) {
    const modal = document.getElementById('p2a-paywall-modal');
    if (!modal) return;
    const eyebrow = modal.querySelector('.pw-eyebrow');
    const heading = modal.querySelector('.pw-heading');
    const body    = modal.querySelector('.pw-body');
    const cta     = modal.querySelector('.pw-cta');

    if (tier === 'anon') {
      eyebrow.textContent = 'Daily limit reached';
      heading.textContent = "You've used your 5 free uses today";
      body.textContent    = 'Sign up free to get 20 uses per day — no credit card required.';
      cta.innerHTML = `
        <button class="pw-btn" onclick="P2AAuth.closePaywall();P2AAuth.openAuthModal('signup')">Sign up free</button>
        <button class="pw-btn-secondary" onclick="P2AAuth.closePaywall()">Maybe later</button>`;
    } else {
      eyebrow.textContent = 'Daily limit reached';
      heading.textContent = "You've used your 20 free uses today";
      body.textContent    = 'Upgrade to Pro for unlimited routes, saved trips, and more.';
      cta.innerHTML = `
        <button class="pw-btn" onclick="P2AAuth.closePaywall();P2AAuth.openAuthModal('signup')">Upgrade to Pro</button>
        <button class="pw-btn-secondary" onclick="P2AAuth.closePaywall()">Maybe later</button>`;
    }
    modal.classList.add('visible');
  }

  function closePaywall() {
    document.getElementById('p2a-paywall-modal')?.classList.remove('visible');
  }

  function _injectPaywallModal() {
    if (document.getElementById('p2a-paywall-modal')) return;

    const style = document.createElement('style');
    style.textContent = `
      #p2a-paywall-modal{display:none;position:fixed;inset:0;background:rgba(0,0,0,.75);z-index:9999;align-items:center;justify-content:center}
      #p2a-paywall-modal.visible{display:flex}
      #p2a-paywall-box{background:#1a1d27;border:1px solid #2a2d3a;border-radius:12px;padding:32px 28px;max-width:420px;width:90%;text-align:center}
      .pw-eyebrow{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.12em;color:#a78bfa;margin-bottom:12px}
      .pw-heading{font-size:22px;font-weight:700;color:#e2e8f0;margin-bottom:12px;line-height:1.3}
      .pw-body{font-size:14px;color:#94a3b8;line-height:1.6;margin-bottom:24px}
      .pw-cta{display:flex;flex-direction:column;gap:10px;align-items:center}
      .pw-btn{background:#4f8ef7;color:#fff;border:none;border-radius:8px;padding:10px 28px;font-size:15px;font-weight:600;cursor:pointer;width:100%}
      .pw-btn:hover{background:#3a7de0}
      .pw-btn-secondary{background:none;border:none;color:#64748b;font-size:13px;cursor:pointer;padding:4px}
      .pw-btn-secondary:hover{color:#e2e8f0}
    `;
    document.head.appendChild(style);

    const modal = document.createElement('div');
    modal.id = 'p2a-paywall-modal';
    modal.innerHTML = `
      <div id="p2a-paywall-box">
        <p class="pw-eyebrow"></p>
        <h2 class="pw-heading"></h2>
        <p class="pw-body"></p>
        <div class="pw-cta"></div>
      </div>`;
    document.body.appendChild(modal);
  }

  // ── Header slot ──────────────────────────────────────────────────────────────
  function _injectHeaderSlot() {
    const header = document.querySelector('header');
    if (!header || document.getElementById('p2a-auth-header')) return;
    const slot = document.createElement('div');
    slot.id = 'p2a-auth-header';
    header.appendChild(slot);
  }

  function _updateHeaderUI() {
    const el = document.getElementById('p2a-auth-header');
    if (!el) return;
    if (_session) {
      const email   = _session.user.email || '';
      const initial = email.charAt(0).toUpperCase();
      el.innerHTML = `
        <div class="p2a-user-menu">
          <div class="p2a-avatar" title="${email}">${initial}</div>
          <button class="p2a-signout-btn" onclick="P2AAuth.signOut()">Sign out</button>
        </div>`;
    } else {
      el.innerHTML = `<button class="p2a-signin-link" onclick="P2AAuth.openAuthModal()">Sign in</button>`;
    }
  }

  // ── Public API ───────────────────────────────────────────────────────────────
  window.P2AAuth = {
    checkAndIncrementUsage,
    openAuthModal,
    closeAuthModal,
    closePaywall,
    signOut,
    _switchTab,
    _handleSignIn,
    _handleSignUp,
    _signInWithGoogle,
  };

  // Auto-init
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
