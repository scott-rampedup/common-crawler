// nav.js — the single source of truth for the top navigation on every page.
// Renders the full tab set (role-gated), marks the active tab from the URL, and appends the
// signed-in user chip + Log out. Each page just needs an empty <nav class="nav-actions"></nav>;
// this replaces its contents so the nav is identical everywhere.
(async function () {
  // Only a DEFINITIVE 401 means signed-out. A transient failure (app cold-starting after a deploy, a
  // 502/503 from the proxy, a network blip) must NOT log the user out — that was bouncing people to
  // /login mid-navigation. Retry a couple of times to ride out a cold start, then give up quietly.
  async function fetchMe(tries) {
    for (let i = 0; i < tries; i++) {
      try {
        const res = await fetch('/api/auth/me', { cache: 'no-store' });
        if (res.status === 401) return { signedOut: true };
        if (res.ok) return { me: await res.json() };
      } catch (e) { /* transient — retry */ }
      if (i < tries - 1) await new Promise((r) => setTimeout(r, 400 * (i + 1)));
    }
    return {};   // transient failure after retries: leave the page as-is
  }
  const r = await fetchMe(3);
  if (r.signedOut) { window.location.href = '/login'; return; }
  const me = r.me;
  if (!me) return;   // couldn't confirm the session (transient) — render nothing, don't bounce

  document.body.dataset.role = me.role;
  const rank = ({ user: 0, analyst: 1, admin: 2 })[me.role] || 0;

  const nav = document.querySelector('.nav-actions');
  if (!nav) return;

  const path = (location.pathname.replace(/\/+$/, '') || '/');
  const view = new URLSearchParams(location.search).get('view');

  // Canonical tab set, in order. min = lowest role rank that sees the tab.
  const TABS = [
    { href: '/search',          label: '👤 Contact Crawler', min: 0, green: true, active: (p) => p === '/search' },
    { href: '/company-crawler', label: '🏢 Company Crawler', min: 0, green: true, active: (p) => p === '/company-crawler' },
    { href: '/sitemaps',        label: '🗺 Sitemap Library', min: 1, green: true, active: (p) => p === '/sitemaps' },
    { href: '/corporate-places',label: '🏬 Corporate Places',min: 0, green: true, active: (p) => p === '/corporate-places' },
    { href: '/atp-library',     label: '📍 All The Places Library', min: 2, green: true, active: (p) => p === '/atp-library' },
    { href: '/site-search',     label: '🌐 Site Search',    min: 1, green: true, active: (p) => p === '/site-search' },
    { href: '/serp-lookup',     label: '🔎 SERP Look Up',   min: 1, green: true, active: (p) => p === '/serp-lookup' },
    { href: '/?view=db',        label: '📥 Data Importer',  min: 1, active: (p, v) => p === '/' && v === 'db' },
    { href: '/admin',           label: '⚙ Admin',           min: 2, active: (p) => p === '/admin' },
  ];

  nav.innerHTML = '';
  for (const t of TABS) {
    if (rank < t.min) continue;
    const on = t.active(path, view);
    const a = document.createElement('a');
    a.className = 'nav-link' + (t.green ? ' green' : '') + (on ? ' active' : '');
    a.href = t.href;
    a.textContent = t.label;
    if (on) a.setAttribute('aria-current', 'page');
    nav.appendChild(a);
  }

  const chip = document.createElement('span');
  chip.className = 'user-chip';
  chip.textContent = me.username + ' · ' + me.role;
  nav.appendChild(chip);

  const out = document.createElement('button');
  out.type = 'button';
  out.className = 'nav-logout';
  out.textContent = 'Log out';
  out.addEventListener('click', async () => {
    try { await fetch('/api/auth/logout', { method: 'POST' }); } catch (e) { /* ignore */ }
    window.location.href = '/login';
  });
  nav.appendChild(out);
})();
