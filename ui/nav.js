// nav.js — the single source of truth for the top navigation on every page.
// Renders the full tab set (role-gated), marks the active tab from the URL, and appends the
// signed-in user chip + Log out. Each page just needs an empty <nav class="nav-actions"></nav>;
// this replaces its contents so the nav is identical everywhere.
(async function () {
  let me = null;
  try {
    const res = await fetch('/api/auth/me');
    if (res.ok) me = await res.json();
  } catch (e) { /* ignore */ }
  if (!me) { window.location.href = '/login'; return; }

  document.body.dataset.role = me.role;
  const rank = ({ user: 0, analyst: 1, admin: 2 })[me.role] || 0;

  const nav = document.querySelector('.nav-actions');
  if (!nav) return;

  const path = (location.pathname.replace(/\/+$/, '') || '/');
  const view = new URLSearchParams(location.search).get('view');

  // Canonical tab set, in order. min = lowest role rank that sees the tab.
  const TABS = [
    { href: '/search',          label: '🔍 Contact Crawler', min: 0, green: true, active: (p) => p === '/search' || p === '/' },
    { href: '/company-crawler', label: '🏢 Company Crawler', min: 1, green: true, active: (p) => p === '/company-crawler' },
    { href: '/site-search',     label: '🌐 Site Search',    min: 1, green: true, active: (p) => p === '/site-search' },
    { href: '/serp-lookup',     label: '🔎 SERP Look Up',   min: 1, green: true, active: (p) => p === '/serp-lookup' },
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
