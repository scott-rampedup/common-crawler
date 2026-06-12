// nav.js — shared top-nav setup on the Master DB + Search pages.
// Shows the signed-in user + Log out, adds the Admin link for admins, hides the Master
// Database link for plain 'user' accounts, and tags <body data-role> so role-specific UI
// (e.g. Edit/Delete/AI buttons) can hide itself via CSS.
(async function () {
  let me = null;
  try {
    const res = await fetch('/api/auth/me');
    if (res.ok) me = await res.json();
  } catch (e) { /* ignore */ }
  if (!me) { window.location.href = '/login'; return; }

  document.body.dataset.role = me.role;
  const rank = ({ user: 0, analyst: 1, admin: 2 })[me.role] || 0;

  const nav = document.querySelector('.nav-actions') || document.querySelector('.actions');
  if (!nav) return;

  // Plain users can't see the Master Database.
  if (rank < 1) {
    const master = nav.querySelector('#dbButton') || nav.querySelector('a[href^="/?"]') || nav.querySelector('a[href="/"]');
    if (master) master.style.display = 'none';
  }

  // Admin link for admins.
  if (rank >= 2 && !nav.querySelector('a[href="/admin"]')) {
    const a = document.createElement('a');
    a.className = 'nav-link';
    a.href = '/admin';
    a.textContent = '⚙ Admin';
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
