const $ = (id) => document.getElementById(id);
function esc(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, (c) => (
    { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]
  ));
}

async function api(method, url, body) {
  const opt = { method, headers: {} };
  if (body) { opt.headers['Content-Type'] = 'application/json'; opt.body = JSON.stringify(body); }
  const res = await fetch(url, opt);
  const out = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(out.error || ('HTTP ' + res.status));
  return out;
}

async function loadUsers() {
  const list = await api('GET', '/api/admin/users');
  const body = $('usersBody');
  body.innerHTML = '';
  let pending = 0;
  list.forEach((u) => {
    if (!u.active) pending++;
    const tr = document.createElement('tr');
    if (!u.active) tr.classList.add('row-pending');
    const name = [u.first, u.last].filter(Boolean).join(' ');
    const acts = [];
    acts.push(u.active
      ? `<button data-act="deactivate" data-id="${u.id}">Deactivate</button>`
      : `<button class="go" data-act="activate" data-id="${u.id}">Activate</button>`);
    acts.push(`<button data-act="promote" data-id="${u.id}"${u.role === 'admin' ? ' disabled' : ''}>Promote</button>`);
    acts.push(`<button data-act="demote" data-id="${u.id}"${u.role === 'user' ? ' disabled' : ''}>Demote</button>`);
    acts.push(`<button data-act="reset-password" data-id="${u.id}">Reset PW</button>`);
    acts.push(`<button class="danger" data-act="delete" data-id="${u.id}">Delete</button>`);
    tr.innerHTML =
      `<td>${esc(u.username)}</td><td>${esc(name)}</td><td>${esc(u.company)}</td><td>${esc(u.title)}</td>` +
      `<td>${esc(u.email)}</td><td>${esc(u.phone)}</td>` +
      `<td><span class="role-tag role-${esc(u.role)}">${esc(u.role)}</span></td>` +
      `<td>${u.active ? '<span class="status-active">Active</span>' : '<span class="status-pending">Pending</span>'}</td>` +
      `<td class="admin-actions">${acts.join('')}</td>`;
    body.appendChild(tr);
  });
  $('pendingBadge').textContent = pending ? `${pending} pending` : '';
}

$('usersBody').addEventListener('click', async (e) => {
  const btn = e.target.closest('button[data-act]');
  if (!btn) return;
  const id = btn.dataset.id, act = btn.dataset.act;
  if (act === 'delete' && !window.confirm('Permanently delete this user? This cannot be undone.')) return;
  if (act === 'demote' && !window.confirm('Demote this user one level?')) return;
  btn.disabled = true;
  try {
    const out = await api('POST', `/api/admin/users/${id}/${act}`);
    if (act === 'reset-password' && out.tempPassword) {
      window.alert('New temporary password:\n\n' + out.tempPassword + '\n\nShare it with the user — they should change it after signing in.');
    }
    await loadUsers();
  } catch (err) { window.alert('Failed: ' + err.message); btn.disabled = false; }
});

$('createForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  const msg = $('createMsg'); msg.textContent = ''; msg.className = 'admin-msg';
  const body = {
    username: $('c-username').value.trim(), password: $('c-password').value,
    role: $('c-role').value, active: $('c-active').checked,
    first: $('c-first').value.trim(), last: $('c-last').value.trim(),
    company: $('c-company').value.trim(), title: $('c-title').value.trim(),
    email: $('c-email').value.trim(), phone: $('c-phone').value.trim(),
  };
  try {
    await api('POST', '/api/admin/users', body);
    $('createForm').reset(); $('c-active').checked = true;
    msg.textContent = 'User created.'; msg.className = 'admin-msg ok';
    await loadUsers();
  } catch (err) { msg.textContent = err.message; msg.className = 'admin-msg err'; }
});

async function loadPages() {
  for (const key of ['privacy', 'terms']) {
    try { const out = await api('GET', '/api/admin/pages/' + key); $('page-' + key).value = out.content || ''; }
    catch (e) { /* ignore */ }
  }
}
document.querySelectorAll('button[data-page]').forEach((btn) => {
  btn.addEventListener('click', async () => {
    const key = btn.dataset.page;
    const msg = $('pagesMsg'); msg.textContent = ''; msg.className = 'admin-msg';
    try {
      await api('POST', '/api/admin/pages/' + key, { content: $('page-' + key).value });
      msg.textContent = (key === 'privacy' ? 'Privacy Policy' : 'Terms of Use') + ' saved.';
      msg.className = 'admin-msg ok';
    } catch (err) { msg.textContent = err.message; msg.className = 'admin-msg err'; }
  });
});

async function loadEmailStatus() {
  try {
    const s = await api('GET', '/api/admin/email-status');
    const badge = $('emailStatus');
    badge.textContent = s.enabled ? 'SMTP configured' : 'SMTP not configured';
    if (s.adminEmail) $('t-to').placeholder = `Recipient (default: ${s.adminEmail})`;
  } catch (e) { /* ignore */ }
}
$('testEmailForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  const msg = $('testEmailMsg'); msg.textContent = 'Sending…'; msg.className = 'admin-msg';
  const btn = e.target.querySelector('button[type="submit"]'); btn.disabled = true;
  try {
    const out = await api('POST', '/api/admin/test-email', { to: $('t-to').value.trim() });
    msg.textContent = `Test email sent to ${out.to}. Check that inbox.`; msg.className = 'admin-msg ok';
  } catch (err) { msg.textContent = 'Failed: ' + err.message; msg.className = 'admin-msg err'; }
  btn.disabled = false;
});

// ---- Sitemap Monitor (folded in from the old /monitor tab) ----
const monSetStatus = (msg, isErr) => { const el = $('monStatus'); if (!el) return; el.textContent = msg; el.classList.toggle('err', !!isErr); };
const monFmtTime = (t) => { if (!t) return '—'; const d = new Date(t); return isNaN(d) ? esc(t) : d.toLocaleString(); };
const monEventPill = (e) => ({ new_bio: '<span class="pill new">new hire</span>', departed: '<span class="pill dep">departed</span>',
  reappeared: '<span class="pill rea">reappeared</span>' }[e] || esc(e));

async function monLoadStats() {
  try {
    const d = await (await fetch('/api/monitor')).json();
    const s = d.stats || {}, obs = s.observations || {};
    $('monStats').innerHTML = [
      `Watches: <b>${s.watches || 0}</b> (${s.activeWatches || 0} active)`,
      `Bios tracked: <b>${s.present || 0}</b>`, `Departed: <b>${s.departed || 0}</b>`,
      `New hires seen: <b>${obs.new_bio || 0}</b>`, `Departures seen: <b>${obs.departed || 0}</b>`,
      `Nightly pass: <b>${d.enabled ? 'ON (' + d.intervalHours + 'h)' : 'OFF'}</b>`,
      `Last pass: <b>${monFmtTime(s.lastPass)}</b>`,
    ].map((x) => `<span>${x}</span>`).join('');
    $('monBadge').textContent = d.enabled ? '' : 'nightly pass OFF';
    if (!d.enabled) monSetStatus('The nightly pass is OFF on the server (set MONITOR_ENABLED=1). You can still run passes manually below.', false);
    else monSetStatus(d.running ? 'A monitoring pass is running…' : 'Ready.', false);
  } catch (e) { monSetStatus('Could not load status: ' + e.message, true); }
}

async function monLoadWatches() {
  try {
    const d = await (await fetch('/api/monitor/watches')).json();
    const ws = d.watches || [];
    $('monWatches').innerHTML = ws.length ? ws.map((w) => `
      <tr><td>${esc(w.domain || '')}</td>
        <td class="u"><a href="${esc(w.sitemap_url)}" target="_blank" rel="noopener">${esc(w.sitemap_url)}</a></td>
        <td><b>${w.present_count || 0}</b>${w.departed_count ? ` / <span style="color:#991b1b">${w.departed_count}</span>` : ''}</td>
        <td>${w.bio_ratio == null ? '—' : (w.bio_ratio * 100).toFixed(0) + '%'}</td>
        <td>${monFmtTime(w.last_fetched)}</td>
        <td><span class="pill ${w.status === 'paused' ? 'paused' : 'active'}">${esc(w.status || 'active')}</span></td>
        <td><button class="linklike" data-mon="toggle" data-sm="${esc(w.sitemap_url)}" data-status="${w.status === 'paused' ? 'active' : 'paused'}">${w.status === 'paused' ? 'resume' : 'pause'}</button>
            <button class="linklike" data-mon="unwatch" data-sm="${esc(w.sitemap_url)}" style="color:#b91c1c">remove</button></td>
      </tr>`).join('') : `<tr><td colspan="7" class="mon-empty">No watches yet — add a domain above.</td></tr>`;
  } catch (e) { /* keep last */ }
}

async function monLoadChanges() {
  try {
    const ev = $('monEventFilter').value;
    const d = await (await fetch('/api/monitor/changes?limit=300' + (ev ? '&event=' + encodeURIComponent(ev) : ''))).json();
    const rows = d.changes || [];
    $('monChanges').innerHTML = rows.length ? rows.map((c) => `
      <tr><td>${monFmtTime(c.ts)}</td><td>${monEventPill(c.event)}</td><td>${esc(c.domain || '')}</td>
        <td class="url"><a href="${esc(c.url)}" target="_blank" rel="noopener">${esc(c.url)}</a></td></tr>`).join('')
      : `<tr><td colspan="4" class="mon-empty">No changes yet — they appear after a pass detects new/departed bios.</td></tr>`;
  } catch (e) { /* keep last */ }
}

function monRefresh() { if ($('monStatus')) { monLoadStats(); monLoadWatches(); monLoadChanges(); } }

async function monAddWatch() {
  const lines = $('monInput').value.split(/[\n,]/).map((s) => s.trim()).filter(Boolean);
  if (!lines.length) { monSetStatus('Enter at least one domain or sitemap URL.', true); return; }
  const domains = [], sitemaps = [];
  for (const l of lines) (/\.xml(\.gz)?($|\?)/i.test(l) || /\/sitemap/i.test(l)) ? sitemaps.push(l) : domains.push(l);
  $('monAdd').disabled = true; monSetStatus('Discovering bio-dedicated sitemaps…');
  try {
    const d = await api('POST', '/api/monitor/watch', { domains, sitemaps });
    monSetStatus(`Added ${d.added} watch(es).` + (d.added ? '' : ' No bio-dedicated child sitemaps found for that input.'));
    if (d.added) $('monInput').value = '';
  } catch (e) { monSetStatus('Failed: ' + e.message, true); }
  $('monAdd').disabled = false; monRefresh();
}

async function monRunPass(force) {
  $('monRun').disabled = $('monRunForce').disabled = true;
  monSetStatus('Running a monitoring pass…');
  try {
    const d = await api('POST', '/api/monitor/run', { force: !!force });
    if (d.summary && d.summary.skipped === true) monSetStatus(d.summary.reason || 'Already running.', false);
    else { const s = d.summary || {}; monSetStatus(`Pass done — scanned ${s.scanned}, skipped ${s.skipped}, new ${s.newBios}, departed ${s.departed}` + (s.extracted ? `, ${s.extracted} queued for extraction` : '') + '.'); }
  } catch (e) { monSetStatus('Pass failed: ' + e.message, true); }
  $('monRun').disabled = $('monRunForce').disabled = false; monRefresh();
}

if ($('monWatches')) {
  $('monWatches').addEventListener('click', async (e) => {
    const btn = e.target.closest('button[data-mon]'); if (!btn) return;
    const sm = btn.dataset.sm;
    try {
      if (btn.dataset.mon === 'toggle') await api('POST', '/api/monitor/toggle', { sitemapUrl: sm, status: btn.dataset.status });
      else if (btn.dataset.mon === 'unwatch') { if (!window.confirm('Stop watching this sitemap and drop its baseline?')) return; await api('POST', '/api/monitor/unwatch', { sitemapUrl: sm }); }
    } catch (err) { window.alert('Failed: ' + err.message); }
    monRefresh();
  });
  $('monAdd').addEventListener('click', monAddWatch);
  $('monRun').addEventListener('click', () => monRunPass(false));
  $('monRunForce').addEventListener('click', () => monRunPass(true));
  $('monEventFilter').addEventListener('change', monLoadChanges);
}

loadUsers();
loadPages();
loadEmailStatus();
monRefresh();
