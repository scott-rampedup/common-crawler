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

loadUsers();
loadPages();
