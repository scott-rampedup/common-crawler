// Search Database page — queries the central (master) contacts DB server-side, with
// row checkboxes (cross-page selection), sortable columns, and CSV download.

// Full CSV schema (matches the engine / main-page download).
const CSV_COLUMNS = [
  'Time Stamp', 'Source', 'Web Source URL', 'Directory', 'Path ID', 'Domain', 'Last Path',
  'Bio Check', 'First', 'Last', 'Gender', 'Title', 'Position', 'Description', 'Email Address',
  'Email Type', 'LinkedIn URL', 'Google Maps', 'Phone', 'Phone Type', 'Phone Location',
  'Phone 2', 'Phone 2 Type', 'Type'
];

// On-screen columns, in display order. Each: key (record field), label (header text),
// type (how the cell renders), sortable (whether the header sorts server-side).
const COLUMNS = [
  { key: 'Image URL',      label: 'Image',         type: 'image',    sortable: false },
  { key: 'Last Path',      label: 'Last Path',     type: 'lastpath', sortable: true  },
  { key: 'Position',       label: 'Position',      type: 'position', sortable: true  },
  { key: 'Domain',         label: 'Domain',        type: 'domain',   sortable: true  },
  { key: 'Email Address',  label: 'Email Address', type: 'text',     sortable: true  },
  { key: 'Email Type',     label: 'Type',          type: 'text',     sortable: true  },
  { key: 'LinkedIn URL',   label: 'LinkedIn',      type: 'linkedin', sortable: false },
  { key: 'Google Maps',    label: 'Google Maps',   type: 'maps',     sortable: false },
  { key: 'Phone Location', label: 'Location',      type: 'location', sortable: true  },
  { key: 'Phone',          label: 'Phone',         type: 'text',     sortable: true  },
  { key: 'Phone Type',     label: 'Type',          type: 'text',     sortable: true  },
  { key: 'Phone 2',        label: 'Phone 2',       type: 'text',     sortable: true  },
  { key: 'Phone 2 Type',   label: 'Type',          type: 'text',     sortable: true  },
  { key: 'Type',           label: 'Domain Type',   type: 'text',     sortable: true  },
];

// Fields the manual Edit modal lets you change for each selected record.
const EDIT_FIELDS = [
  { key: 'First',          label: 'First Name' },
  { key: 'Last',           label: 'Last Name' },
  { key: 'Position',       label: 'Position (title)' },
  { key: 'Title',          label: 'Title' },
  { key: 'Email Address',  label: 'Email Address' },
  { key: 'Email Type',     label: 'Email Type' },
  { key: 'Phone',          label: 'Phone' },
  { key: 'Phone Type',     label: 'Phone Type' },
  { key: 'Phone Location', label: 'Phone Location' },
  { key: 'LinkedIn URL',   label: 'LinkedIn URL' },
  { key: 'Domain',         label: 'Domain' },
  { key: 'Description',    label: 'Description' },
];

const PAGE_SIZE = 50;

const PIN_SVG = '<svg viewBox="0 0 24 24" width="18" height="18" aria-hidden="true">'
  + '<path fill="currentColor" d="M12 2C8.13 2 5 5.13 5 9c0 5.25 7 13 7 13s7-7.75 7-13c0-3.87-3.13-7-7-7zm0 9.5A2.5 2.5 0 1 1 12 6a2.5 2.5 0 0 1 0 5.5z"/></svg>';

// LinkedIn "in" glyph for the LinkedIn column header + cells.
const LINKEDIN_SVG = '<svg viewBox="0 0 24 24" width="18" height="18" aria-hidden="true">'
  + '<path fill="currentColor" d="M19 3a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h14zM8.34 9.67H5.67V18h2.67V9.67zM7 5.67a1.55 1.55 0 1 0 0 3.1 1.55 1.55 0 0 0 0-3.1zM18.33 18v-4.57c0-2.45-1.31-3.59-3.06-3.59-1.41 0-2.04.78-2.39 1.33v-1.5h-2.67V18h2.67v-4.65c0-.25.02-.49.09-.67.2-.49.65-1 1.4-1 .99 0 1.38.75 1.38 1.85V18h2.67z"/></svg>';

const state = {
  rows: [],                  // current page of records
  total: 0,                  // server-reported match count
  page: 1,
  sort: { column: null, dir: 1 },   // dir: 1 asc, -1 desc
  selected: new Map(),       // email(lower) -> record, persists across pages
};

let searchTimer = null;
let allCheck = null;
const el = {};

function $(id) { return document.getElementById(id); }
function norm(v) { return String(v || '').trim().toLowerCase(); }

function escapeHtml(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, (c) => (
    { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]
  ));
}

function csvCell(v) {
  const val = String(v == null ? '' : v);
  return (val.includes(',') || val.includes('"') || val.includes('\n'))
    ? `"${val.replace(/"/g, '""')}"`
    : val;
}

function parseHostname(value) {
  const text = String(value || '').trim();
  if (!text) return '';
  try {
    return new URL(text).hostname.replace(/^www\./i, '').toLowerCase();
  } catch (e) {
    return text.replace(/^https?:\/\//i, '').split('/')[0].split('?')[0].split('#')[0]
      .replace(/^www\./i, '').toLowerCase();
  }
}

function parseDomains(text) {
  return Array.from(new Set(
    String(text || '').split(/[\r\n,]+/).map(parseHostname).filter(Boolean)
  ));
}

function initElements() {
  el.fSearch = $('f-search');
  el.fDirectory = $('f-directory');
  el.fDomains = $('f-domains');
  el.fPosition = $('f-position');
  el.fEmailType = $('f-emailType');
  el.fType = $('f-type');
  el.fGender = $('f-gender');
  el.fLinkedin = $('f-linkedin');
  el.applyBtn = $('applyBtn');
  el.clearBtn = $('clearBtn');
  el.headerRow = $('headerRow');
  el.body = $('resultsBody');
  el.summary = $('resultsSummary');
  el.selectedInfo = $('selectedInfo');
  el.downloadBtn = $('downloadBtn');
  el.editBtn = $('editBtn');
  el.aiBtn = $('aiBtn');
  el.deleteBtn = $('deleteBtn');
  el.modalRoot = $('modalRoot');
  el.pageInfo = $('pageInfo');
  el.firstBtn = $('firstBtn');
  el.prevBtn = $('prevBtn');
  el.nextBtn = $('nextBtn');
  el.lastBtn = $('lastBtn');
}

function createHeader() {
  el.headerRow.innerHTML = '';

  // leading select-all checkbox column
  const checkTh = document.createElement('th');
  checkTh.className = 'check-col';
  allCheck = document.createElement('input');
  allCheck.type = 'checkbox';
  allCheck.className = 'all-check';
  allCheck.title = 'Select all on this page';
  allCheck.addEventListener('change', () => toggleSelectAll(allCheck.checked));
  checkTh.appendChild(allCheck);
  el.headerRow.appendChild(checkTh);

  for (const col of COLUMNS) {
    const th = document.createElement('th');
    if (col.type === 'image') th.classList.add('photo-col');
    else if (col.type === 'lastpath') th.classList.add('lastpath-col');
    else if (col.type === 'position') th.classList.add('position-col');
    else if (col.type === 'location') th.classList.add('location-col');
    if (col.type === 'linkedin' || col.type === 'maps') th.classList.add('icon-col');

    if (col.type === 'linkedin') {
      th.innerHTML = LINKEDIN_SVG;        // header shows the LinkedIn icon, not text
      th.title = 'LinkedIn';
    } else if (col.type === 'maps') {
      th.innerHTML = PIN_SVG;             // header shows a map pin, not text
      th.title = 'Google Maps';
    } else if (col.sortable) {
      th.classList.add('sortable');
      const active = state.sort.column === col.key;
      const arrow = active ? (state.sort.dir === 1 ? ' ▲' : ' ▼') : '';
      th.textContent = col.label + arrow;
      if (active) th.classList.add('sorted');
      th.title = `Sort by ${col.label}`;
      th.addEventListener('click', () => sortBy(col.key));
    } else {
      th.textContent = col.label;
    }
    el.headerRow.appendChild(th);
  }
}

function sortBy(column) {
  if (state.sort.column === column) state.sort.dir = -state.sort.dir;
  else state.sort = { column, dir: 1 };
  state.page = 1;
  createHeader();
  query();
}

function renderRows() {
  el.body.innerHTML = '';

  if (!state.rows.length) {
    const tr = document.createElement('tr');
    const td = document.createElement('td');
    td.colSpan = COLUMNS.length + 1;
    td.textContent = 'No contacts match your search.';
    td.style.padding = '20px';
    tr.appendChild(td);
    el.body.appendChild(tr);
    updateAllCheck();
    return;
  }

  const frag = document.createDocumentFragment();
  for (const record of state.rows) {
    const tr = document.createElement('tr');
    const email = norm(record['Email Address']);

    const checkTd = document.createElement('td');
    checkTd.className = 'check-col';
    const cb = document.createElement('input');
    cb.type = 'checkbox';
    cb.className = 'row-check';
    cb.checked = state.selected.has(email);
    cb.addEventListener('change', () => {
      if (cb.checked) state.selected.set(email, record);
      else state.selected.delete(email);
      tr.classList.toggle('selected', cb.checked);
      updateSelectedInfo();
      updateAllCheck();
    });
    if (cb.checked) tr.classList.add('selected');
    checkTd.appendChild(cb);
    tr.appendChild(checkTd);

    for (const col of COLUMNS) {
      const cell = document.createElement('td');
      const value = record[col.key];

      if (col.type === 'image') {
        // thumbnail from Image URL (not a link). Hover shows Description, else Title, else nothing.
        cell.className = 'photo-cell';
        const src = record['Image URL'];
        const tip = record['Description'] || record['Title'] || '';
        if (src) {
          const img = document.createElement('img');
          img.className = 'row-photo';
          img.src = src;
          img.alt = '';
          img.loading = 'lazy';
          img.referrerPolicy = 'no-referrer';
          if (tip) img.title = tip;
          img.addEventListener('error', () => { img.remove(); });
          cell.appendChild(img);
        }
      } else if (col.type === 'lastpath') {
        // Last Path text, hyperlinked to the Web Source URL
        cell.className = 'lastpath-cell';
        const pageUrl = record['Web Source URL'];
        if (value && pageUrl) {
          const a = document.createElement('a');
          a.href = pageUrl;
          a.target = '_blank';
          a.rel = 'noopener noreferrer';
          a.className = 'domain-link';
          a.title = value;
          a.textContent = value;
          cell.appendChild(a);
        } else {
          cell.textContent = value || '';
          if (value) cell.title = value;
        }
      } else if (col.type === 'domain') {
        if (value) {
          const a = document.createElement('a');
          a.href = /^https?:\/\//i.test(value) ? value : `https://${value}`;
          a.target = '_blank';
          a.rel = 'noopener noreferrer';
          a.className = 'domain-link';
          a.textContent = value;
          cell.appendChild(a);
        }
      } else if (col.type === 'linkedin') {
        cell.className = 'linkedin-cell';
        if (value) {
          const a = document.createElement('a');
          a.href = value;
          a.target = '_blank';
          a.rel = 'noopener noreferrer';
          a.className = 'linkedin-link';
          a.title = 'View LinkedIn profile';
          a.innerHTML = LINKEDIN_SVG;
          cell.appendChild(a);
        }
      } else if (col.type === 'maps') {
        cell.className = 'maps-cell';
        if (value) {
          const a = document.createElement('a');
          a.href = value;
          a.target = '_blank';
          a.rel = 'noopener noreferrer';
          a.className = 'maps-pin';
          a.title = 'Open in Google Maps';
          a.innerHTML = PIN_SVG;
          cell.appendChild(a);
        }
      } else if (col.type === 'position') {
        cell.className = 'position-cell';
        cell.textContent = value || '';
      } else if (col.type === 'location') {
        cell.className = 'location-cell';
        cell.textContent = value || '';
      } else {
        cell.textContent = value || '';
      }
      tr.appendChild(cell);
    }
    frag.appendChild(tr);
  }
  el.body.appendChild(frag);
  updateSelectedInfo();
  updateAllCheck();
}

function toggleSelectAll(checked) {
  for (const record of state.rows) {
    const email = norm(record['Email Address']);
    if (checked) state.selected.set(email, record);
    else state.selected.delete(email);
  }
  renderRows();
}

function updateAllCheck() {
  if (!allCheck) return;
  const emails = state.rows.map((r) => norm(r['Email Address']));
  const selCount = emails.filter((e) => state.selected.has(e)).length;
  allCheck.checked = selCount > 0 && selCount === emails.length;
  allCheck.indeterminate = selCount > 0 && selCount < emails.length;
}

function updateSelectedInfo() {
  const n = state.selected.size;
  el.selectedInfo.textContent = n ? `${n.toLocaleString()} selected` : '';
  el.downloadBtn.textContent = n ? `⬇ Download ${n.toLocaleString()} selected` : '⬇ Download CSV';
  if (el.editBtn) el.editBtn.disabled = n === 0;
  if (el.aiBtn) el.aiBtn.disabled = n === 0;
  if (el.deleteBtn) el.deleteBtn.disabled = n === 0;
}

function buildParams(extra) {
  const p = new URLSearchParams();
  const s = el.fSearch.value.trim();
  if (s) p.set('search', s);
  if (el.fDirectory.value) p.set('directory', el.fDirectory.value);
  const doms = parseDomains(el.fDomains.value);
  if (doms.length) p.set('domains', doms.join(','));
  const pos = el.fPosition.value.trim();
  if (pos) p.set('position', pos);
  if (el.fEmailType.value) p.set('emailType', el.fEmailType.value);
  if (el.fType.value) p.set('type', el.fType.value);
  const g = el.fGender.value;
  if (g && g !== 'na') p.set('gender', g);
  if (el.fLinkedin.checked) p.set('linkedin', '1');
  if (state.sort.column) { p.set('sort', state.sort.column); p.set('dir', String(state.sort.dir)); }
  if (extra) for (const k of Object.keys(extra)) p.set(k, extra[k]);
  return p;
}

function totalPages() { return Math.max(1, Math.ceil(state.total / PAGE_SIZE)); }

function renderPagination() {
  const pages = totalPages();
  el.pageInfo.textContent = `Page ${state.page} of ${pages}`;
  const atStart = state.page <= 1;
  const atEnd = state.page >= pages;
  el.firstBtn.disabled = atStart;
  el.prevBtn.disabled = atStart;
  el.nextBtn.disabled = atEnd;
  el.lastBtn.disabled = atEnd;
}

function updateSummary() {
  const total = state.total;
  const start = total ? (state.page - 1) * PAGE_SIZE + 1 : 0;
  const end = Math.min(state.page * PAGE_SIZE, total);
  el.summary.innerHTML = total
    ? `<strong>${total.toLocaleString()}</strong> contacts match · showing <strong>${start.toLocaleString()}–${end.toLocaleString()}</strong>`
    : 'No contacts match your search.';
}

async function query() {
  try {
    el.summary.textContent = 'Searching…';
    const res = await fetch('/api/db/query?' + buildParams({ page: state.page, pageSize: PAGE_SIZE }).toString());
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const out = await res.json();
    state.rows = out.rows || [];
    state.total = out.total || 0;
    // clamp page if filters shrank the result set
    const pages = totalPages();
    if (state.page > pages) { state.page = pages; }
    renderRows();
    renderPagination();
    updateSummary();
    el.resultsScroll && el.resultsScroll.scrollTo({ top: 0 });
  } catch (e) {
    el.summary.textContent = `Error: ${escapeHtml(e.message)}`;
    el.body.innerHTML = '';
  }
}

async function loadFacets() {
  try {
    const res = await fetch('/api/db/facets');
    if (!res.ok) return;
    const f = await res.json();
    const fill = (sel, values, label) => {
      const prev = sel.value;
      sel.innerHTML = `<option value="">All ${label}</option>`;
      for (const v of (values || [])) {
        const o = document.createElement('option');
        o.value = v; o.textContent = v; sel.appendChild(o);
      }
      if (prev && (values || []).includes(prev)) sel.value = prev;
    };
    fill(el.fDirectory, f.directory, 'directories');
    fill(el.fEmailType, f.emailType, 'email types');
    fill(el.fType, f.type, 'types');
  } catch (e) { /* ignore */ }
}

function triggerDownload(text, filename) {
  const blob = new Blob([text], { type: 'text/csv;charset=utf-8;' });
  const link = document.createElement('a');
  const href = URL.createObjectURL(blob);
  link.setAttribute('href', href);
  link.setAttribute('download', filename);
  link.style.visibility = 'hidden';
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(href);
}

function download() {
  const today = new Date().toISOString().split('T')[0];

  // selection present -> export exactly those rows (client-side, across pages)
  if (state.selected.size > 0) {
    const lines = [CSV_COLUMNS.join(',')];
    for (const r of state.selected.values()) {
      lines.push(CSV_COLUMNS.map((c) => csvCell(r[c])).join(','));
    }
    triggerDownload(lines.join('\n'), `contacts-selected-${today}.csv`);
    return;
  }

  // nothing selected -> stream the full filtered result set from the server
  const p = buildParams();
  p.delete('sort'); p.delete('dir');
  window.location.href = '/api/db/export.csv?' + p.toString();
}

// ---- modal helper ----
function closeModal() { if (el.modalRoot) el.modalRoot.innerHTML = ''; }

function openModal(title, buildBody, footerButtons) {
  el.modalRoot.innerHTML = '';
  const overlay = document.createElement('div');
  overlay.className = 'modal-overlay';
  const dialog = document.createElement('div');
  dialog.className = 'modal-dialog';

  const head = document.createElement('div');
  head.className = 'modal-head';
  const h = document.createElement('h2');
  h.textContent = title;
  const x = document.createElement('button');
  x.type = 'button'; x.className = 'modal-close'; x.title = 'Close'; x.innerHTML = '&times;';
  x.addEventListener('click', closeModal);
  head.appendChild(h); head.appendChild(x);

  const bodyWrap = document.createElement('div');
  bodyWrap.className = 'modal-body';
  buildBody(bodyWrap);

  const foot = document.createElement('div');
  foot.className = 'modal-foot';
  (footerButtons || []).forEach((b) => foot.appendChild(b));

  dialog.appendChild(head); dialog.appendChild(bodyWrap); dialog.appendChild(foot);
  overlay.appendChild(dialog);
  overlay.addEventListener('click', (ev) => { if (ev.target === overlay) closeModal(); });
  el.modalRoot.appendChild(overlay);
  return { overlay, dialog, bodyWrap, foot };
}

// ---- manual edit of the selected records ----
function openEditModal() {
  const records = [...state.selected.values()];
  if (!records.length) return;
  const cards = [];

  const cancelBtn = document.createElement('button');
  cancelBtn.type = 'button'; cancelBtn.className = 'ghost'; cancelBtn.textContent = 'Cancel';
  cancelBtn.addEventListener('click', closeModal);
  const saveBtn = document.createElement('button');
  saveBtn.type = 'button'; saveBtn.className = 'primary'; saveBtn.textContent = `Save ${records.length} record(s)`;

  openModal(`Edit ${records.length} record(s)`, (body) => {
    records.forEach((rec) => {
      const card = document.createElement('div');
      card.className = 'edit-card';
      const heading = document.createElement('div');
      heading.className = 'edit-card-title';
      heading.textContent = ((rec['First'] || '') + ' ' + (rec['Last'] || '')).trim()
        + ' — ' + (rec['Email Address'] || '(no email)');
      card.appendChild(heading);

      const grid = document.createElement('div');
      grid.className = 'edit-grid';
      const inputs = {};
      EDIT_FIELDS.forEach((f) => {
        const wrap = document.createElement('label');
        wrap.className = 'edit-field' + (f.key === 'Description' ? ' wide' : '');
        const span = document.createElement('span');
        span.textContent = f.label;
        let input;
        if (f.key === 'Description') { input = document.createElement('textarea'); input.rows = 2; }
        else { input = document.createElement('input'); input.type = 'text'; }
        input.value = rec[f.key] || '';
        wrap.appendChild(span); wrap.appendChild(input);
        grid.appendChild(wrap);
        inputs[f.key] = input;
      });
      card.appendChild(grid);
      cards.push({ rec, inputs });
      body.appendChild(card);
    });
  }, [cancelBtn, saveBtn]);

  saveBtn.addEventListener('click', async () => {
    const edits = [];
    for (const { rec, inputs } of cards) {
      const updates = {};
      for (const f of EDIT_FIELDS) {
        const val = inputs[f.key].value;
        if (String(val) !== String(rec[f.key] || '')) updates[f.key] = val;
      }
      if (Object.keys(updates).length) {
        edits.push({ email: String(rec['Email Address'] || '').toLowerCase(), updates });
      }
    }
    if (!edits.length) { closeModal(); return; }
    saveBtn.disabled = true; cancelBtn.disabled = true; saveBtn.textContent = 'Saving…';
    try {
      const res = await fetch('/api/db/update', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ edits }),
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      closeModal();
      state.selected.clear();
      updateSelectedInfo();
      query();
    } catch (e) {
      saveBtn.disabled = false; cancelBtn.disabled = false;
      saveBtn.textContent = `Save ${records.length} record(s)`;
      alert('Save failed: ' + e.message);
    }
  });
}

// ---- AI Search: Claude reviews + corrects fields, auto-applied ----
async function runAiSearch() {
  const records = [...state.selected.values()];
  if (!records.length) return;
  const emails = records.map((r) => String(r['Email Address'] || '').toLowerCase()).filter(Boolean);
  if (!emails.length) return;
  if (!window.confirm(`Run AI Search on ${emails.length} record(s)?\n\nClaude reviews and corrects fields (Title, First/Last, Email, LinkedIn). Changes are saved automatically.`)) return;

  const status = document.createElement('p');
  status.className = 'ai-status';
  status.textContent = `Reviewing ${emails.length} record(s) with Claude… this can take a moment.`;
  const closeBtn = document.createElement('button');
  closeBtn.type = 'button'; closeBtn.className = 'primary'; closeBtn.textContent = 'Close';
  closeBtn.disabled = true;
  closeBtn.addEventListener('click', closeModal);
  const m = openModal('AI Search', (b) => { b.appendChild(status); }, [closeBtn]);

  try {
    const res = await fetch('/api/db/ai-enrich', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ emails }),
    });
    const out = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(out.error || `HTTP ${res.status}`);
    const results = out.results || [];
    const changed = results.filter((r) => r.ok && (r.changed || 0) > 0);
    const failed = results.filter((r) => !r.ok);

    status.remove();
    const summary = document.createElement('p');
    summary.className = 'ai-status';
    summary.innerHTML = `<strong>${changed.length}</strong> of ${results.length} record(s) updated`
      + (failed.length ? ` · <span class="ai-fail">${failed.length} failed</span>` : '');
    m.bodyWrap.appendChild(summary);

    if (changed.length) {
      const list = document.createElement('div');
      list.className = 'ai-report';
      changed.forEach((r) => {
        const item = document.createElement('div');
        item.className = 'ai-report-item';
        const rows = Object.keys(r.changes || {}).map((f) =>
          `<div><b>${escapeHtml(f)}</b>: <span class="ai-from">${escapeHtml(r.changes[f].from) || '∅'}</span> &rarr; <span class="ai-to">${escapeHtml(r.changes[f].to)}</span></div>`
        ).join('');
        item.innerHTML = `<div class="ai-report-email">${escapeHtml(r.newEmail || r.email)}</div>${rows}`;
        list.appendChild(item);
      });
      m.bodyWrap.appendChild(list);
    }
    closeBtn.disabled = false;
    state.selected.clear();
    updateSelectedInfo();
    query();
  } catch (e) {
    status.textContent = 'AI Search failed: ' + e.message;
    closeBtn.disabled = false;
  }
}

// ---- permanently delete the selected records ----
async function deleteSelected() {
  const records = [...state.selected.values()];
  if (!records.length) return;
  const emails = records.map((r) => String(r['Email Address'] || '').toLowerCase()).filter(Boolean);
  if (!emails.length) return;
  if (!window.confirm(`Permanently delete ${emails.length} record(s)?\n\nThis removes them from the master database and cannot be undone.`)) return;

  el.deleteBtn.disabled = true;
  try {
    const res = await fetch('/api/db/delete', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ emails }),
    });
    const out = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(out.error || `HTTP ${res.status}`);
    state.selected.clear();
    updateSelectedInfo();
    query();
  } catch (e) {
    alert('Delete failed: ' + e.message);
    updateSelectedInfo();
  }
}

function runSearch() {
  state.page = 1;
  query();
}

function clearFilters() {
  el.fSearch.value = '';
  el.fDirectory.value = '';
  el.fDomains.value = '';
  el.fPosition.value = '';
  el.fEmailType.value = '';
  el.fType.value = '';
  el.fGender.value = 'na';
  el.fLinkedin.checked = false;
  state.selected.clear();
  updateSelectedInfo();
  runSearch();
}

function debouncedSearch() {
  clearTimeout(searchTimer);
  searchTimer = setTimeout(runSearch, 300);
}

function attachEvents() {
  el.fSearch.addEventListener('input', debouncedSearch);
  el.fPosition.addEventListener('input', debouncedSearch);
  el.fDomains.addEventListener('input', debouncedSearch);
  el.fDirectory.addEventListener('change', runSearch);
  el.fEmailType.addEventListener('change', runSearch);
  el.fType.addEventListener('change', runSearch);
  el.fGender.addEventListener('change', runSearch);
  el.fLinkedin.addEventListener('change', runSearch);
  el.applyBtn.addEventListener('click', runSearch);
  el.clearBtn.addEventListener('click', clearFilters);
  el.downloadBtn.addEventListener('click', download);
  el.editBtn.addEventListener('click', openEditModal);
  el.aiBtn.addEventListener('click', runAiSearch);
  el.deleteBtn.addEventListener('click', deleteSelected);

  el.firstBtn.addEventListener('click', () => { state.page = 1; query(); });
  el.prevBtn.addEventListener('click', () => { state.page = Math.max(1, state.page - 1); query(); });
  el.nextBtn.addEventListener('click', () => { state.page = Math.min(totalPages(), state.page + 1); query(); });
  el.lastBtn.addEventListener('click', () => { state.page = totalPages(); query(); });
}

document.addEventListener('DOMContentLoaded', async () => {
  initElements();
  el.resultsScroll = $('resultsScroll');
  attachEvents();
  createHeader();
  updateSelectedInfo();
  await loadFacets();
  query();
});
