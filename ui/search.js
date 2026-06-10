// Search Database page — queries the central (master) contacts DB server-side, with
// row checkboxes (cross-page selection), sortable columns, and CSV download.

// Full CSV schema (matches the engine / main-page download).
const CSV_COLUMNS = [
  'Time Stamp', 'Source', 'Web Source URL', 'Directory', 'Path ID', 'Domain', 'Last Path',
  'Bio Check', 'First', 'Last', 'Gender', 'Title', 'Position', 'Description', 'Email Address',
  'Email Type', 'LinkedIn URL', 'Google Maps', 'Phone', 'Phone Type', 'Phone Location',
  'Phone 2', 'Phone 2 Type'
];

// On-screen columns (same set the main page's table shows).
const DISPLAY_COLUMNS = [
  'Time Stamp', 'Source', 'Directory', 'Path ID', 'Domain', 'Last Path', 'First', 'Last', 'Gender',
  'Position', 'Description', 'Email Address', 'Email Type', 'LinkedIn URL', 'Google Maps',
  'Phone', 'Phone Type', 'Phone Location', 'Phone 2', 'Phone 2 Type'
];

const PAGE_SIZE = 50;

const PIN_SVG = '<svg viewBox="0 0 24 24" width="18" height="18" aria-hidden="true">'
  + '<path fill="currentColor" d="M12 2C8.13 2 5 5.13 5 9c0 5.25 7 13 7 13s7-7.75 7-13c0-3.87-3.13-7-7-7zm0 9.5A2.5 2.5 0 1 1 12 6a2.5 2.5 0 0 1 0 5.5z"/></svg>';

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
  el.fGender = $('f-gender');
  el.fLinkedin = $('f-linkedin');
  el.applyBtn = $('applyBtn');
  el.clearBtn = $('clearBtn');
  el.headerRow = $('headerRow');
  el.body = $('resultsBody');
  el.summary = $('resultsSummary');
  el.selectedInfo = $('selectedInfo');
  el.downloadBtn = $('downloadBtn');
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

  for (const column of DISPLAY_COLUMNS) {
    const th = document.createElement('th');
    th.className = 'sortable';
    if (column === 'Description') th.classList.add('desc-col');
    const active = state.sort.column === column;
    const arrow = active ? (state.sort.dir === 1 ? ' ▲' : ' ▼') : '';
    th.textContent = column + arrow;
    if (active) th.classList.add('sorted');
    th.title = `Sort by ${column}`;
    th.addEventListener('click', () => sortBy(column));
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
    td.colSpan = DISPLAY_COLUMNS.length + 1;
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

    for (const field of DISPLAY_COLUMNS) {
      const cell = document.createElement('td');
      const value = record[field];

      if (field === 'Domain') {
        if (value) {
          const a = document.createElement('a');
          a.href = /^https?:\/\//i.test(value) ? value : `https://${value}`;
          a.target = '_blank';
          a.rel = 'noopener noreferrer';
          a.className = 'domain-link';
          a.textContent = value;
          cell.appendChild(a);
        }
      } else if (field === 'LinkedIn URL') {
        if (value) {
          const a = document.createElement('a');
          a.href = value;
          a.target = '_blank';
          a.rel = 'noopener noreferrer';
          a.className = 'domain-link';
          a.textContent = 'LinkedIn';
          cell.appendChild(a);
        }
      } else if (field === 'Google Maps') {
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
      } else if (field === 'Description') {
        cell.className = 'desc-cell';
        const clip = document.createElement('div');
        clip.className = 'desc-clip';
        clip.textContent = value || '';
        if (value) cell.title = value;
        cell.appendChild(clip);
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
  el.fGender.addEventListener('change', runSearch);
  el.fLinkedin.addEventListener('change', runSearch);
  el.applyBtn.addEventListener('click', runSearch);
  el.clearBtn.addEventListener('click', clearFilters);
  el.downloadBtn.addEventListener('click', download);

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
