// Full record schema (order matches the engine / CSV download). "Image URL" is NOT
// here — it only powers the row thumbnail.
const CSV_COLUMNS = [
  'Time Stamp',
  'Source',
  'Web Source URL',
  'Directory',
  'ID',
  'Last Path',
  'Bio Check',
  'First',
  'Last',
  'Gender',
  'Title',
  'Position',
  'Description',
  'Email Address',
  'Email Type',
  'LinkedIn URL',
  'Google Maps',
  'Phone',
  'Phone Type',
  'Phone Location',
  'Phone 2',
  'Phone 2 Type'
];

// On-screen table columns. Differs from the CSV: Web Source URL, Title, Bio Check, ID
// are hidden (still in the CSV download); "Domain" (root domain of Web Source URL) is
// shown in ID's place; the thumbnail links to the Web Source URL.
const DISPLAY_COLUMNS = [
  'Time Stamp', 'Source', 'Directory', 'Domain', 'Last Path', 'First', 'Last', 'Gender',
  'Position', 'Description', 'Email Address', 'Email Type', 'LinkedIn URL', 'Google Maps',
  'Phone', 'Phone Type', 'Phone Location', 'Phone 2', 'Phone 2 Type'
];

const PAGE_SIZE = 50;

// red location pin (Google-Maps style), hyperlinked to the Google Maps URL
const PIN_SVG = '<svg viewBox="0 0 24 24" width="18" height="18" aria-hidden="true">'
  + '<path fill="currentColor" d="M12 2C8.13 2 5 5.13 5 9c0 5.25 7 13 7 13s7-7.75 7-13c0-3.87-3.13-7-7-7zm0 9.5A2.5 2.5 0 1 1 12 6a2.5 2.5 0 0 1 0 5.5z"/></svg>';
// small "open link" glyph for rows that have a source URL but no image
const LINK_SVG = '<svg viewBox="0 0 24 24" width="16" height="16" aria-hidden="true">'
  + '<path fill="currentColor" d="M14 3v2h3.59l-9.3 9.29 1.42 1.42L19 6.41V10h2V3h-7zM5 5h6V3H5a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-6h-2v6H5V5z"/></svg>';

const state = {
  data: [],
  filtered: [],
  domainList: [],
  jobs: [],
  viewingJobId: null,
  lastViewedCount: -1,
  sort: { column: null, dir: 1 },   // dir: 1 asc, -1 desc
  jobsCollapsed: true,              // default collapsed; the latest job still shows
  page: 1,
};

let jobsPollTimer = null;
let topScrollWired = false;

const elements = {};

function initElements() {
  elements.tableSummary = document.getElementById('tableSummary');
  elements.loadingIndicator = document.getElementById('loadingIndicator');
  elements.loadingText = document.getElementById('loadingText');
  elements.resultsBody = document.getElementById('resultsBody');
  elements.tableHeaderRow = document.getElementById('tableHeaderRow');
  elements.searchInput = document.getElementById('searchInput');
  elements.directoryFilter = document.getElementById('directoryFilter');
  elements.emailTypeFilter = document.getElementById('emailTypeFilter');
  elements.genderFilter = document.getElementById('genderFilter');
  elements.phoneTypeFilter = document.getElementById('phoneTypeFilter');
  elements.linkedinRequired = document.getElementById('linkedinRequired');
  elements.refreshButton = document.getElementById('refreshButton');
  elements.downloadButton = document.getElementById('downloadButton');
  elements.modeIndicator = document.getElementById('modeIndicator');
  elements.searchStatus = document.getElementById('searchStatus');
  elements.domainInput = document.getElementById('domainInput');
  elements.domainFileInput = document.getElementById('domainFileInput');
  elements.applyDomainsButton = document.getElementById('applyDomainsButton');
  elements.clearDomainsButton = document.getElementById('clearDomainsButton');
  elements.liveOnlyCheckbox = document.getElementById('liveOnlyCheckbox');
  elements.jobsList = document.getElementById('jobsList');
  elements.refreshJobsButton = document.getElementById('refreshJobsButton');
  elements.jobsToggle = document.getElementById('jobsToggle');
  elements.viewingIndicator = document.getElementById('viewingIndicator');
  elements.tableScroll = document.getElementById('tableScroll');
  elements.tableScrollTop = document.getElementById('tableScrollTop');
  elements.tableScrollTopInner = document.getElementById('tableScrollTopInner');
  elements.pagination = document.getElementById('pagination');
  elements.pageInfo = document.getElementById('pageInfo');
  elements.firstPageBtn = document.getElementById('firstPageBtn');
  elements.prevPageBtn = document.getElementById('prevPageBtn');
  elements.nextPageBtn = document.getElementById('nextPageBtn');
  elements.lastPageBtn = document.getElementById('lastPageBtn');
}

function createHeader() {
  elements.tableHeaderRow.innerHTML = '';

  const photoTh = document.createElement('th');     // leading thumbnail column (not sortable)
  photoTh.textContent = '';
  photoTh.className = 'photo-col';
  elements.tableHeaderRow.appendChild(photoTh);

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
    elements.tableHeaderRow.appendChild(th);
  }
}

function sortBy(column) {
  if (state.sort.column === column) {
    state.sort.dir = -state.sort.dir;             // toggle direction
  } else {
    state.sort = { column, dir: 1 };
  }
  state.page = 1;
  createHeader();                                 // refresh sort arrows
  applyFilters();
}

function compareValues(a, b) {
  const av = a == null ? '' : String(a).trim();
  const bv = b == null ? '' : String(b).trim();
  if (av === '' && bv === '') return 0;
  if (av === '') return 1;                         // blanks always sort last
  if (bv === '') return -1;
  return av.localeCompare(bv, undefined, { numeric: true, sensitivity: 'base' });
}

function normalizeValue(value) {
  return String(value || '').trim().toLowerCase();
}

function parseHostname(value) {
  const text = String(value || '').trim();
  if (!text) return '';

  try {
    const url = new URL(text);
    return url.hostname.replace(/^www\./i, '').toLowerCase();
  } catch (error) {
    const cleaned = text.replace(/^https?:\/\//i, '').split('/')[0].split('?')[0].split('#')[0];
    return cleaned.replace(/^www\./i, '').toLowerCase();
  }
}

function parseDomainList(text) {
  return Array.from(
    new Set(
      String(text || '')
        .split(/[\r\n,]+/)
        .map((line) => parseHostname(line))
        .filter(Boolean)
    )
  );
}

function buildFilterOptions(field, selectElement) {
  const previous = selectElement.value;                 // preserve the user's current choice
  const uniqueValues = Array.from(
    new Set(state.data.map((row) => normalizeValue(row[field])).filter(Boolean))
  ).sort();

  selectElement.innerHTML = `<option value="">All ${field.toLowerCase()}</option>`;
  uniqueValues.forEach((value) => {
    const option = document.createElement('option');
    option.value = value;
    option.textContent = value;
    selectElement.appendChild(option);
  });
  if (previous && uniqueValues.includes(previous)) selectElement.value = previous;
}

function rebuildFilters() {
  buildFilterOptions('Directory', elements.directoryFilter);
  buildFilterOptions('Email Type', elements.emailTypeFilter);
  if (elements.phoneTypeFilter) buildFilterOptions('Phone Type', elements.phoneTypeFilter);
}

// root domain of the Web Source URL, attached so it's sortable/searchable like any column
function decorateRows(rows) {
  for (const r of rows) r.Domain = parseHostname(r['Web Source URL']);
  return rows;
}

// Totals live in the Search Results header (the 3 top boxes were removed).
function updateSummary() {
  const total = state.data.length;
  const displayed = state.filtered.length;
  const uniqueEmails = new Set(state.data.map((row) => normalizeValue(row['Email Address'])).filter(Boolean)).size;
  const domainCount = state.domainList.length;

  const start = displayed ? (state.page - 1) * PAGE_SIZE + 1 : 0;
  const end = Math.min(state.page * PAGE_SIZE, displayed);
  const domainText = domainCount ? ` · ${domainCount} domain${domainCount === 1 ? '' : 's'} filtered` : '';

  elements.tableSummary.innerHTML =
    `<strong>${total}</strong> records · showing <strong>${start}-${end}</strong> of <strong>${displayed}</strong> · `
    + `<strong>${uniqueEmails}</strong> unique emails${domainText}`;
}

function matchesSearch(row, query) {
  if (!query) return true;
  return Object.values(row).some((value) => normalizeValue(value).includes(query));
}

function matchesDomainFilter(row) {
  if (state.domainList.length === 0) return true;

  const hostname = parseHostname(row['Web Source URL']);
  if (!hostname) return false;

  return state.domainList.some((domain) => hostname === domain || hostname.endsWith(`.${domain}`));
}

function applyFilters() {
  const searchQuery = normalizeValue(elements.searchInput.value);
  const directoryValue = normalizeValue(elements.directoryFilter.value);
  const emailTypeValue = normalizeValue(elements.emailTypeFilter.value);
  const phoneTypeValue = elements.phoneTypeFilter ? normalizeValue(elements.phoneTypeFilter.value) : '';
  const genderValue = elements.genderFilter ? elements.genderFilter.value : 'na';
  const linkedinRequired = !!(elements.linkedinRequired && elements.linkedinRequired.checked);

  state.filtered = state.data.filter((row) => {
    if (directoryValue && normalizeValue(row.Directory) !== directoryValue) return false;
    if (emailTypeValue && normalizeValue(row['Email Type']) !== emailTypeValue) return false;
    if (phoneTypeValue && normalizeValue(row['Phone Type']) !== phoneTypeValue) return false;

    const g = String(row.Gender || '').trim().toUpperCase();
    // 'na' (default) = no gender filter — show everything
    if (genderValue === 'male' && g !== 'M') return false;
    else if (genderValue === 'female' && g !== 'F') return false;
    else if (genderValue === 'all' && !(g === 'M' || g === 'F')) return false;       // All = M or F
    else if (genderValue === 'none' && (g === 'M' || g === 'F')) return false;       // None = neither

    if (linkedinRequired && !String(row['LinkedIn URL'] || '').trim()) return false;

    if (!matchesDomainFilter(row)) return false;
    return matchesSearch(row, searchQuery);
  });

  if (state.sort.column) {
    const col = state.sort.column, dir = state.sort.dir;
    state.filtered.sort((a, b) => dir * compareValues(a[col], b[col]));
  }

  renderTable();
  updateSummary();
}

function totalPages() {
  return Math.max(1, Math.ceil(state.filtered.length / PAGE_SIZE));
}

function renderPagination() {
  const pages = totalPages();
  if (elements.pageInfo) elements.pageInfo.textContent = `Page ${state.page} of ${pages}`;
  const atStart = state.page <= 1;
  const atEnd = state.page >= pages;
  if (elements.firstPageBtn) elements.firstPageBtn.disabled = atStart;
  if (elements.prevPageBtn) elements.prevPageBtn.disabled = atStart;
  if (elements.nextPageBtn) elements.nextPageBtn.disabled = atEnd;
  if (elements.lastPageBtn) elements.lastPageBtn.disabled = atEnd;
}

function setPage(p) {
  state.page = p;
  renderTable();
  updateSummary();
}

function renderTable() {
  // clamp page to the available range, then slice this page
  const pages = totalPages();
  if (state.page > pages) state.page = pages;
  if (state.page < 1) state.page = 1;
  const startIdx = (state.page - 1) * PAGE_SIZE;
  const rowsToRender = state.filtered.slice(startIdx, startIdx + PAGE_SIZE);

  elements.resultsBody.innerHTML = '';

  if (rowsToRender.length === 0) {
    const row = document.createElement('tr');
    const cell = document.createElement('td');
    cell.colSpan = DISPLAY_COLUMNS.length + 1;     // +1 for the leading photo column
    cell.textContent = 'No records match the current filters.';
    cell.style.padding = '20px';
    row.appendChild(cell);
    elements.resultsBody.appendChild(row);
    renderPagination();
    syncTopScrollbar();
    return;
  }

  const frag = document.createDocumentFragment();
  for (const record of rowsToRender) {
    const row = document.createElement('tr');

    // leading cell: thumbnail (or a link glyph), hyperlinked to the Web Source URL
    const photoCell = document.createElement('td');
    photoCell.className = 'photo-cell';
    const src = record['Image URL'];
    const pageUrl = record['Web Source URL'];

    let inner = null;
    if (src) {
      const img = document.createElement('img');
      img.className = 'row-photo';
      img.src = src;
      img.alt = '';
      img.loading = 'lazy';
      img.referrerPolicy = 'no-referrer';
      img.addEventListener('error', () => { img.replaceWith(makeLinkFallback(!!pageUrl)); });
      inner = img;
    } else if (pageUrl) {
      inner = makeLinkFallback(true);
    }

    if (inner) {
      if (pageUrl) {
        const a = document.createElement('a');
        a.href = pageUrl;
        a.target = '_blank';
        a.rel = 'noopener noreferrer';
        a.title = 'Open source page';
        a.className = 'photo-link';
        a.appendChild(inner);
        photoCell.appendChild(a);
      } else {
        photoCell.appendChild(inner);
      }
    }
    row.appendChild(photoCell);

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
        if (value) cell.title = value;            // full text on hover
        cell.appendChild(clip);
      } else {
        cell.textContent = value || '';
      }
      row.appendChild(cell);
    }
    frag.appendChild(row);
  }
  elements.resultsBody.appendChild(frag);
  renderPagination();
  syncTopScrollbar();
}

function makeLinkFallback(hasUrl) {
  const span = document.createElement('span');
  span.className = 'photo-fallback';
  if (hasUrl) span.innerHTML = LINK_SVG;
  return span;
}

// Keep the dummy top scrollbar's width in sync with the real table width.
function syncTopScrollbar() {
  if (!elements.tableScroll || !elements.tableScrollTopInner) return;
  const table = document.getElementById('resultsTable');
  const w = table ? table.scrollWidth : 0;
  elements.tableScrollTopInner.style.width = w + 'px';
}

function wireTopScrollbar() {
  if (topScrollWired || !elements.tableScroll || !elements.tableScrollTop) return;
  topScrollWired = true;
  let lock = false;
  elements.tableScrollTop.addEventListener('scroll', () => {
    if (lock) return; lock = true;
    elements.tableScroll.scrollLeft = elements.tableScrollTop.scrollLeft;
    lock = false;
  });
  elements.tableScroll.addEventListener('scroll', () => {
    if (lock) return; lock = true;
    elements.tableScrollTop.scrollLeft = elements.tableScroll.scrollLeft;
    lock = false;
  });
  window.addEventListener('resize', syncTopScrollbar);
}

async function loadResults() {
  try {
    showLoading('Loading results...');
    const response = await fetch('/api/results');
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const data = await response.json();
    state.data = decorateRows(data);
    state.page = 1;

    createHeader();
    rebuildFilters();
    applyFilters();
  } catch (error) {
    elements.tableSummary.textContent = `Error loading data: ${error.message}`;
    elements.resultsBody.innerHTML = '';
  } finally {
    hideLoading();
  }
}

// Start a search as a server-side background job, then watch it via polling.
async function searchContacts() {
  const domains = parseDomainList(elements.domainInput.value);
  if (domains.length === 0) {
    setSearchStatus('Please enter one or more domains to search.');
    return;
  }
  const directoryFilter = elements.directoryFilter.value.trim();
  const liveOnly = !!(elements.liveOnlyCheckbox && elements.liveOnlyCheckbox.checked);

  try {
    setSearchStatus(`Starting a job for ${domains.length} domain${domains.length === 1 ? '' : 's'}...`);
    const response = await fetch('/api/jobs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ domains, directoryFilter, liveOnly }),
    });
    if (!response.ok) {
      const payload = await response.json().catch(() => ({}));
      throw new Error(payload.error || `HTTP ${response.status}`);
    }
    const job = await response.json();
    setSearchStatus(`Job started for ${domains.length} domain${domains.length === 1 ? '' : 's'}. It runs on the server — you can leave this page.`);
    await fetchJobs();
    viewJob(job.id);            // attach the table to the new job; polling keeps it live
  } catch (error) {
    setSearchStatus(`Could not start job: ${error.message}`);
  }
}

// ---------------------------------------------------------------- jobs
async function fetchJobs() {
  try {
    const res = await fetch('/api/jobs');
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    state.jobs = await res.json();
    renderJobs();
  } catch (error) {
    if (elements.jobsList) elements.jobsList.innerHTML = `<p class="jobs-empty">Could not load jobs: ${escapeHtml(error.message)}</p>`;
  }
}

function escapeHtml(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, (c) => (
    { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]
  ));
}

function shortJobLabel(job) {
  const when = job.createdAt ? new Date(job.createdAt).toLocaleString() : job.id;
  return `${job.total} domain${job.total === 1 ? '' : 's'} · ${when}`;
}

function renderJobs() {
  const list = elements.jobsList;
  if (!list) return;
  if (!state.jobs.length) {
    list.innerHTML = '<p class="jobs-empty">No jobs yet. Paste domains above and click <strong>FIND CONTACTS</strong>.</p>';
    return;
  }

  list.innerHTML = '';
  // collapsed view keeps just the latest job (jobs are newest-first); expanded shows all
  const jobsToShow = state.jobsCollapsed ? state.jobs.slice(0, 1) : state.jobs;
  for (const job of jobsToShow) {
    const pct = job.total ? Math.round((job.done / job.total) * 100) : 0;
    const cov = job.coverage || { found: 0, live: 0, empty: 0 };
    const isViewing = job.id === state.viewingJobId;

    const card = document.createElement('div');
    card.className = `job-card${isViewing ? ' viewing' : ''}`;

    const progressBit = job.status === 'running'
      ? `<div class="progress-track"><div class="progress-fill" style="width:${pct}%"></div></div>
         <span class="job-meta">${job.done}/${job.total} domains</span>`
      : `<span class="job-meta">${job.done}/${job.total} domains</span>`;

    const liveOnlyBit = job.liveOnly ? ' <span class="job-meta">· live-only</span>' : '';
    const errorBit = job.error ? `<span class="job-meta" style="color:#b91c1c">${escapeHtml(job.error)}</span>` : '';

    card.innerHTML = `
      <div class="job-main">
        <div class="job-line">
          <span class="badge ${job.status}">${job.status}</span>
          <strong>${escapeHtml(shortJobLabel(job))}</strong>${liveOnlyBit}
        </div>
        <div class="job-line">
          ${progressBit}
          <span class="job-coverage">
            <span class="cc">${cov.found} CC</span> ·
            <span class="live">${cov.live} live</span> ·
            <span class="none">${cov.empty} none</span> ·
            <strong>${job.recordCount} people</strong>
          </span>
          ${errorBit}
        </div>
      </div>
      <div class="job-actions"></div>
    `;

    const actions = card.querySelector('.job-actions');

    const viewBtn = document.createElement('button');
    viewBtn.textContent = isViewing ? 'Viewing' : 'View';
    if (isViewing) viewBtn.classList.add('primary');
    viewBtn.addEventListener('click', () => viewJob(job.id));
    actions.appendChild(viewBtn);

    if (job.status === 'running') {
      const stopBtn = document.createElement('button');
      stopBtn.textContent = 'Stop';
      stopBtn.classList.add('danger');
      stopBtn.addEventListener('click', () => stopJobUI(job.id));
      actions.appendChild(stopBtn);
    }

    if (job.recordCount > 0) {
      const dlBtn = document.createElement('button');
      dlBtn.textContent = 'Download CSV';
      dlBtn.addEventListener('click', () => { window.location.href = `/api/jobs/${job.id}/results.csv`; });
      actions.appendChild(dlBtn);
    }

    if (job.status === 'interrupted' || job.status === 'failed' || job.status === 'stopped') {
      const resumeBtn = document.createElement('button');
      resumeBtn.textContent = 'Resume';
      resumeBtn.classList.add('primary');
      resumeBtn.addEventListener('click', () => resumeJobUI(job.id));
      actions.appendChild(resumeBtn);
    }

    const delBtn = document.createElement('button');
    delBtn.textContent = 'Delete';
    delBtn.classList.add('danger');
    delBtn.addEventListener('click', () => deleteJobUI(job.id));
    actions.appendChild(delBtn);

    list.appendChild(card);
  }

  if (state.jobsCollapsed && state.jobs.length > 1) {
    const note = document.createElement('p');
    note.className = 'jobs-empty jobs-more';
    note.textContent = `+ ${state.jobs.length - 1} older job${state.jobs.length - 1 === 1 ? '' : 's'} hidden — show all`;
    note.addEventListener('click', () => toggleJobs());
    list.appendChild(note);
  }
}

function applyJobsCollapsedUI() {
  if (elements.jobsToggle) {
    elements.jobsToggle.textContent = state.jobsCollapsed ? '▸' : '▾';
    elements.jobsToggle.setAttribute('aria-expanded', String(!state.jobsCollapsed));
  }
}

function toggleJobs() {
  state.jobsCollapsed = !state.jobsCollapsed;
  applyJobsCollapsedUI();
  renderJobs();
}

async function resumeJobUI(id) {
  try {
    setSearchStatus('Resuming job...');
    const res = await fetch(`/api/jobs/${id}/resume`, { method: 'POST' });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    await fetchJobs();
    viewJob(id);
  } catch (error) {
    setSearchStatus(`Could not resume: ${error.message}`);
  }
}

async function stopJobUI(id) {
  try {
    setSearchStatus('Stopping job…');
    const res = await fetch(`/api/jobs/${id}/stop`, { method: 'POST' });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    setSearchStatus('Stop requested — finishing the current page, then halting.');
    await fetchJobs();
  } catch (error) {
    setSearchStatus(`Could not stop: ${error.message}`);
  }
}

async function deleteJobUI(id) {
  if (!window.confirm('Delete this job and its results? This cannot be undone.')) return;
  try {
    const res = await fetch(`/api/jobs/${id}`, { method: 'DELETE' });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const wasViewing = state.viewingJobId === id;
    await fetchJobs();
    if (wasViewing) {
      state.viewingJobId = null;
      if (state.jobs.length) {
        viewJob(state.jobs[0].id);                 // switch to the newest remaining job
      } else {
        state.data = [];
        if (elements.viewingIndicator) elements.viewingIndicator.classList.add('hidden');
        applyFilters();
      }
    }
    setSearchStatus('Job deleted.');
  } catch (error) {
    setSearchStatus(`Could not delete: ${error.message}`);
  }
}

// Point the table/filters at a specific job's records.
async function viewJob(id) {
  state.viewingJobId = id;
  state.lastViewedCount = -1;
  state.page = 1;
  await refreshViewedRecords(true);
  renderJobs();
}

async function refreshViewedRecords(rebuildHeader) {
  const id = state.viewingJobId;
  if (!id) return;
  try {
    const res = await fetch(`/api/jobs/${id}/records`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const rows = await res.json();
    state.data = decorateRows(rows);
    state.lastViewedCount = rows.length;

    const job = state.jobs.find((j) => j.id === id);
    if (elements.viewingIndicator) {
      elements.viewingIndicator.classList.remove('hidden');
      const running = job && job.status === 'running';
      const lp = job && job.lastProgress;
      const progressText = running && lp && lp.domain
        ? ` — checking ${escapeHtml(lp.domain)} (${lp.index || job.done}/${job.total})`
        : '';
      elements.viewingIndicator.innerHTML = `Viewing job: <strong>${job ? escapeHtml(shortJobLabel(job)) : id}</strong> [${job ? job.status : '?'}]${progressText}`;
    }

    if (rebuildHeader) createHeader();
    rebuildFilters();                 // always refresh so Directory/Email Type fill as data streams in
    applyFilters();
  } catch (error) {
    setSearchStatus(`Could not load job records: ${error.message}`);
  }
}

// Poll jobs; keep the viewed job's table live while it runs.
async function pollJobs() {
  await fetchJobs();
  const id = state.viewingJobId;
  if (!id) return;
  const job = state.jobs.find((j) => j.id === id);
  if (!job) return;
  if (job.recordCount !== state.lastViewedCount || job.status === 'running') {
    await refreshViewedRecords(false);
  }
}

function showLoading(text) {
  elements.loadingIndicator.classList.remove('hidden');
  elements.loadingText.textContent = text;
  setSearchStatus(text);
}

function hideLoading() {
  elements.loadingIndicator.classList.add('hidden');
}

function setSearchStatus(text) {
  if (elements.searchStatus) {
    elements.searchStatus.textContent = String(text || '');
  }
}

function applyDomainListFromInput() {
  state.domainList = parseDomainList(elements.domainInput.value);
  state.page = 1;
  applyFilters();
}

function handleDomainFileUpload(event) {
  const file = event.target.files?.[0];
  if (!file) return;

  const reader = new FileReader();
  reader.onload = () => {
    const rawText = String(reader.result || '');
    const normalized = rawText.replace(/\r/g, '').trim().replace(/,+/g, '\n');
    elements.domainInput.value = normalized;
    applyDomainListFromInput();
    searchContacts();
  };
  reader.readAsText(file);
}

function clearDomains() {
  elements.domainInput.value = '';
  elements.domainFileInput.value = '';
  state.domainList = [];
  state.page = 1;
  applyFilters();
}

function downloadCSV() {
  if (state.data.length === 0) {
    alert('No data to download');
    return;
  }

  // CSV from the filtered rows — uses the FULL column set (incl. Web Source URL,
  // Title, Bio Check), even though those are hidden in the table.
  const csvLines = [CSV_COLUMNS.join(',')];
  for (const row of state.filtered) {
    const values = CSV_COLUMNS.map((col) => {
      const val = String(row[col] || '');
      if (val.includes(',') || val.includes('"') || val.includes('\n')) {
        return `"${val.replace(/"/g, '""')}"`;
      }
      return val;
    });
    csvLines.push(values.join(','));
  }

  const csvContent = csvLines.join('\n');
  const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
  const link = document.createElement('a');
  const url = URL.createObjectURL(blob);
  link.setAttribute('href', url);
  link.setAttribute('download', `cc-results-${new Date().toISOString().split('T')[0]}.csv`);
  link.style.visibility = 'hidden';
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
}

function attachEvents() {
  elements.searchInput.addEventListener('input', () => { state.page = 1; applyFilters(); });
  elements.directoryFilter.addEventListener('change', () => { state.page = 1; applyFilters(); });
  elements.emailTypeFilter.addEventListener('change', () => { state.page = 1; applyFilters(); });
  if (elements.genderFilter) elements.genderFilter.addEventListener('change', () => { state.page = 1; applyFilters(); });
  if (elements.phoneTypeFilter) elements.phoneTypeFilter.addEventListener('change', () => { state.page = 1; applyFilters(); });
  if (elements.linkedinRequired) elements.linkedinRequired.addEventListener('change', () => { state.page = 1; applyFilters(); });
  elements.refreshButton.addEventListener('click', () => {
    if (state.viewingJobId) refreshViewedRecords(true); else loadResults();
  });
  elements.downloadButton.addEventListener('click', () => downloadCSV());
  if (elements.refreshJobsButton) elements.refreshJobsButton.addEventListener('click', () => fetchJobs());
  if (elements.jobsToggle) elements.jobsToggle.addEventListener('click', () => toggleJobs());
  elements.applyDomainsButton.addEventListener('click', () => searchContacts());
  elements.clearDomainsButton.addEventListener('click', () => clearDomains());
  elements.domainFileInput.addEventListener('change', (event) => handleDomainFileUpload(event));

  if (elements.firstPageBtn) elements.firstPageBtn.addEventListener('click', () => setPage(1));
  if (elements.prevPageBtn) elements.prevPageBtn.addEventListener('click', () => setPage(state.page - 1));
  if (elements.nextPageBtn) elements.nextPageBtn.addEventListener('click', () => setPage(state.page + 1));
  if (elements.lastPageBtn) elements.lastPageBtn.addEventListener('click', () => setPage(totalPages()));

  wireTopScrollbar();
}

async function loadConfig() {
  if (!elements.modeIndicator) return;   // connection-mode verbiage removed from the header
  try {
    const res = await fetch('/api/config');
    if (!res.ok) throw new Error('Failed to load mode');
    const config = await res.json();
    elements.modeIndicator.textContent = config.demoMode
      ? 'Demo mode: mock Common Crawl data'
      : 'Connected to live Common Crawl';
    elements.modeIndicator.className = `mode-indicator ${config.demoMode ? 'demo' : 'live'}`;
  } catch (error) {
    elements.modeIndicator.textContent = 'Unable to determine connection mode';
    elements.modeIndicator.className = 'mode-indicator demo';
  }
}

document.addEventListener('DOMContentLoaded', async () => {
  initElements();
  attachEvents();
  createHeader();
  applyJobsCollapsedUI();      // reflect the default-collapsed state on the toggle
  loadConfig();

  await fetchJobs();
  // open the newest job if there is one; otherwise fall back to the legacy cc-results.csv view
  if (state.jobs.length) {
    viewJob(state.jobs[0].id);
  } else {
    loadResults();
  }

  jobsPollTimer = setInterval(pollJobs, 1500);   // keep jobs + the viewed job live
});
