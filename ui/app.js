const COLUMNS = [
  'Time Stamp',
  'Source',
  'Web Source URL',
  'Directory',
  'ID',
  'Bio Check',
  'First',
  'Last',
  'Gender',
  'Title',
  'Position',
  'Description',
  'Image URL',
  'Email Address',
  'Email Type',
  'LinkedIn URL',
  'Google Maps',
  'Phone',
  'Phone Type',
  'Phone Location',
  'Phone 2'
];

const MAX_ROWS_DEFAULT = 200;

const state = {
  data: [],
  filtered: [],
  showAll: false,
  domainList: [],
  jobs: [],
  viewingJobId: null,
  lastViewedCount: -1,
};

let jobsPollTimer = null;

const elements = {};

function initElements() {
  elements.totalCount = document.getElementById('totalCount');
  elements.filteredCount = document.getElementById('filteredCount');
  elements.emailCount = document.getElementById('emailCount');
  elements.tableSummary = document.getElementById('tableSummary');
  elements.loadingIndicator = document.getElementById('loadingIndicator');
  elements.loadingText = document.getElementById('loadingText');
  elements.resultsBody = document.getElementById('resultsBody');
  elements.tableHeaderRow = document.getElementById('tableHeaderRow');
  elements.searchInput = document.getElementById('searchInput');
  elements.directoryFilter = document.getElementById('directoryFilter');
  elements.emailTypeFilter = document.getElementById('emailTypeFilter');
  elements.refreshButton = document.getElementById('refreshButton');
  elements.downloadButton = document.getElementById('downloadButton');
  elements.modeIndicator = document.getElementById('modeIndicator');
  elements.searchStatus = document.getElementById('searchStatus');
  elements.showAllCheckbox = document.getElementById('showAllCheckbox');
  elements.domainInput = document.getElementById('domainInput');
  elements.domainFileInput = document.getElementById('domainFileInput');
  elements.applyDomainsButton = document.getElementById('applyDomainsButton');
  elements.clearDomainsButton = document.getElementById('clearDomainsButton');
  elements.jobsList = document.getElementById('jobsList');
  elements.refreshJobsButton = document.getElementById('refreshJobsButton');
  elements.viewingIndicator = document.getElementById('viewingIndicator');
}

function createHeader() {
  elements.tableHeaderRow.innerHTML = '';
  const photoTh = document.createElement('th');     // leading thumbnail column
  photoTh.textContent = '';
  photoTh.className = 'photo-col';
  elements.tableHeaderRow.appendChild(photoTh);
  for (const column of COLUMNS) {
    const th = document.createElement('th');
    th.textContent = column;
    elements.tableHeaderRow.appendChild(th);
  }
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

function updateSummary() {
  const total = state.data.length;
  const displayed = state.filtered.length;
  const uniqueEmails = new Set(state.data.map((row) => normalizeValue(row['Email Address']))).size;
  const domainCount = state.domainList.length;

  elements.totalCount.textContent = total;
  elements.filteredCount.textContent = displayed;
  elements.emailCount.textContent = uniqueEmails;

  const rowLimit = state.showAll ? 'all' : MAX_ROWS_DEFAULT;
  const shown = state.showAll ? displayed : Math.min(displayed, MAX_ROWS_DEFAULT);
  const domainText = domainCount ? ` | ${domainCount} domain${domainCount === 1 ? '' : 's'} applied` : '';

  elements.tableSummary.textContent = `Showing ${shown} ${displayed === shown ? '' : `of ${displayed} `}records.${domainText} ${rowLimit === 'all' ? 'Showing all rows.' : `Showing first ${rowLimit}.`}`;
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

  state.filtered = state.data.filter((row) => {
    if (directoryValue && normalizeValue(row.Directory) !== directoryValue) return false;
    if (emailTypeValue && normalizeValue(row['Email Type']) !== emailTypeValue) return false;
    if (!matchesDomainFilter(row)) return false;
    return matchesSearch(row, searchQuery);
  });

  renderTable();
  updateSummary();
}

function renderTable() {
  elements.resultsBody.innerHTML = '';
  const rowsToRender = state.showAll ? state.filtered : state.filtered.slice(0, MAX_ROWS_DEFAULT);

  if (rowsToRender.length === 0) {
    const row = document.createElement('tr');
    const cell = document.createElement('td');
    cell.colSpan = COLUMNS.length + 1;          // +1 for the leading photo column
    cell.textContent = 'No records match the current filters.';
    cell.style.padding = '20px';
    row.appendChild(cell);
    elements.resultsBody.appendChild(row);
    return;
  }

  for (const record of rowsToRender) {
    const row = document.createElement('tr');

    // leading thumbnail from the record's image (hidden if missing or fails to load)
    const photoCell = document.createElement('td');
    photoCell.className = 'photo-cell';
    const src = record['Image URL'];
    if (src) {
      const img = document.createElement('img');
      img.className = 'row-photo';
      img.src = src;
      img.alt = '';
      img.loading = 'lazy';
      img.referrerPolicy = 'no-referrer';
      img.addEventListener('error', () => img.remove());
      photoCell.appendChild(img);
    }
    row.appendChild(photoCell);

    for (const field of COLUMNS) {
      const cell = document.createElement('td');
      cell.textContent = record[field] || '';
      row.appendChild(cell);
    }
    elements.resultsBody.appendChild(row);
  }
}

async function loadResults() {
  try {
    showLoading('Loading results...');
    const response = await fetch('/api/results');
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const data = await response.json();
    state.data = data;
    state.showAll = elements.showAllCheckbox.checked;

    createHeader();
    buildFilterOptions('Directory', elements.directoryFilter);
    buildFilterOptions('Email Type', elements.emailTypeFilter);
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

  try {
    setSearchStatus(`Starting a job for ${domains.length} domain${domains.length === 1 ? '' : 's'}...`);
    const response = await fetch('/api/jobs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ domains, directoryFilter }),
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
  for (const job of state.jobs) {
    const pct = job.total ? Math.round((job.done / job.total) * 100) : 0;
    const cov = job.coverage || { found: 0, live: 0, empty: 0 };
    const isViewing = job.id === state.viewingJobId;

    const card = document.createElement('div');
    card.className = `job-card${isViewing ? ' viewing' : ''}`;

    const progressBit = job.status === 'running'
      ? `<div class="progress-track"><div class="progress-fill" style="width:${pct}%"></div></div>
         <span class="job-meta">${job.done}/${job.total} domains</span>`
      : `<span class="job-meta">${job.done}/${job.total} domains</span>`;

    const errorBit = job.error ? `<span class="job-meta" style="color:#b91c1c">${escapeHtml(job.error)}</span>` : '';

    card.innerHTML = `
      <div class="job-main">
        <div class="job-line">
          <span class="badge ${job.status}">${job.status}</span>
          <strong>${escapeHtml(shortJobLabel(job))}</strong>
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

    if (job.recordCount > 0) {
      const dlBtn = document.createElement('button');
      dlBtn.textContent = 'Download CSV';
      dlBtn.addEventListener('click', () => { window.location.href = `/api/jobs/${job.id}/results.csv`; });
      actions.appendChild(dlBtn);
    }

    if (job.status === 'interrupted' || job.status === 'failed') {
      const resumeBtn = document.createElement('button');
      resumeBtn.textContent = 'Resume';
      resumeBtn.classList.add('primary');
      resumeBtn.addEventListener('click', () => resumeJobUI(job.id));
      actions.appendChild(resumeBtn);
    }

    list.appendChild(card);
  }
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

// Point the table/filters at a specific job's records.
async function viewJob(id) {
  state.viewingJobId = id;
  state.lastViewedCount = -1;
  await refreshViewedRecords(true);
  renderJobs();
}

async function refreshViewedRecords(rebuildFilters) {
  const id = state.viewingJobId;
  if (!id) return;
  try {
    const res = await fetch(`/api/jobs/${id}/records`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const rows = await res.json();
    state.data = rows;
    state.lastViewedCount = rows.length;
    state.showAll = elements.showAllCheckbox.checked;

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

    if (rebuildFilters) {
      createHeader();
      buildFilterOptions('Directory', elements.directoryFilter);
      buildFilterOptions('Email Type', elements.emailTypeFilter);
    }
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
  applyFilters();
}

function downloadCSV() {
  if (state.data.length === 0) {
    alert('No data to download');
    return;
  }

  // Build CSV content from filtered data
  const csvLines = [COLUMNS.join(',')];
  for (const row of state.filtered) {
    const values = COLUMNS.map((col) => {
      const val = String(row[col] || '');
      // Escape quotes and wrap in quotes if contains comma, quote, or newline
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
  elements.searchInput.addEventListener('input', () => applyFilters());
  elements.directoryFilter.addEventListener('change', () => applyFilters());
  elements.emailTypeFilter.addEventListener('change', () => applyFilters());
  elements.refreshButton.addEventListener('click', () => {
    if (state.viewingJobId) refreshViewedRecords(true); else loadResults();
  });
  elements.downloadButton.addEventListener('click', () => downloadCSV());
  if (elements.refreshJobsButton) elements.refreshJobsButton.addEventListener('click', () => fetchJobs());
  elements.showAllCheckbox.addEventListener('change', (event) => {
    state.showAll = event.target.checked;
    renderTable();
    updateSummary();
  });
  elements.applyDomainsButton.addEventListener('click', () => searchContacts());
  elements.clearDomainsButton.addEventListener('click', () => clearDomains());
  elements.domainFileInput.addEventListener('change', (event) => handleDomainFileUpload(event));
}

async function loadConfig() {
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
