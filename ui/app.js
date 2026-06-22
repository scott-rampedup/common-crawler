// Full record schema (order matches the engine / CSV download). "Image URL" is NOT
// here — it only powers the row thumbnail.
const CSV_COLUMNS = [
  'Time Stamp',
  'Source',
  'Web Source URL',
  'Directory',
  'Path ID',
  'Domain',
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
  'Facebook',
  'Twitter',
  'WhatsApp',
  'Google Maps',
  'vCard',
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
  'Time Stamp', 'Source', 'Directory', 'Path ID', 'Domain', 'Last Path', 'First', 'Last', 'Gender',
  'Position', 'Description', 'Email Address', 'Email Type', 'LinkedIn URL',
  'Facebook', 'Twitter', 'WhatsApp', 'Google Maps',
  'vCard', 'Phone', 'Phone Type', 'Phone Location', 'Phone 2', 'Phone 2 Type'
];

const PAGE_SIZE = 50;

// red location pin (Google-Maps style), hyperlinked to the Google Maps URL
const PIN_SVG = '<svg viewBox="0 0 24 24" width="18" height="18" aria-hidden="true">'
  + '<path fill="currentColor" d="M12 2C8.13 2 5 5.13 5 9c0 5.25 7 13 7 13s7-7.75 7-13c0-3.87-3.13-7-7-7zm0 9.5A2.5 2.5 0 1 1 12 6a2.5 2.5 0 0 1 0 5.5z"/></svg>';
// small "open link" glyph for rows that have a source URL but no image
const LINK_SVG = '<svg viewBox="0 0 24 24" width="16" height="16" aria-hidden="true">'
  + '<path fill="currentColor" d="M14 3v2h3.59l-9.3 9.29 1.42 1.42L19 6.41V10h2V3h-7zM5 5h6V3H5a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-6h-2v6H5V5z"/></svg>';
// address-card glyph for the vCard (.vcf) download link
const VCARD_SVG = '<svg viewBox="0 0 24 24" width="18" height="18" aria-hidden="true">'
  + '<path fill="currentColor" d="M20 4H4a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2V6a2 2 0 0 0-2-2zm0 14H4V6h16v12zM9 12a2 2 0 1 0 0-4 2 2 0 0 0 0 4zm-3.5 4c0-1.66 2.34-2.5 3.5-2.5s3.5.84 3.5 2.5H5.5zM14 9h5v1.5h-5V9zm0 3h5v1.5h-5V12zm0 3h3.5v1.5H14V15z"/></svg>';
// social glyphs (Facebook / X / WhatsApp)
const FB_SVG = '<svg viewBox="0 0 24 24" width="18" height="18" aria-hidden="true"><path fill="currentColor" d="M22 12a10 10 0 1 0-11.56 9.88v-6.99H7.9V12h2.54V9.8c0-2.5 1.49-3.89 3.78-3.89 1.09 0 2.24.2 2.24.2v2.46h-1.26c-1.24 0-1.63.77-1.63 1.56V12h2.78l-.44 2.89h-2.34v6.99A10 10 0 0 0 22 12z"/></svg>';
const TW_SVG = '<svg viewBox="0 0 24 24" width="16" height="16" aria-hidden="true"><path fill="currentColor" d="M18.9 2H22l-7.4 8.45L23 22h-6.84l-5.36-7-6.13 7H1.56l7.9-9.03L1 2h7l4.85 6.4L18.9 2zm-1.2 18h1.9L7.4 4H5.4l12.3 16z"/></svg>';
const WA_SVG = '<svg viewBox="0 0 24 24" width="18" height="18" aria-hidden="true"><path fill="currentColor" d="M.06 24l1.69-6.16A11.87 11.87 0 0 1 .14 11.9C.14 5.34 5.48 0 12.05 0a11.82 11.82 0 0 1 8.41 3.49 11.82 11.82 0 0 1 3.48 8.42c0 6.56-5.34 11.9-11.9 11.9a11.9 11.9 0 0 1-5.69-1.45L.06 24zM6.6 20.2c1.68 1 3.28 1.6 5.44 1.6 5.46 0 9.9-4.44 9.9-9.89A9.86 9.86 0 0 0 12.05 2C6.6 2 2.16 6.44 2.16 11.9c0 2.27.66 3.97 1.77 5.75l-1 3.65 3.67-.96zm11.39-5.46c-.07-.12-.27-.2-.57-.35-.3-.15-1.76-.87-2.03-.97-.27-.1-.47-.15-.67.15-.2.3-.77.97-.94 1.17-.17.2-.35.22-.65.07-.3-.15-1.26-.46-2.4-1.48-.89-.79-1.49-1.77-1.66-2.07-.17-.3-.02-.46.13-.61.13-.13.3-.35.45-.52.15-.17.2-.3.3-.5.1-.2.05-.37-.02-.52-.07-.15-.67-1.61-.92-2.21-.24-.58-.49-.5-.67-.51l-.57-.01c-.2 0-.52.07-.8.37-.27.3-1.04 1.02-1.04 2.48 0 1.46 1.07 2.88 1.22 3.08.15.2 2.1 3.2 5.08 4.49.71.31 1.26.49 1.69.62.71.23 1.36.2 1.87.12.57-.08 1.76-.72 2-1.41.25-.7.25-1.29.17-1.42z"/></svg>';
const SOCIAL = { 'Facebook': { svg: FB_SVG, cls: 'fb-link', title: 'Facebook' }, 'Twitter': { svg: TW_SVG, cls: 'tw-link', title: 'X / Twitter' }, 'WhatsApp': { svg: WA_SVG, cls: 'wa-link', title: 'WhatsApp' } };

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
  viewingDb: false,                // true when the table shows the central database
  lastDbCount: -1,
  dbTotal: 0,                      // server-reported total for the current DB query
};

let dbSearchTimer = null;

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
  elements.dbButton = document.getElementById('dbButton');
  elements.modeIndicator = document.getElementById('modeIndicator');
  elements.searchStatus = document.getElementById('searchStatus');
  elements.domainInput = document.getElementById('domainInput');
  elements.domainFileInput = document.getElementById('domainFileInput');
  elements.applyDomainsButton = document.getElementById('applyDomainsButton');
  elements.clearDomainsButton = document.getElementById('clearDomainsButton');
  elements.liveOnlyCheckbox = document.getElementById('liveOnlyCheckbox');
  elements.modeHint = document.getElementById('modeHint');
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
  if (state.viewingDb) queryDb(); else applyFilters();
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

// Webpage mode: keep the FULL URL per line (path/query intact), one per line.
function parseUrlList(text) {
  return Array.from(
    new Set(
      String(text || '')
        .split(/[\r\n]+/)
        .map((s) => s.trim())
        .filter((s) => s && s.includes('.'))
    )
  );
}

function getSearchMode() {
  const checked = document.querySelector('input[name="searchMode"]:checked');
  const v = checked ? checked.value : 'domain';
  return v === 'webpage' || v === 'sitemap' || v === 'ccdiscover' ? v : 'domain';
}

function updateModeHint() {
  if (!elements.modeHint) return;
  const mode = getSearchMode();
  if (mode === 'sitemap') {
    elements.modeHint.textContent =
      'Sitemap: reads the sitemap(s) and processes the Bio/Contact URLs it finds. Paste XML or sitemap URLs, or upload a .xml / .xml.gz file (index files supported).';
    if (elements.domainInput) {
      elements.domainInput.placeholder = 'Paste sitemap XML, or one sitemap URL per line (e.g. https://example.com/sitemap.xml)';
    }
  } else if (mode === 'ccdiscover') {
    elements.modeHint.textContent =
      'Common Crawl: auto-finds Bio/Contact pages for each domain straight from the Common Crawl archive (no sitemap needed), then processes them. Fast and bypasses live blocks.';
    if (elements.domainInput) elements.domainInput.placeholder = 'Enter one domain per line (e.g. example.com)';
  } else if (mode === 'webpage') {
    elements.modeHint.textContent = 'Webpage: pulls contacts only from the exact URLs you provide (one per line).';
    if (elements.domainInput) elements.domainInput.placeholder = 'Enter one full webpage URL per line';
  } else {
    elements.modeHint.textContent = 'Domain: crawls the whole site for contacts.';
    if (elements.domainInput) elements.domainInput.placeholder = 'Enter one domain or URL per line';
  }
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
  if (state.viewingDb) {
    const total = state.dbTotal;
    const start = total ? (state.page - 1) * PAGE_SIZE + 1 : 0;
    const end = Math.min(state.page * PAGE_SIZE, total);
    elements.tableSummary.innerHTML =
      `Master database · <strong>${total.toLocaleString()}</strong> contacts · showing <strong>${start.toLocaleString()}-${end.toLocaleString()}</strong>`;
    return;
  }
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
  if (state.viewingDb) { queryDb(); return; }      // database view filters server-side
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
  const n = state.viewingDb ? state.dbTotal : state.filtered.length;
  return Math.max(1, Math.ceil(n / PAGE_SIZE));
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
  if (state.viewingDb) { queryDb(); return; }     // fetch that page from the server
  renderTable();
  updateSummary();
}

function renderTable() {
  // clamp page to the available range, then slice this page
  const pages = totalPages();
  if (state.page > pages) state.page = pages;
  if (state.page < 1) state.page = 1;
  const startIdx = (state.page - 1) * PAGE_SIZE;
  // DB view: state.data is already the current server page; job view: slice locally
  const rowsToRender = state.viewingDb ? state.data : state.filtered.slice(startIdx, startIdx + PAGE_SIZE);

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
    const domain = record['Domain'] || parseHostname(pageUrl);
    // when there's no image (or it fails to load), fall back to the site's favicon by domain,
    // then to a link glyph. The thumbnail stays hyperlinked to the Web Source URL below.
    const favicon = domain ? `https://www.google.com/s2/favicons?domain=${encodeURIComponent(domain)}&sz=128` : '';
    const sources = [src, favicon].filter(Boolean);

    let inner = null;
    if (sources.length) {
      const img = document.createElement('img');
      img.className = 'row-photo';
      img.alt = '';
      img.loading = 'lazy';
      img.referrerPolicy = 'no-referrer';
      let si = 0;
      img.src = sources[0];
      img.addEventListener('error', () => {
        si += 1;
        if (si < sources.length) img.src = sources[si];
        else img.replaceWith(makeLinkFallback(!!pageUrl));
      });
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
      } else if (SOCIAL[field]) {
        cell.className = 'social-cell';
        if (value) {
          const a = document.createElement('a');
          a.href = value;
          a.target = '_blank';
          a.rel = 'noopener noreferrer';
          a.className = SOCIAL[field].cls;
          a.title = SOCIAL[field].title;
          a.innerHTML = SOCIAL[field].svg;
          cell.appendChild(a);
        }
      } else if (field === 'vCard') {
        cell.className = 'vcard-cell';
        if (value) {
          const a = document.createElement('a');
          a.href = value;
          a.target = '_blank';
          a.rel = 'noopener noreferrer';
          a.className = 'vcard-link';
          a.title = 'Download vCard (.vcf)';
          a.innerHTML = VCARD_SVG;
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
    state.viewingDb = false;

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

// Sitemap mode: send the pasted/uploaded sitemap(s) to the server, which processes them ONE AT A
// TIME — extracting each sitemap's Bio/Contact URLs and starting a separate 'webpage' job for it
// (so several large sitemaps no longer have to be merged into one job that would fail). `body` is
// text (paste) or an ArrayBuffer (file, possibly .xml.gz — the server gunzips it). directoryFilter
// + liveOnly ride along as query params since the body is the raw sitemap, not JSON.
async function extractSitemapAndRun(body, contentType) {
  try {
    setSearchStatus('Reading sitemap(s) and starting a job per sitemap…');
    const directoryFilter = elements.directoryFilter.value.trim();
    const liveOnly = !!(elements.liveOnlyCheckbox && elements.liveOnlyCheckbox.checked);
    const qs = new URLSearchParams();
    if (directoryFilter) qs.set('directoryFilter', directoryFilter);
    if (liveOnly) qs.set('liveOnly', '1');
    const res = await fetch('/api/sitemap/run?' + qs.toString(), {
      method: 'POST',
      headers: { 'Content-Type': contentType },
      body,
    });
    if (!res.ok) {
      const payload = await res.json().catch(() => ({}));
      throw new Error(payload.error || `HTTP ${res.status}`);
    }
    const data = await res.json();
    const jobs = Array.isArray(data.jobs) ? data.jobs : [];
    const scanned = data.totalUrls || 0;
    const fetched = data.sitemapsFetched || 0;
    const fetchedOk = data.sitemapsOk || 0;
    if (jobs.length === 0) {
      if (fetched > 0 && fetchedOk === 0) {
        setSearchStatus('Couldn’t read the sitemap — the site is likely blocking automated access (e.g. Cloudflare). Open the sitemap in your browser, then paste its XML here or upload the saved .xml file (that skips the fetch).');
      } else if (scanned === 0) {
        setSearchStatus('No URLs found. Make sure the input is a valid <urlset> or <sitemapindex> sitemap.');
      } else {
        setSearchStatus(`Scanned ${scanned} URL${scanned === 1 ? '' : 's'} but none looked like Bio/Contact pages.`);
      }
      return;
    }
    const totalBio = data.totalBioUrls || jobs.reduce((n, j) => n + (j.bioUrls || 0), 0);
    setSearchStatus(`Started ${jobs.length} job${jobs.length === 1 ? '' : 's'} (one per sitemap) for ${totalBio} Bio URL${totalBio === 1 ? '' : 's'}. They run on the server — you can leave this page.`);
    await fetchJobs();
    if (jobs[0] && jobs[0].id) viewJob(jobs[0].id);
  } catch (error) {
    setSearchStatus(`Could not process sitemap: ${error.message}`);
  }
}

// Common Crawl discovery mode: ask the server to find Bio/Contact URLs for the given domains
// straight from the Common Crawl index (one query per domain, no sitemap), then run those URLs
// as a normal 'webpage' job (tagged 'CC Discovery').
async function discoverFromCCAndRun(domains) {
  try {
    setSearchStatus(`Searching Common Crawl for Bio URLs across ${domains.length} domain${domains.length === 1 ? '' : 's'}…`);
    const res = await fetch('/api/cc-discover', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ domains }),
    });
    if (!res.ok) {
      const payload = await res.json().catch(() => ({}));
      throw new Error(payload.error || `HTTP ${res.status}`);
    }
    const data = await res.json();
    const bioUrls = Array.isArray(data.bioUrls) ? data.bioUrls : [];
    const scanned = data.totalUrls || 0;
    if (bioUrls.length === 0) {
      setSearchStatus(scanned === 0
        ? 'None of those domains are in Common Crawl (or have no archived pages). Try Domain or Sitemap mode instead.'
        : `Scanned ${scanned} archived URL${scanned === 1 ? '' : 's'} but none looked like Bio/Contact pages.`);
      return;
    }
    setSearchStatus(`Found ${bioUrls.length} Bio URL${bioUrls.length === 1 ? '' : 's'} in Common Crawl from ${scanned} archived URL${scanned === 1 ? '' : 's'}. Starting job…`);

    const directoryFilter = elements.directoryFilter.value.trim();
    const liveOnly = !!(elements.liveOnlyCheckbox && elements.liveOnlyCheckbox.checked);
    const jobRes = await fetch('/api/jobs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ domains: bioUrls, mode: 'webpage', directoryFilter, liveOnly, type: 'CC Discovery' }),
    });
    if (!jobRes.ok) {
      const payload = await jobRes.json().catch(() => ({}));
      throw new Error(payload.error || `HTTP ${jobRes.status}`);
    }
    const job = await jobRes.json();
    setSearchStatus(`Job started for ${bioUrls.length} Bio URL${bioUrls.length === 1 ? '' : 's'} discovered in Common Crawl. It runs on the server — you can leave this page.`);
    await fetchJobs();
    viewJob(job.id);
  } catch (error) {
    setSearchStatus(`Could not discover from Common Crawl: ${error.message}`);
  }
}

// Start a search as a server-side background job, then watch it via polling.
async function searchContacts() {
  const mode = getSearchMode();
  if (mode === 'sitemap') {
    const text = elements.domainInput.value.trim();
    if (!text) {
      setSearchStatus('Paste sitemap XML or sitemap URL(s) above, or upload a sitemap file.');
      return;
    }
    await extractSitemapAndRun(text, 'text/plain; charset=utf-8');
    return;
  }
  if (mode === 'ccdiscover') {
    const domains = parseDomainList(elements.domainInput.value);
    if (domains.length === 0) {
      setSearchStatus('Enter one or more domains to discover Bio URLs from Common Crawl.');
      return;
    }
    await discoverFromCCAndRun(domains);
    return;
  }
  const domains = mode === 'webpage'
    ? parseUrlList(elements.domainInput.value)
    : parseDomainList(elements.domainInput.value);
  if (domains.length === 0) {
    setSearchStatus(mode === 'webpage' ? 'Please enter one or more webpage URLs.' : 'Please enter one or more domains to search.');
    return;
  }
  const directoryFilter = elements.directoryFilter.value.trim();
  const liveOnly = !!(elements.liveOnlyCheckbox && elements.liveOnlyCheckbox.checked);
  const unit = mode === 'webpage' ? 'webpage' : 'domain';

  try {
    setSearchStatus(`Starting a job for ${domains.length} ${unit}${domains.length === 1 ? '' : 's'}...`);
    const response = await fetch('/api/jobs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ domains, directoryFilter, liveOnly, mode, type: mode === 'webpage' ? 'Webpages' : 'Domains' }),
    });
    if (!response.ok) {
      const payload = await response.json().catch(() => ({}));
      throw new Error(payload.error || `HTTP ${response.status}`);
    }
    const job = await response.json();
    setSearchStatus(`Job started for ${domains.length} ${unit}${domains.length === 1 ? '' : 's'}. It runs on the server — you can leave this page.`);
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

    const unit = job.type === 'Domains' ? 'domain' : job.type === 'Google Sheet' ? 'row' : 'URL';
    const progressBit = job.status === 'running'
      ? `<div class="progress-track"><div class="progress-fill" style="width:${pct}%"></div></div>
         <span class="job-meta">${job.done}/${job.total} ${unit}s</span>`
      : `<span class="job-meta">${job.done}/${job.total} ${unit}s</span>`;

    const liveOnlyBit = job.liveOnly ? ' <span class="job-meta">· live-only</span>' : '';
    const errorBit = job.error ? `<span class="job-meta" style="color:#b91c1c">${escapeHtml(job.error)}</span>` : '';

    card.innerHTML = `
      <div class="job-main">
        <div class="job-line">
          <span class="badge ${job.status}">${job.status}</span>
          <span class="job-type">${escapeHtml(job.type || '')}</span>
          <strong class="job-name">${escapeHtml(job.name || shortJobLabel(job))}</strong>
          <button class="job-rename" type="button" title="Rename job" aria-label="Rename job">✎</button>${liveOnlyBit}
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

    const renameBtn = card.querySelector('.job-rename');
    if (renameBtn) renameBtn.addEventListener('click', () => renameJobUI(job.id, job.name || ''));

    const actions = card.querySelector('.job-actions');

    // The Google Sheet job is a managed sync mirror (its contacts live in the Master DB), so it
    // shows status only — no View/Download/Resume/Stop/Delete.
    if (job.type !== 'Google Sheet') {
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
    }

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

async function renameJobUI(id, currentName) {
  const name = window.prompt('Job name:', currentName || '');
  if (name === null) return;                         // cancelled
  try {
    const res = await fetch(`/api/jobs/${id}/name`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name }),
    });
    if (!res.ok) { const p = await res.json().catch(() => ({})); throw new Error(p.error || `HTTP ${res.status}`); }
    await fetchJobs();
  } catch (e) { setSearchStatus(`Could not rename job: ${e.message}`); }
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
  state.viewingDb = false;
  state.lastViewedCount = -1;
  state.page = 1;
  await refreshViewedRecords(true);
  renderJobs();
}

// Point the table at the central database (server-side paginated/filtered).
async function viewDatabase() {
  state.viewingDb = true;
  state.viewingJobId = null;
  state.page = 1;
  setSearchStatus('Loading master database…');
  createHeader();
  await populateDbFilters();      // fill dropdowns from DISTINCT values in the DB
  await queryDb();
  renderJobs();                   // clear any job's "viewing" highlight
}

// Gather the current controls into DB query params.
function dbQueryParams() {
  const p = new URLSearchParams();
  p.set('page', String(state.page));
  p.set('pageSize', String(PAGE_SIZE));
  const s = elements.searchInput.value.trim();
  if (s) p.set('search', s);
  if (elements.directoryFilter.value) p.set('directory', elements.directoryFilter.value);
  if (elements.emailTypeFilter.value) p.set('emailType', elements.emailTypeFilter.value);
  if (elements.phoneTypeFilter && elements.phoneTypeFilter.value) p.set('phoneType', elements.phoneTypeFilter.value);
  const g = elements.genderFilter ? elements.genderFilter.value : 'na';
  if (g && g !== 'na') p.set('gender', g);
  if (elements.linkedinRequired && elements.linkedinRequired.checked) p.set('linkedin', '1');
  if (state.sort.column) { p.set('sort', state.sort.column); p.set('dir', String(state.sort.dir)); }
  return p;
}

async function queryDb() {
  try {
    const res = await fetch('/api/db/query?' + dbQueryParams().toString());
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const out = await res.json();
    state.data = decorateRows(out.rows || []);
    state.dbTotal = out.total || 0;
    renderTable();
    updateSummary();
    if (elements.viewingIndicator) {
      elements.viewingIndicator.classList.remove('hidden');
      elements.viewingIndicator.innerHTML = `Viewing: <strong>Master database</strong> — ${state.dbTotal.toLocaleString()} contact(s) match`;
    }
  } catch (error) { setSearchStatus(`Could not query database: ${error.message}`); }
}

async function populateDbFilters() {
  try {
    const res = await fetch('/api/db/facets');
    if (!res.ok) return;
    const f = await res.json();
    const fill = (sel, values, label) => {
      if (!sel) return;
      const prev = sel.value;
      sel.innerHTML = `<option value="">All ${label}</option>`;
      for (const v of (values || [])) {
        const o = document.createElement('option'); o.value = v; o.textContent = v; sel.appendChild(o);
      }
      if (prev && (values || []).includes(prev)) sel.value = prev;
    };
    fill(elements.directoryFilter, f.directory, 'directories');
    fill(elements.emailTypeFilter, f.emailType, 'email types');
    fill(elements.phoneTypeFilter, f.phoneType, 'phone types');
  } catch (e) { /* ignore */ }
}

async function refreshDbCount() {
  try {
    const res = await fetch('/api/db/stats');
    if (!res.ok) return;
    const { total } = await res.json();
    if (elements.dbButton) elements.dbButton.textContent = `Master database (${total.toLocaleString()})`;
    if (state.viewingDb && total !== state.lastDbCount) { state.lastDbCount = total; queryDb(); }
    else state.lastDbCount = total;
  } catch (e) { /* ignore */ }
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
  refreshDbCount();
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

  // Sitemap mode: files may be gzipped (.xml.gz) — send raw bytes so the server can gunzip + parse.
  if (getSearchMode() === 'sitemap') {
    const reader = new FileReader();
    reader.onload = () => { extractSitemapAndRun(reader.result, 'application/octet-stream'); };
    reader.readAsArrayBuffer(file);
    return;
  }

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
  // Database view: export the whole (filtered) DB straight from the server (streamed).
  if (state.viewingDb) {
    const p = dbQueryParams();
    p.delete('page'); p.delete('pageSize'); p.delete('sort'); p.delete('dir');
    window.location.href = '/api/db/export.csv?' + p.toString();
    return;
  }
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
  elements.searchInput.addEventListener('input', () => {
    state.page = 1;
    if (state.viewingDb) { clearTimeout(dbSearchTimer); dbSearchTimer = setTimeout(queryDb, 300); }
    else applyFilters();
  });
  elements.directoryFilter.addEventListener('change', () => { state.page = 1; applyFilters(); });
  elements.emailTypeFilter.addEventListener('change', () => { state.page = 1; applyFilters(); });
  if (elements.genderFilter) elements.genderFilter.addEventListener('change', () => { state.page = 1; applyFilters(); });
  if (elements.phoneTypeFilter) elements.phoneTypeFilter.addEventListener('change', () => { state.page = 1; applyFilters(); });
  if (elements.linkedinRequired) elements.linkedinRequired.addEventListener('change', () => { state.page = 1; applyFilters(); });
  elements.refreshButton.addEventListener('click', () => {
    if (state.viewingDb) viewDatabase();
    else if (state.viewingJobId) refreshViewedRecords(true);
    else loadResults();
  });
  if (elements.dbButton) elements.dbButton.addEventListener('click', () => viewDatabase());
  elements.downloadButton.addEventListener('click', () => downloadCSV());
  if (elements.refreshJobsButton) elements.refreshJobsButton.addEventListener('click', () => fetchJobs());
  if (elements.jobsToggle) elements.jobsToggle.addEventListener('click', () => toggleJobs());
  elements.applyDomainsButton.addEventListener('click', () => searchContacts());
  elements.clearDomainsButton.addEventListener('click', () => clearDomains());
  elements.domainFileInput.addEventListener('change', (event) => handleDomainFileUpload(event));
  document.querySelectorAll('input[name="searchMode"]').forEach((el) =>
    el.addEventListener('change', updateModeHint));
  updateModeHint();

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
  refreshDbCount();
  // ?view=db (e.g. the "Master database" link on the Search page) opens the DB view directly;
  // otherwise open the newest job, or fall back to the legacy cc-results.csv view.
  if (new URLSearchParams(location.search).get('view') === 'db') {
    viewDatabase();
  } else if (state.jobs.length) {
    viewJob(state.jobs[0].id);
  } else {
    loadResults();
  }

  jobsPollTimer = setInterval(pollJobs, 1500);   // keep jobs + the viewed job live
});
