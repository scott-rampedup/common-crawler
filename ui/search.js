// Search Database page — queries the central (master) contacts DB server-side, with
// row checkboxes (cross-page selection), sortable columns, and CSV download.

// Full CSV schema (matches the engine / main-page download).
const CSV_COLUMNS = [
  'Time Stamp', 'Source', 'Web Source URL', 'Directory', 'Path ID', 'Domain', 'Last Path',
  'Bio Check', 'First', 'Last', 'Gender', 'Title', 'Position', 'Description', 'Email Address',
  'Email Type', 'LinkedIn URL', 'Facebook', 'Twitter', 'WhatsApp', 'Google Maps', 'vCard', 'Phone', 'Phone Type', 'Phone Location',
  'Phone 2', 'Phone 2 Type', 'Type',
  'Industry', 'Company Size', 'Company HQ', 'Founded', 'Company Name', 'Company LinkedIn'
];

// On-screen columns, in display order. Each: key (record field), label (header text),
// type (how the cell renders), sortable (whether the header sorts server-side).
const COLUMNS = [
  { key: 'Image URL',      label: '',              type: 'image',       sortable: false },
  { key: 'Last',           label: 'Contact Name',  type: 'contactname', sortable: true  },
  { key: 'Company Name',   label: 'Company',       type: 'companyname', sortable: false, cls: 'company-col' },   // wider + bold; linked to the company website (Domain); falls back to the website when no name
  { key: 'Position',       label: 'Position',      type: 'position', sortable: true  },
  { key: 'Email Address',  label: 'Email Address', type: 'text',     sortable: true  },
  { key: 'Email Type',     label: 'Type',          type: 'text',     sortable: true  },
  { key: 'New Hire',       label: 'New',           type: 'newhire',  sortable: false },
  { key: 'updated_at',     label: 'Last Updated',  type: 'date',     sortable: true  },   // sorts server-side (SORTABLE.updated_at)
  { key: 'LinkedIn URL',   label: 'LinkedIn',      type: 'linkedin', sortable: false },
  { key: 'Facebook',       label: 'Facebook',      type: 'social',   sortable: false },
  { key: 'Twitter',        label: 'Twitter',       type: 'social',   sortable: false },
  { key: 'WhatsApp',       label: 'WhatsApp',      type: 'social',   sortable: false },
  { key: 'Google Maps',    label: 'Google Maps',   type: 'maps',     sortable: false },
  { key: 'Phone Location', label: 'Location',      type: 'location', sortable: true  },
  { key: 'vCard',          label: 'vCard',         type: 'vcard',    sortable: false },
  { key: 'Phone',          label: 'Phone',         type: 'text',     sortable: true  },
  { key: 'Phone Type',     label: 'Type',          type: 'text',     sortable: true  },
  { key: 'Phone 2',        label: 'Phone 2',       type: 'text',     sortable: true  },
  { key: 'Phone 2 Type',   label: 'Type',          type: 'text',     sortable: true  },
  { key: 'Industry',       label: 'Industry',      type: 'text',     sortable: false, tc: true },
  { key: 'Company Size',   label: 'Co. Size',      type: 'text',     sortable: false, cls: 'cosize-col' },
  { key: '_edit',          label: 'Edit',          type: 'edit',     sortable: false },
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

// Bulk edit sets ONE field to ONE shared value across every selected record, so identity fields
// (name/email/phone/LinkedIn) — unique per person — are excluded. The server enforces the same list.
// Company, Gender, Email Pattern and Email Domain get special server-side handling.
const BULK_FIELDS = [
  { key: 'Company',        label: 'Company' },
  { key: 'Position',       label: 'Position (title)' },
  { key: 'Gender',         label: 'Gender' },
  { key: 'Email Type',     label: 'Email Type' },
  { key: 'Email Pattern',  label: 'Email Pattern (modelled email)' },
  { key: 'Email Domain',   label: 'Email Domain (modelled email)' },
  { key: 'Phone Type',     label: 'Phone Type' },
  { key: 'Phone Location', label: 'Phone Location' },
  { key: 'Domain',         label: 'Domain (website)' },
  { key: 'Description',    label: 'Description' },
];

// Email-pattern dropdown: label shows the token form + an example; value is the email-pattern.js template.
const EMAIL_PATTERN_OPTIONS = [
  ['{first}.{last}', 'First . last  —  {fn}.{ln}  (e.g. john.doe)'],
  ['{first}{last}',  'First last  —  {fn}{ln}  (e.g. johndoe)'],
  ['{f}{last}',      'First-initial + last  —  {fi}{ln}  (e.g. jdoe)'],
  ['{f}.{last}',     'First-initial . last  —  {fi}.{ln}  (e.g. j.doe)'],
  ['{first}_{last}', 'First _ last  —  {fn}_{ln}  (e.g. john_doe)'],
  ['{first}',        'First name  —  {fn}  (e.g. john)'],
];

const PAGE_SIZE = 50;

// Proper-case for DISPLAY ONLY (never mutates stored data / filters). Only reformats values that are
// all-UPPERCASE or all-lowercase — already mixed-case text (e.g. "McDonald", "eBay") is left as-is.
// Keeps known business acronyms upper and small joining words lower. Mirrors the Company Crawler tc().
const TC_UP = new Set(['llc', 'inc', 'llp', 'plc', 'pllc', 'pc', 'ltd', 'usa', 'us', 'uk', 'uae', 'eu', 'ny', 'la', 'dc', 'it', 'hr', 'pr', 'ceo', 'cfo', 'coo', 'cto', 'vp', 'svp', 'evp']);
const TC_LOW = new Set(['of', 'and', 'the', 'for', 'to', 'in', 'on', 'at', 'a', 'an', 'or', 'de', 'du', 'van', 'von']);
function properCase(s) {
  s = String(s == null ? '' : s);
  if (!s) return '';
  const letters = s.replace(/[^A-Za-z]/g, '');
  if (!letters || (letters !== letters.toUpperCase() && letters !== letters.toLowerCase())) return s;  // already mixed-case → leave it
  return s.toLowerCase().split(/(\s+|-|\/|,|\.)/).map((w, i) => {
    if (/^(\s+|-|\/|,|\.)$/.test(w) || !w) return w;
    if (TC_UP.has(w)) return w.toUpperCase();
    if (i > 0 && TC_LOW.has(w)) return w;
    return w.charAt(0).toUpperCase() + w.slice(1);
  }).join('');
}

const PIN_SVG = '<svg viewBox="0 0 24 24" width="18" height="18" aria-hidden="true">'
  + '<path fill="currentColor" d="M12 2C8.13 2 5 5.13 5 9c0 5.25 7 13 7 13s7-7.75 7-13c0-3.87-3.13-7-7-7zm0 9.5A2.5 2.5 0 1 1 12 6a2.5 2.5 0 0 1 0 5.5z"/></svg>';

// LinkedIn "in" glyph for the LinkedIn column header + cells.
const LINKEDIN_SVG = '<svg viewBox="0 0 24 24" width="18" height="18" aria-hidden="true">'
  + '<path fill="currentColor" d="M19 3a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h14zM8.34 9.67H5.67V18h2.67V9.67zM7 5.67a1.55 1.55 0 1 0 0 3.1 1.55 1.55 0 0 0 0-3.1zM18.33 18v-4.57c0-2.45-1.31-3.59-3.06-3.59-1.41 0-2.04.78-2.39 1.33v-1.5h-2.67V18h2.67v-4.65c0-.25.02-.49.09-.67.2-.49.65-1 1.4-1 .99 0 1.38.75 1.38 1.85V18h2.67z"/></svg>';

// address-card glyph for the vCard (.vcf) download column header + cells.
const VCARD_SVG = '<svg viewBox="0 0 24 24" width="18" height="18" aria-hidden="true">'
  + '<path fill="currentColor" d="M20 4H4a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2V6a2 2 0 0 0-2-2zm0 14H4V6h16v12zM9 12a2 2 0 1 0 0-4 2 2 0 0 0 0 4zm-3.5 4c0-1.66 2.34-2.5 3.5-2.5s3.5.84 3.5 2.5H5.5zM14 9h5v1.5h-5V9zm0 3h5v1.5h-5V12zm0 3h3.5v1.5H14V15z"/></svg>';
// social glyphs (Facebook / X / WhatsApp) for the header + cells.
const FB_SVG = '<svg viewBox="0 0 24 24" width="18" height="18" aria-hidden="true"><path fill="currentColor" d="M22 12a10 10 0 1 0-11.56 9.88v-6.99H7.9V12h2.54V9.8c0-2.5 1.49-3.89 3.78-3.89 1.09 0 2.24.2 2.24.2v2.46h-1.26c-1.24 0-1.63.77-1.63 1.56V12h2.78l-.44 2.89h-2.34v6.99A10 10 0 0 0 22 12z"/></svg>';
const TW_SVG = '<svg viewBox="0 0 24 24" width="16" height="16" aria-hidden="true"><path fill="currentColor" d="M18.9 2H22l-7.4 8.45L23 22h-6.84l-5.36-7-6.13 7H1.56l7.9-9.03L1 2h7l4.85 6.4L18.9 2zm-1.2 18h1.9L7.4 4H5.4l12.3 16z"/></svg>';
const WA_SVG = '<svg viewBox="0 0 24 24" width="18" height="18" aria-hidden="true"><path fill="currentColor" d="M.06 24l1.69-6.16A11.87 11.87 0 0 1 .14 11.9C.14 5.34 5.48 0 12.05 0a11.82 11.82 0 0 1 8.41 3.49 11.82 11.82 0 0 1 3.48 8.42c0 6.56-5.34 11.9-11.9 11.9a11.9 11.9 0 0 1-5.69-1.45L.06 24zM6.6 20.2c1.68 1 3.28 1.6 5.44 1.6 5.46 0 9.9-4.44 9.9-9.89A9.86 9.86 0 0 0 12.05 2C6.6 2 2.16 6.44 2.16 11.9c0 2.27.66 3.97 1.77 5.75l-1 3.65 3.67-.96zm11.39-5.46c-.07-.12-.27-.2-.57-.35-.3-.15-1.76-.87-2.03-.97-.27-.1-.47-.15-.67.15-.2.3-.77.97-.94 1.17-.17.2-.35.22-.65.07-.3-.15-1.26-.46-2.4-1.48-.89-.79-1.49-1.77-1.66-2.07-.17-.3-.02-.46.13-.61.13-.13.3-.35.45-.52.15-.17.2-.3.3-.5.1-.2.05-.37-.02-.52-.07-.15-.67-1.61-.92-2.21-.24-.58-.49-.5-.67-.51l-.57-.01c-.2 0-.52.07-.8.37-.27.3-1.04 1.02-1.04 2.48 0 1.46 1.07 2.88 1.22 3.08.15.2 2.1 3.2 5.08 4.49.71.31 1.26.49 1.69.62.71.23 1.36.2 1.87.12.57-.08 1.76-.72 2-1.41.25-.7.25-1.29.17-1.42z"/></svg>';
const SOCIAL = { 'Facebook': { svg: FB_SVG, cls: 'fb-link', title: 'Facebook' }, 'Twitter': { svg: TW_SVG, cls: 'tw-link', title: 'X / Twitter' }, 'WhatsApp': { svg: WA_SVG, cls: 'wa-link', title: 'WhatsApp' } };

const state = {
  rows: [],                  // current page of records
  total: 0,                  // server-reported match count
  approx: null,              // null = exact; 'estimate' (~) for unfiltered total; 'capped' (+) past 10k
  page: 1,
  sort: { column: null, dir: 1 },   // dir: 1 asc, -1 desc
  selected: new Map(),       // email(lower) -> record, persists across pages
  allMatching: false,        // true when EVERY matching record (across pages) is selected
};

const MAX_SELECT_ALL = 10000;   // cap on "select all matching" so the browser stays responsive

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
  el.fLocation = $('f-location');
  el.fEmailType = $('f-emailType');
  el.fPhoneType = $('f-phoneType');
  el.fType = $('f-type');
  el.fGender = $('f-gender');
  el.fIndustry = $('f-industry');
  el.fCompanySize = $('f-companySize');
  el.fCompanyLocation = $('f-companyLocation');
  el.fFoundedMin = $('f-foundedMin');
  el.fFoundedMax = $('f-foundedMax');
  el.fLinkedin = $('f-linkedin');
  el.fNewHire = $('f-newhire');
  el.applyBtn = $('applyBtn');
  el.clearBtn = $('clearBtn');
  el.headerRow = $('headerRow');
  el.body = $('resultsBody');
  el.summary = $('resultsSummary');
  el.selectedInfo = $('selectedInfo');
  el.selectionBanner = $('selectionBanner');
  el.downloadBtn = $('downloadBtn');
  el.editBtn = $('editBtn');
  el.bulkEditBtn = $('bulkEditBtn');
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
    else if (col.type === 'lastpath' || col.type === 'contactname') th.classList.add('lastpath-col');
    else if (col.type === 'position') th.classList.add('position-col');
    else if (col.type === 'location') th.classList.add('location-col');
    else if (col.type === 'edit') th.classList.add('row-edit');   // admin-only column (hidden via CSS otherwise)
    if (col.type === 'linkedin' || col.type === 'maps' || col.type === 'vcard' || col.type === 'social') th.classList.add('icon-col');
    if (col.cls) th.classList.add(col.cls);

    if (col.type === 'linkedin') {
      th.innerHTML = LINKEDIN_SVG;        // header shows the LinkedIn icon, not text
      th.title = 'LinkedIn';
    } else if (col.type === 'social') {
      th.innerHTML = SOCIAL[col.key].svg; // header shows the platform icon
      th.title = SOCIAL[col.key].title;
    } else if (col.type === 'maps') {
      th.innerHTML = PIN_SVG;             // header shows a map pin, not text
      th.title = 'Google Maps';
    } else if (col.type === 'vcard') {
      th.innerHTML = VCARD_SVG;           // header shows an address-card icon, not text
      th.title = 'vCard (.vcf)';
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
      else { state.selected.delete(email); state.allMatching = false; }   // un-ticking one exits "all matching"
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
      if (col.cls) cell.classList.add(col.cls);

      if (col.type === 'image') {
        // thumbnail: Image URL, else the site's favicon (by domain), hyperlinked to the Web
        // Source URL. Hover shows Description, else Title. Falls through to nothing if all fail.
        cell.className = 'photo-cell';
        const src = record['Image URL'];
        const pageUrl = record['Web Source URL'];
        const domain = record['Domain'] || String(pageUrl || '').replace(/^https?:\/\//i, '').split('/')[0].replace(/^www\./i, '');
        const favicon = domain ? `https://www.google.com/s2/favicons?domain=${encodeURIComponent(domain)}&sz=128` : '';
        const tip = record['Description'] || record['Title'] || '';
        const sources = [src, favicon].filter(Boolean);
        if (sources.length) {
          const img = document.createElement('img');
          img.className = 'row-photo';
          img.alt = '';
          img.loading = 'lazy';
          img.referrerPolicy = 'no-referrer';
          if (tip) img.title = tip;
          let si = 0;
          img.src = sources[0];
          img.addEventListener('error', () => {
            si += 1;
            if (si < sources.length) img.src = sources[si];
            else { const w = img.closest('a') || img; w.remove(); }
          });
          if (pageUrl) {
            const a = document.createElement('a');
            a.href = pageUrl; a.target = '_blank'; a.rel = 'noopener noreferrer';
            a.className = 'photo-link'; a.title = 'Open source page';
            a.appendChild(img);
            cell.appendChild(a);
          } else {
            cell.appendChild(img);
          }
        }
      } else if (col.type === 'contactname') {
        // Contact Name = First + Last, hyperlinked to the Web Source URL (frozen left column)
        cell.className = 'lastpath-cell';
        const name = properCase(((record['First'] || '') + ' ' + (record['Last'] || '')).trim());
        const pageUrl = record['Web Source URL'];
        if (name && pageUrl) {
          const a = document.createElement('a');
          a.href = pageUrl; a.target = '_blank'; a.rel = 'noopener noreferrer';
          a.className = 'domain-link'; a.title = name; a.textContent = name;
          cell.appendChild(a);
        } else {
          cell.textContent = name;
          if (name) cell.title = name;
        }
      } else if (col.type === 'companyname') {
        // Company Name hyperlinked to the company website (the contact's Domain). If there is no
        // company name, show the website itself as the name.
        const dom = record['Domain'] || '';
        const nameText = record['Company Name'] ? properCase(record['Company Name']) : dom;
        if (nameText && dom) {
          const a = document.createElement('a');
          a.href = /^https?:\/\//i.test(dom) ? dom : `https://${dom}`;
          a.target = '_blank'; a.rel = 'noopener noreferrer';
          a.className = 'domain-link'; a.title = nameText; a.textContent = nameText;
          cell.appendChild(a);
        } else {
          cell.textContent = nameText || '';
          if (nameText) cell.title = nameText;
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
      } else if (col.type === 'vcard') {
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
      } else if (col.type === 'social') {
        cell.className = 'social-cell';
        if (value) {
          const a = document.createElement('a');
          a.href = value;
          a.target = '_blank';
          a.rel = 'noopener noreferrer';
          a.className = SOCIAL[col.key].cls;
          a.title = SOCIAL[col.key].title;
          a.innerHTML = SOCIAL[col.key].svg;
          cell.appendChild(a);
        }
      } else if (col.type === 'newhire') {
        cell.style.textAlign = 'center';
        if (value === 'Y') {
          const s = document.createElement('span');
          s.className = 'newhire-pill';
          s.textContent = 'NEW';
          cell.appendChild(s);
        }
      } else if (col.type === 'edit') {
        cell.className = 'row-edit';
        cell.style.textAlign = 'center';
        const b = document.createElement('button');
        b.type = 'button'; b.className = 'row-edit-btn'; b.title = 'Edit'; b.textContent = '✎';
        b.addEventListener('click', (e) => { e.stopPropagation(); openEditModal([record]); });
        cell.appendChild(b);
      } else if (col.type === 'date') {
        // Last-updated timestamp → show the date (YYYY-MM-DD); full timestamp on hover.
        cell.style.whiteSpace = 'nowrap';
        if (value) { cell.textContent = String(value).slice(0, 10); cell.title = String(value); }
      } else if (col.type === 'position') {
        cell.className = 'position-cell';
        cell.textContent = properCase(value || '');
      } else if (col.type === 'location') {
        cell.className = 'location-cell';
        cell.textContent = properCase(value || '');
      } else {
        cell.textContent = col.tc ? properCase(value || '') : (value || '');
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
  if (!checked) state.allMatching = false;       // deselecting the page exits "all matching" mode
  for (const record of state.rows) {
    const email = norm(record['Email Address']);
    if (checked) state.selected.set(email, record);
    else state.selected.delete(email);
  }
  renderRows();
}

// Empty the selection (and exit "all matching" mode), then re-render.
function clearSelection() {
  state.selected.clear();
  state.allMatching = false;
  renderRows();
  updateSelectedInfo();
}

// The thin bar above the table: offers "select all N matching" once the visible page is fully
// selected, and shows the all-matching state with a Clear action. (Gmail-style.)
function renderSelectionBanner() {
  const b = el.selectionBanner;
  if (!b) return;
  b.innerHTML = '';
  const total = state.total;
  const pageCount = state.rows.length;
  const pageAllSelected = pageCount > 0 && state.rows.every((r) => state.selected.has(norm(r['Email Address'])));

  const addText = (html) => { const s = document.createElement('span'); s.innerHTML = html; b.appendChild(s); };
  const addLink = (label, fn) => {
    const a = document.createElement('button');
    a.type = 'button'; a.className = 'link-btn'; a.textContent = label;
    a.addEventListener('click', fn); b.appendChild(a);
  };

  if (state.allMatching) {
    addText(`All <strong>${total.toLocaleString()}</strong> matching contacts are selected.`);
    addLink('Clear selection', clearSelection);
    b.hidden = false;
  } else if (pageAllSelected && total > pageCount) {
    addText(`All <strong>${pageCount}</strong> on this page are selected.`);
    addLink(`Select all ${total.toLocaleString()} matching`, selectAllMatching);
    b.hidden = false;
  } else {
    b.hidden = true;
  }
}

// Pull EVERY matching record (across all pages) into the selection so the bulk actions operate on
// the whole filtered set, not just the visible 50. Paginates the same query the table uses.
async function selectAllMatching() {
  const total = state.total;
  if (!total) return;
  if (total > MAX_SELECT_ALL) {
    alert(`${total.toLocaleString()} matching contacts exceeds the ${MAX_SELECT_ALL.toLocaleString()}-record selection limit.\n\n`
      + `• Download (with nothing selected) still exports all ${total.toLocaleString()}.\n`
      + `• To Edit, AI Search, or Delete a set this large, narrow your filters first.`);
    return;
  }
  const b = el.selectionBanner;
  b.hidden = false; b.textContent = 'Selecting…';
  try {
    const pageSize = 500;
    const pages = Math.ceil(total / pageSize);
    for (let pg = 1; pg <= pages; pg++) {
      const res = await fetch('/api/db/query?' + buildParams({ page: pg, pageSize }).toString());
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const out = await res.json();
      for (const r of (out.rows || [])) state.selected.set(norm(r['Email Address']), r);
      b.textContent = `Selecting… ${Math.min(pg * pageSize, total).toLocaleString()} of ${total.toLocaleString()}`;
    }
    state.allMatching = true;
    renderRows();              // re-tick the visible checkboxes
    updateSelectedInfo();      // also re-renders the banner
  } catch (e) {
    alert('Could not select all matching: ' + e.message);
    renderSelectionBanner();
  }
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
  if (el.bulkEditBtn) el.bulkEditBtn.disabled = n === 0;
  if (el.aiBtn) el.aiBtn.disabled = n === 0;
  if (el.deleteBtn) el.deleteBtn.disabled = n === 0;
  renderSelectionBanner();
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
  const loc = el.fLocation.value.trim();
  if (loc) p.set('location', loc);
  if (el.fEmailType.value) p.set('emailType', el.fEmailType.value);
  if (el.fPhoneType.value) p.set('phoneType', el.fPhoneType.value);
  if (el.fType.value) p.set('type', el.fType.value);
  const g = el.fGender.value;
  if (g && g !== 'na') p.set('gender', g);
  if (el.fIndustry && el.fIndustry.value) p.set('industry', el.fIndustry.value);
  if (el.fCompanySize && el.fCompanySize.value) p.set('companySize', el.fCompanySize.value);
  const cloc = el.fCompanyLocation && el.fCompanyLocation.value.trim();
  if (cloc) p.set('companyLocation', cloc);
  const fmin = el.fFoundedMin && el.fFoundedMin.value.trim();
  if (fmin) p.set('foundedMin', fmin);
  const fmax = el.fFoundedMax && el.fFoundedMax.value.trim();
  if (fmax) p.set('foundedMax', fmax);
  if (el.fLinkedin.checked) p.set('linkedin', '1');
  if (el.fNewHire && el.fNewHire.checked) p.set('newHire', '1');
  if (state.sort.column) { p.set('sort', state.sort.column); p.set('dir', String(state.sort.dir)); }
  if (extra) for (const k of Object.keys(extra)) p.set(k, extra[k]);
  return p;
}

function totalPages() {
  // The count is exact, but OpenSearch caps from+size at max_result_window (50k) — pages beyond that
  // aren't reachable via prev/next (narrow with filters, or export for the full set). Cap the navigable
  // page count to the window so Next/Last stop at the last real page instead of repeating clamped rows.
  const navMax = Math.floor(50000 / PAGE_SIZE);
  return Math.max(1, Math.min(Math.ceil(state.total / PAGE_SIZE), navMax));
}

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
  // total may be inexact: 'estimate' = planner row estimate for the unfiltered view (~),
  // 'capped' = more than the 10k count cap (+). Both keep Search fast on a huge table.
  const label = state.approx === 'estimate' ? `~${total.toLocaleString()}`
    : state.approx === 'capped' ? `${total.toLocaleString()}+`
    : total.toLocaleString();
  el.summary.innerHTML = total
    ? `<strong>${label}</strong> contacts match · showing <strong>${start.toLocaleString()}–${end.toLocaleString()}</strong>`
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
    state.approx = out.approx || null;
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
    fill(el.fPhoneType, f.phoneType, 'phone types');
    fill(el.fType, f.type, 'types');
    if (el.fIndustry) fill(el.fIndustry, f.industry, 'industries');
    if (el.fCompanySize) fill(el.fCompanySize, f.companySize, 'sizes');
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

  // "all matching" or no explicit selection -> stream the full filtered set from the server
  // (exact + unlimited, so it works even beyond the in-browser selection cap).
  if (state.allMatching || state.selected.size === 0) {
    const p = buildParams();
    p.delete('sort'); p.delete('dir');
    window.location.href = '/api/db/export.csv?' + p.toString();
    return;
  }

  // an explicit subset is selected -> export exactly those rows (client-side, across pages)
  const lines = [CSV_COLUMNS.join(',')];
  for (const r of state.selected.values()) {
    lines.push(CSV_COLUMNS.map((c) => csvCell(r[c])).join(','));
  }
  triggerDownload(lines.join('\n'), `contacts-selected-${today}.csv`);
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
function openEditModal(recordsArg) {
  const records = (recordsArg && recordsArg.length) ? recordsArg : [...state.selected.values()];
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
      state.allMatching = false;
      updateSelectedInfo();
      query();
    } catch (e) {
      saveBtn.disabled = false; cancelBtn.disabled = false;
      saveBtn.textContent = `Save ${records.length} record(s)`;
      alert('Save failed: ' + e.message);
    }
  });
}

// ---- bulk edit: set ONE field to ONE value across every selected record ----
function openBulkEditModal() {
  const emails = [...state.selected.keys()].filter(Boolean);
  if (!emails.length) return;
  const nStr = emails.length.toLocaleString();

  let fieldSel, valueInput, valueWrap, noteEl;
  const rebuildValue = () => {
    valueWrap.innerHTML = '';
    const f = fieldSel.value;
    if (f === 'Gender') {
      valueInput = document.createElement('select');
      [['M', 'Male (M)'], ['F', 'Female (F)']].forEach(([v, t]) => { const o = document.createElement('option'); o.value = v; o.textContent = t; valueInput.appendChild(o); });
    } else if (f === 'Email Pattern') {
      valueInput = document.createElement('select');
      EMAIL_PATTERN_OPTIONS.forEach(([v, t]) => { const o = document.createElement('option'); o.value = v; o.textContent = t; valueInput.appendChild(o); });
    } else if (f === 'Description') {
      valueInput = document.createElement('textarea'); valueInput.rows = 3;
      valueInput.placeholder = 'New value (leave blank to clear the field)';
    } else {
      valueInput = document.createElement('input'); valueInput.type = 'text';
      valueInput.placeholder = f === 'Email Domain' ? 'e.g. acme.com' : 'New value (leave blank to clear the field)';
    }
    valueWrap.appendChild(valueInput);
    let msg = '';
    if (f === 'Email Pattern' || f === 'Email Domain') msg = 'Only modelled or blank emails are rewritten — verified emails are left as-is. The pattern + domain are also saved to the company for future modelling.';
    else if (f === 'Company') msg = 'Sets the Company shown in the grid. The domain→company enrichment may re-fill it later for contacts whose website domain matches a company in the database.';
    if (noteEl) { noteEl.textContent = msg; noteEl.style.display = msg ? 'block' : 'none'; }
  };

  const cancelBtn = document.createElement('button');
  cancelBtn.type = 'button'; cancelBtn.className = 'ghost'; cancelBtn.textContent = 'Cancel';
  cancelBtn.addEventListener('click', closeModal);
  const saveBtn = document.createElement('button');
  saveBtn.type = 'button'; saveBtn.className = 'primary'; saveBtn.textContent = `Apply to ${nStr} record(s)`;

  openModal(`Bulk edit ${nStr} record(s)`, (body) => {
    const warn = document.createElement('p');
    warn.className = 'bulk-warn';
    warn.innerHTML = `Sets one field to the same value on <strong>all ${nStr} selected record(s)</strong>. This overwrites any existing value and cannot be undone.`;
    body.appendChild(warn);

    const fLabel = document.createElement('label'); fLabel.className = 'bulk-field';
    fLabel.appendChild(Object.assign(document.createElement('span'), { textContent: 'Field to change' }));
    fieldSel = document.createElement('select');
    BULK_FIELDS.forEach((f) => { const o = document.createElement('option'); o.value = f.key; o.textContent = f.label; fieldSel.appendChild(o); });
    fLabel.appendChild(fieldSel);
    body.appendChild(fLabel);

    const vLabel = document.createElement('label'); vLabel.className = 'bulk-field';
    vLabel.appendChild(Object.assign(document.createElement('span'), { textContent: 'New value' }));
    valueWrap = document.createElement('div'); vLabel.appendChild(valueWrap);
    body.appendChild(vLabel);
    noteEl = document.createElement('p'); noteEl.className = 'bulk-note'; noteEl.style.display = 'none';
    body.appendChild(noteEl);
    rebuildValue();
    fieldSel.addEventListener('change', rebuildValue);
  }, [cancelBtn, saveBtn]);

  saveBtn.addEventListener('click', async () => {
    const field = fieldSel.value;
    const value = valueInput.value;
    if (!window.confirm(`Set "${field}" to "${value || '(blank)'}" on ${nStr} record(s)?\n\nThis cannot be undone.`)) return;
    saveBtn.disabled = true; cancelBtn.disabled = true; saveBtn.textContent = 'Applying…';
    try {
      const res = await fetch('/api/db/bulk-update', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ field, value, emails }),
      });
      const out = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(out.error || `HTTP ${res.status}`);
      closeModal();
      state.selected.clear(); state.allMatching = false;
      updateSelectedInfo();
      query();
      const parts = [`${Number(out.updated || 0).toLocaleString()} updated`];
      if (out.protected) parts.push(`${out.protected.toLocaleString()} kept (verified email)`);
      if (out.skipped) parts.push(`${out.skipped.toLocaleString()} skipped`);
      if (out.companiesUpdated) parts.push(`${out.companiesUpdated.toLocaleString()} company record(s) tagged`);
      if (out.errors) parts.push(`${out.errors.toLocaleString()} error(s)`);
      alert('Bulk edit complete: ' + parts.join(', ') + '.');
    } catch (e) {
      saveBtn.disabled = false; cancelBtn.disabled = false;
      saveBtn.textContent = `Apply to ${nStr} record(s)`;
      alert('Bulk edit failed: ' + e.message);
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
    state.allMatching = false;
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
    state.allMatching = false;
    updateSelectedInfo();
    query();
  } catch (e) {
    alert('Delete failed: ' + e.message);
    updateSelectedInfo();
  }
}

function runSearch() {
  state.allMatching = false;     // a changed filter means a different match set; re-offer select-all
  state.page = 1;
  query();
}

function clearFilters() {
  el.fSearch.value = '';
  el.fDirectory.value = '';
  el.fDomains.value = '';
  el.fPosition.value = '';
  el.fLocation.value = '';
  el.fEmailType.value = '';
  el.fPhoneType.value = '';
  el.fType.value = '';
  el.fGender.value = 'all';
  if (el.fNewHire) el.fNewHire.checked = false;
  if (el.fIndustry) el.fIndustry.value = '';
  if (el.fCompanySize) el.fCompanySize.value = '';
  if (el.fCompanyLocation) el.fCompanyLocation.value = '';
  if (el.fFoundedMin) el.fFoundedMin.value = '';
  if (el.fFoundedMax) el.fFoundedMax.value = '';
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
  el.fLocation.addEventListener('input', debouncedSearch);
  el.fDomains.addEventListener('input', debouncedSearch);
  el.fDirectory.addEventListener('change', runSearch);
  el.fEmailType.addEventListener('change', runSearch);
  el.fPhoneType.addEventListener('change', runSearch);
  el.fType.addEventListener('change', runSearch);
  el.fGender.addEventListener('change', runSearch);
  el.fLinkedin.addEventListener('change', runSearch);
  if (el.fNewHire) el.fNewHire.addEventListener('change', runSearch);
  el.applyBtn.addEventListener('click', runSearch);
  el.clearBtn.addEventListener('click', clearFilters);
  el.downloadBtn.addEventListener('click', download);
  el.editBtn.addEventListener('click', openEditModal);
  el.bulkEditBtn.addEventListener('click', openBulkEditModal);
  el.aiBtn.addEventListener('click', runAiSearch);
  el.deleteBtn.addEventListener('click', deleteSelected);

  el.firstBtn.addEventListener('click', () => { state.page = 1; query(); });
  el.prevBtn.addEventListener('click', () => { state.page = Math.max(1, state.page - 1); query(); });
  el.nextBtn.addEventListener('click', () => { state.page = Math.min(totalPages(), state.page + 1); query(); });
  el.lastBtn.addEventListener('click', () => { state.page = totalPages(); query(); });
}

// Pre-populate filters from the URL query (deep-links, e.g. the Company Crawler's Contact Count ->
// /search?domain=acme.com). Runs before the initial query so the linked-to results load straight away.
function applyUrlParams() {
  const p = new URLSearchParams(location.search);
  const setVal = (key, elm) => { const v = p.get(key); if (v != null && elm) elm.value = v; };
  setVal('search', el.fSearch); setVal('directory', el.fDirectory); setVal('position', el.fPosition);
  setVal('location', el.fLocation); setVal('emailType', el.fEmailType); setVal('phoneType', el.fPhoneType);
  setVal('type', el.fType); setVal('gender', el.fGender);
  setVal('industry', el.fIndustry); setVal('companySize', el.fCompanySize); setVal('companyLocation', el.fCompanyLocation);
  setVal('foundedMin', el.fFoundedMin); setVal('foundedMax', el.fFoundedMax);
  if (p.get('domain') && el.fDomains) el.fDomains.value = p.get('domain');
  if ((p.get('linkedin') === '1' || p.get('linkedin') === 'yes') && el.fLinkedin) el.fLinkedin.checked = true;
}

document.addEventListener('DOMContentLoaded', async () => {
  initElements();
  el.resultsScroll = $('resultsScroll');
  attachEvents();
  createHeader();
  applyUrlParams();  // deep-link filters from the URL (Company Crawler Contact Count links here)
  updateSelectedInfo();
  query();          // load the results table immediately
  loadFacets();     // fill the filter dropdowns in the background (don't block the table)
});
