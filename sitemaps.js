/**
 * sitemaps.js — the Sitemap Library: a queryable OpenSearch index of child sitemaps that are dedicated
 * to People (agents/advisors/providers) or Location (stores/branches/offices/dealers) pages. Populated by
 * discover-sitemaps.js (which classifies via cc-engine.discoverSitemaps) and later consumed by the crawl +
 * new-hire/new-location monitor. Mirrors companies.js: same makeClient (SigV4), an ensureIndex + a bulk
 * upsert keyed by the sitemap URL (dedupe), and a few read helpers.
 *
 *   _id = sitemap_url. Upsert preserves discovered_at (first seen); refreshes last_seen + counts + lastmod.
 */
const os = require('./opensearch');

const INDEX = process.env.SITEMAPS_INDEX || 'sitemaps';

const MAPPING = {
  settings: { number_of_shards: 1, number_of_replicas: 0, 'index.max_result_window': 50000 },
  mappings: {
    properties: {
      sitemap_url:  { type: 'keyword' },
      domain:       { type: 'keyword' },
      parent_url:   { type: 'keyword' },
      kind:         { type: 'keyword' },                                 // People | Location
      type:         { type: 'keyword' },                                 // Parent | Child (stored; seeded from parent_url)
      keyword:      { type: 'keyword' },                                 // matched sitemap filename / lexeme
      url_count:    { type: 'integer' },                                 // total <loc> in the child sitemap
      item_count:   { type: 'integer' },                                 // People/Location urls kept
      ratio:        { type: 'float' },
      by_name:      { type: 'boolean' },                                 // matched the name lexicon (vs ratio)
      industry:     { type: 'keyword' },
      company_id:   { type: 'keyword' },
      lastmod:      { type: 'keyword' },                                 // parent index <lastmod> for this child
      source:       { type: 'keyword' },                                 // discovered | reviewed
      status:       { type: 'keyword' },                                 // active | ...
      discovered_at:{ type: 'date' },
      last_seen:    { type: 'date' },
      monitored:    { type: 'boolean' },                                 // opt-in: re-check + gap-fill this sitemap
      last_checked: { type: 'date' },                                    // last monitor pass over this sitemap
      last_new:     { type: 'integer' },                                 // URLs handed to extraction last pass
      total_new:    { type: 'integer' },                                 // cumulative extracted via monitoring
      monitor_note: { type: 'keyword' },                                 // last monitor status/error
    },
  },
};

const makeClient = os.makeClient;

// Fields added after the index was first created — applied via putMapping on an existing index (additive,
// non-breaking). Keeps a live Library current with new columns (the monitor fields) without a reindex.
const ADDED_FIELDS = {
  type:         { type: 'keyword' },
  monitored:    { type: 'boolean' },
  last_checked: { type: 'date' },
  last_new:     { type: 'integer' },
  total_new:    { type: 'integer' },
  monitor_note: { type: 'keyword' },
};
async function ensureIndex(client) {
  const ex = await client.indices.exists({ index: INDEX });
  if (!(ex.body === true || ex === true)) { await client.indices.create({ index: INDEX, body: MAPPING }); return; }
  try { await client.indices.putMapping({ index: INDEX, body: { properties: ADDED_FIELDS } }); } catch (e) { /* additive-only; ignore if already present */ }
}

// Classify a sitemap's Type from its URL structure (stored + admin-editable):
//   Sub-Domain — on a non-www subdomain of the company domain (agents.acme.com/…)
//   Parent     — the domain's ROOT sitemap (acme.com/sitemap.xml, /sitemap_index.xml, or /)
//   Child      — any other (nested/specific) sitemap on the domain (/agent-sitemap.xml, /sm/agents.xml)
const ROOT_SITEMAP_PATHS = new Set(['', '/', '/sitemap.xml', '/sitemap_index.xml', '/sitemap-index.xml', '/sitemapindex.xml']);
function deriveType(sitemapUrl, domain) {
  let dom = String(domain || '').toLowerCase(); if (dom.startsWith('www.')) dom = dom.slice(4);
  let u; try { u = new URL(sitemapUrl); } catch (e) { return 'Parent'; }
  let host = u.hostname.toLowerCase(); if (host.startsWith('www.')) host = host.slice(4);
  if (dom && host && host !== dom && host.endsWith('.' + dom)) return 'Sub-Domain';
  let path = u.pathname.toLowerCase(); if (path.length > 1 && path.endsWith('/')) path = path.slice(0, -1);
  return ROOT_SITEMAP_PATHS.has(path) ? 'Parent' : 'Child';
}

// Build a Library doc from a cc-engine.discoverSitemaps watch, tagged with the source company's metadata.
function docFromWatch(w, extra = {}) {
  return {
    sitemap_url: w.sitemapUrl,
    domain: w.domain || '',
    parent_url: w.parentUrl || '',
    kind: w.kind || '',
    type: deriveType(w.sitemapUrl, w.domain, w.parentUrl),
    keyword: w.keyword || '',
    url_count: w.urlCount || 0,
    item_count: w.itemCount || 0,
    ratio: Number(w.ratio || 0),
    by_name: !!w.byName,
    industry: extra.industry || '',
    company_id: extra.company_id || '',
    lastmod: w.lastmod || '',
    source: extra.source || 'discovered',
    status: 'active',
  };
}

// Upsert by sitemap_url. First sight writes the full doc (incl. discovered_at); re-sights refresh the
// mutable fields + last_seen and leave discovered_at intact. refresh:false — callers batch.
async function bulkUpsert(client, docs, nowIso) {
  const now = nowIso || new Date().toISOString();
  const body = [];
  for (const d of docs) {
    const id = String(d.sitemap_url || '');
    if (!id || Buffer.byteLength(id) > 512) continue;                    // OpenSearch _id hard cap
    const mutable = {
      domain: d.domain || '', parent_url: d.parent_url || '', kind: d.kind || '', type: d.type || deriveType(d.sitemap_url, d.domain, d.parent_url), keyword: d.keyword || '',
      url_count: d.url_count || 0, item_count: d.item_count || 0, ratio: Number(d.ratio || 0), by_name: !!d.by_name,
      industry: d.industry || '', company_id: d.company_id || '', lastmod: d.lastmod || '',
      status: d.status || 'active', last_seen: now,
    };
    const full = { sitemap_url: id, source: d.source || 'discovered', discovered_at: now, ...mutable };
    body.push({ update: { _index: INDEX, _id: id } });
    body.push({ doc: mutable, upsert: full });
  }
  if (!body.length) return { upserted: 0, errors: 0 };
  const res = await client.bulk({ body, refresh: false });
  const r = res.body || res;
  let errors = 0, upserted = 0;
  if (r.items) for (const it of r.items) { const u = it.update || {}; if (u.error) errors++; else upserted++; }
  return { upserted, errors };
}

// The monitored sitemaps of a kind, LEAST-recently-checked first (never-checked first), for a monitor pass.
async function monitoredBatch(client, size = 2000, kind = 'People') {
  const filter = [{ term: { monitored: true } }];
  if (kind) filter.push({ term: { kind } });
  const r = await client.search({ index: INDEX, body: {
    size, query: { bool: { filter } },
    sort: [{ last_checked: { order: 'asc', missing: '_first' } }, { sitemap_url: 'asc' }],
  } });
  return ((r.body || r).hits.hits || []).map((h) => h._source);
}

// Update a sitemap's monitor state after a pass (partial doc).
async function setMonitorState(client, sitemapUrl, patch) {
  if (!sitemapUrl) return;
  try { await client.update({ index: INDEX, id: sitemapUrl, body: { doc: patch } }); } catch (e) { /* best-effort */ }
}

// Which of these domains already have at least one Library entry (for the driver's resume skip).
async function existingDomains(client, domains) {
  const list = [...new Set((domains || []).map((d) => String(d || '').trim().toLowerCase()).filter(Boolean))];
  const have = new Set();
  for (let i = 0; i < list.length; i += 1024) {
    const chunk = list.slice(i, i + 1024);
    const r = await client.search({ index: INDEX, body: { size: 0, query: { terms: { domain: chunk } }, aggs: { d: { terms: { field: 'domain', size: chunk.length } } } } });
    for (const b of ((r.body || r).aggregations.d.buckets || [])) have.add(b.key);
  }
  return have;
}

async function count(client, query) {
  const body = query ? { query } : {};
  const r = await client.count({ index: INDEX, body });
  return (r.body || r).count;
}

// Total + per-kind counts — the verification snapshot.
async function stats(client) {
  const r = await client.search({ index: INDEX, body: { size: 0, track_total_hits: true, aggs: { kind: { terms: { field: 'kind', size: 10 } }, byname: { terms: { field: 'by_name', size: 2 } } } } });
  const a = (r.body || r).aggregations;
  return {
    total: (r.body || r).hits.total.value,
    byKind: Object.fromEntries((a.kind.buckets || []).map((b) => [b.key, b.doc_count])),
    byName: Object.fromEntries((a.byname.buckets || []).map((b) => [String(b.key), b.doc_count])),
  };
}

const SORT_COLS = new Set(['item_count', 'url_count', 'ratio', 'domain', 'kind', 'industry', 'keyword', 'discovered_at', 'last_seen', 'lastmod']);

// Build the OpenSearch query for the Library UI filters.
function buildFilter(f = {}) {
  const filter = [], must = [];
  if (f.kind) filter.push({ term: { kind: f.kind } });
  // Type (stored, editable): Parent (top-level) / Child (under an index) / Sub-Domain.
  if (['Parent', 'Child', 'Sub-Domain'].includes(f.type)) filter.push({ term: { type: f.type } });
  if (f.monitored === 'yes' || f.monitored === true) filter.push({ term: { monitored: true } });
  if (f.industry) {
    const arr = (Array.isArray(f.industry) ? f.industry : String(f.industry).split(',')).map((s) => s.trim()).filter(Boolean);
    if (arr.length === 1) filter.push({ term: { industry: arr[0] } });
    else if (arr.length) filter.push({ terms: { industry: arr } });
  }
  if (f.domain) filter.push({ wildcard: { domain: `*${String(f.domain).toLowerCase()}*` } });
  if (f.keyword) filter.push({ wildcard: { keyword: `*${String(f.keyword).toLowerCase()}*` } });
  if (f.byName === 'yes' || f.byName === true) filter.push({ term: { by_name: true } });
  if (f.byName === 'no' || f.byName === false) filter.push({ term: { by_name: false } });
  const mc = Number(f.minCount); if (mc > 0) filter.push({ range: { item_count: { gte: mc } } });
  if (f.q) must.push({ query_string: { query: String(f.q), fields: ['sitemap_url', 'domain', 'keyword', 'industry'], default_operator: 'AND' } });
  const bool = {};
  if (filter.length) bool.filter = filter;
  if (must.length) bool.must = must;
  return Object.keys(bool).length ? { bool } : { match_all: {} };
}

async function search(client, f = {}, { from = 0, size = 50, sort = 'item_count', dir = 'desc' } = {}) {
  const col = SORT_COLS.has(sort) ? sort : 'item_count';
  const body = { from, size, track_total_hits: true, query: buildFilter(f), sort: [{ [col]: dir === 'asc' ? 'asc' : 'desc' }] };
  const r = await client.search({ index: INDEX, body });
  const b = r.body || r;
  return { total: b.hits.total.value, rows: (b.hits.hits || []).map((h) => h._source) };
}

// Facet counts + the harvestable-page total for the current filter set (drives the sidebar + header).
async function facets(client, f = {}) {
  const r = await client.search({ index: INDEX, body: { size: 0, track_total_hits: true, query: buildFilter(f), aggs: {
    kind: { terms: { field: 'kind', size: 5 } },
    industry: { terms: { field: 'industry', size: 40 } },
    type: { terms: { field: 'type', size: 3 } },
    items: { sum: { field: 'item_count' } },
  } } });
  const b = r.body || r; const a = b.aggregations;
  return {
    total: b.hits.total.value,
    harvestable: Math.round(a.items.value || 0),
    kind: (a.kind.buckets || []).map((x) => ({ key: x.key, count: x.doc_count })),
    industry: (a.industry.buckets || []).map((x) => ({ key: x.key, count: x.doc_count })),
    type: ((a.type && a.type.buckets) || []).map((x) => ({ key: x.key, count: x.doc_count })),
  };
}

// Fields an admin may mass-edit in the Library UI.
const EDITABLE = new Set(['kind', 'type', 'industry', 'keyword', 'status', 'monitored']);

// Partial bulk update by _id (sitemap_url). Validates fields against EDITABLE + kind values. Returns
// { updated, errors, total }.
async function bulkUpdate(client, ids, updates) {
  const doc = {};
  for (const k in (updates || {})) { if (EDITABLE.has(k)) doc[k] = String(updates[k] == null ? '' : updates[k]); }
  if (doc.kind && !['People', 'Location'].includes(doc.kind)) delete doc.kind;
  if (doc.type && !['Parent', 'Child', 'Sub-Domain'].includes(doc.type)) delete doc.type;
  if ('monitored' in doc) doc.monitored = /^(1|true|on|yes)$/i.test(doc.monitored);   // boolean field
  const list = [...new Set((ids || []).map((x) => String(x || '').trim()).filter(Boolean))];
  if (!Object.keys(doc).length || !list.length) return { updated: 0, errors: 0, total: list.length };
  let updated = 0, errors = 0;
  for (let i = 0; i < list.length; i += 500) {
    const body = [];
    for (const id of list.slice(i, i + 500)) body.push({ update: { _index: INDEX, _id: id } }, { doc });
    const r = await client.bulk({ body, refresh: false });
    for (const it of (((r.body || r).items) || [])) { const u = it.update || {}; if (u.error || (u.status && u.status >= 400)) errors++; else updated++; }
  }
  try { await client.indices.refresh({ index: INDEX }); } catch (e) { /* best-effort */ }
  return { updated, errors, total: list.length };
}

// Permanently remove Library entries by _id (sitemap_url). Returns { deleted, errors }.
async function bulkDelete(client, ids) {
  const list = [...new Set((ids || []).map((x) => String(x || '').trim()).filter(Boolean))];
  if (!list.length) return { deleted: 0, errors: 0 };
  let deleted = 0, errors = 0;
  for (let i = 0; i < list.length; i += 500) {
    const body = [];
    for (const id of list.slice(i, i + 500)) body.push({ delete: { _index: INDEX, _id: id } });
    const r = await client.bulk({ body, refresh: false });
    for (const it of (((r.body || r).items) || [])) { const d = it.delete || {}; if (d.error) errors++; else if (d.result === 'deleted' || d.status === 200) deleted++; }
  }
  try { await client.indices.refresh({ index: INDEX }); } catch (e) { /* best-effort */ }
  return { deleted, errors };
}

// ---- CSV export (respects the same filters as the UI) ----
const EXPORT_COLS = [
  ['sitemap_url', 'Sitemap URL'], ['domain', 'Domain'], ['kind', 'Kind'], ['type', 'Type'],
  ['keyword', 'Keyword'], ['item_count', 'Pages'], ['url_count', 'Total URLs'], ['ratio', 'Ratio'],
  ['by_name', 'By Name'], ['industry', 'Industry'], ['company_id', 'Company Id'], ['lastmod', 'Last Modified'],
  ['source', 'Source'], ['discovered_at', 'Discovered At'], ['last_seen', 'Last Seen'],
];
const csvCell = (v) => { const s = String(v == null ? '' : v); return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s; };
const csvHeader = () => EXPORT_COLS.map((c) => c[1]).join(',');
const rowToCsvLine = (d) => EXPORT_COLS.map(([k]) => csvCell(d[k])).join(',');

// Stream every matching Library doc to onRow via search_after (for CSV export beyond the 50k window).
async function each(client, f, onRow, cap = 200000) {
  let after = null, n = 0;
  for (;;) {
    const body = { size: 2000, query: buildFilter(f), sort: [{ item_count: 'desc' }, { sitemap_url: 'asc' }] };
    if (after) body.search_after = after;
    const r = await client.search({ index: INDEX, body });
    const hits = (r.body || r).hits.hits;
    if (!hits.length) break;
    for (const h of hits) { await onRow(h._source); if (++n >= cap) return n; }
    after = hits[hits.length - 1].sort;
  }
  return n;
}

module.exports = { INDEX, MAPPING, makeClient, ensureIndex, deriveType, docFromWatch, bulkUpsert, existingDomains, count, stats, search, facets, EDITABLE, bulkUpdate, bulkDelete, csvHeader, rowToCsvLine, each, monitoredBatch, setMonitorState };
