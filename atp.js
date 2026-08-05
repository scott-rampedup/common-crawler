/**
 * atp.js — the "All The Places Library": an OpenSearch index of the alltheplaces/OpenAddresses brand
 * catalog (one row per brand/spider) loaded from "All the places Appended with Websites.csv". Mirrors
 * sitemaps.js: SigV4 makeClient, ensureIndex, a bulk upsert keyed by the spider id, search/facets, admin
 * edit (Type), CSV export. The "All the places Most Recent Link" column points to that brand's GeoJSON of
 * locations, which the Corporate Places ingest fans out over.
 *
 *   _id = spider (e.g. "agnvet_au"); fallback to a website/name slug when spider is blank.
 */
const os = require('./opensearch');

const INDEX = process.env.ATP_LIBRARY_INDEX || 'atp_library';

const MAPPING = {
  settings: { number_of_shards: 1, number_of_replicas: 0, 'index.max_result_window': 50000 },
  mappings: {
    properties: {
      spider:     { type: 'keyword' },                                   // alltheplaces spider id (=_id)
      name:       { type: 'text', fields: { kw: { type: 'keyword' } } }, // BRAND
      website:    { type: 'keyword' },
      country:    { type: 'keyword' },
      source:     { type: 'keyword' },                                   // spiders.html source page
      count:      { type: 'integer' },                                   // # places in the geojson
      type:       { type: 'keyword' },                                   // category/Type (admin-editable)
      link:       { type: 'keyword' },                                   // "Most Recent Link" -> geojson
      date:       { type: 'keyword' },                                   // run date if present
      loaded:     { type: 'integer' },                                   // places ingested into corporate_places
      last_ingest:{ type: 'date' },
      source_file:{ type: 'keyword' },
      loaded_at:  { type: 'date' },
      last_seen:  { type: 'date' },
    },
  },
};

const makeClient = os.makeClient;

const ADDED_FIELDS = { loaded: { type: 'integer' }, last_ingest: { type: 'date' } };
async function ensureIndex(client) {
  const ex = await client.indices.exists({ index: INDEX });
  if (!(ex.body === true || ex === true)) { await client.indices.create({ index: INDEX, body: MAPPING }); return; }
  try { await client.indices.putMapping({ index: INDEX, body: { properties: ADDED_FIELDS } }); } catch (e) { /* additive */ }
}

const slug = (s) => String(s || '').toLowerCase().replace(/^https?:\/\//, '').replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '');
function idFor(row) { return String(row.spider || '').trim() || slug(row.website || row.name || ''); }

// Build a Library doc from a parsed CSV row {name,website,country,spider,source,count,type,link,date,source_file}.
function docFromRow(row, extra = {}) {
  return {
    spider: String(row.spider || '').trim(),
    name: String(row.name || '').trim(),
    website: String(row.website || '').trim().toLowerCase(),
    country: String(row.country || '').trim().toLowerCase(),
    source: String(row.source || '').trim(),
    count: Number(String(row.count || '').replace(/[^0-9]/g, '')) || 0,
    type: String(row.type || '').trim(),
    link: String(row.link || '').trim(),
    date: String(row.date || '').trim(),
    source_file: extra.source_file || row.source_file || '',
  };
}

// Upsert by spider id. First sight writes the full doc (+loaded_at); re-sights refresh mutable fields.
async function bulkUpsert(client, docs, nowIso) {
  const now = nowIso || new Date().toISOString();
  const body = [];
  for (const d of docs) {
    const id = idFor(d);
    if (!id || Buffer.byteLength(id) > 512) continue;
    const mutable = { spider: d.spider || '', name: d.name || '', website: d.website || '', country: d.country || '',
      source: d.source || '', count: d.count || 0, type: d.type || '', link: d.link || '', date: d.date || '',
      source_file: d.source_file || '', last_seen: now };
    const full = { ...mutable, loaded_at: now };
    body.push({ update: { _index: INDEX, _id: id } }, { doc: mutable, upsert: full });
  }
  if (!body.length) return { upserted: 0, errors: 0 };
  const res = await client.bulk({ body, refresh: false });
  const r = res.body || res; let errors = 0, upserted = 0;
  if (r.items) for (const it of r.items) { const u = it.update || {}; if (u.error) errors++; else upserted++; }
  return { upserted, errors };
}

// Record how many places a brand ingested (called by the geojson ingest).
async function setIngestState(client, id, patch) {
  if (!id) return;
  try { await client.update({ index: INDEX, id, body: { doc: patch } }); } catch (e) { /* best-effort */ }
}

const SORT_COLS = new Set(['name.kw', 'count', 'country', 'type', 'spider', 'website', 'loaded', 'loaded_at']);

function buildFilter(f = {}) {
  const filter = [], must = [];
  if (f.country) filter.push({ term: { country: String(f.country).toLowerCase() } });
  if (f.type) {
    const arr = (Array.isArray(f.type) ? f.type : String(f.type).split(',')).map((s) => s.trim()).filter(Boolean);
    if (arr.length === 1) filter.push({ term: { type: arr[0] } });
    else if (arr.length) filter.push({ terms: { type: arr } });
  }
  if (f.website) filter.push({ wildcard: { website: `*${String(f.website).toLowerCase()}*` } });
  if (f.hasLink === 'yes') filter.push({ wildcard: { link: 'http*' } });
  if (f.hasLink === 'no') must.push({ bool: { must_not: [{ wildcard: { link: 'http*' } }] } });
  const mc = Number(f.minCount); if (mc > 0) filter.push({ range: { count: { gte: mc } } });
  if (f.q) must.push({ query_string: { query: String(f.q), fields: ['name', 'website', 'spider', 'type'], default_operator: 'AND' } });
  const bool = {};
  if (filter.length) bool.filter = filter;
  if (must.length) bool.must = must;
  return Object.keys(bool).length ? { bool } : { match_all: {} };
}

async function search(client, f = {}, { from = 0, size = 50, sort = 'count', dir = 'desc' } = {}) {
  const col = SORT_COLS.has(sort) ? sort : 'count';
  const body = { from, size, track_total_hits: true, query: buildFilter(f), sort: [{ [col]: dir === 'asc' ? 'asc' : 'desc' }] };
  const r = await client.search({ index: INDEX, body });
  const b = r.body || r;
  return { total: b.hits.total.value, rows: (b.hits.hits || []).map((h) => h._source) };
}

async function facets(client, f = {}) {
  const r = await client.search({ index: INDEX, body: { size: 0, track_total_hits: true, query: buildFilter(f), aggs: {
    country: { terms: { field: 'country', size: 60 } },
    type: { terms: { field: 'type', size: 60 } },
    places: { sum: { field: 'count' } },
  } } });
  const b = r.body || r; const a = b.aggregations;
  return {
    total: b.hits.total.value,
    places: Math.round((a.places && a.places.value) || 0),
    country: (a.country.buckets || []).map((x) => ({ key: x.key, count: x.doc_count })),
    type: (a.type.buckets || []).map((x) => ({ key: x.key, count: x.doc_count })),
  };
}

async function stats(client) {
  const r = await client.search({ index: INDEX, body: { size: 0, track_total_hits: true, aggs: { places: { sum: { field: 'count' } }, loaded: { sum: { field: 'loaded' } } } } });
  const b = r.body || r; const a = b.aggregations;
  return { total: b.hits.total.value, places: Math.round((a.places.value) || 0), loaded: Math.round((a.loaded.value) || 0) };
}

const EDITABLE = new Set(['type', 'name', 'website', 'country', 'link']);
function sanitizeUpdates(updates) {
  const doc = {};
  for (const k in (updates || {})) { if (EDITABLE.has(k)) doc[k] = String(updates[k] == null ? '' : updates[k]); }
  if (doc.country) doc.country = doc.country.toLowerCase();
  if (doc.website) doc.website = doc.website.toLowerCase();
  return doc;
}
async function bulkUpdate(client, ids, updates) {
  const doc = sanitizeUpdates(updates);
  const list = [...new Set((ids || []).map((x) => String(x || '').trim()).filter(Boolean))];
  if (!Object.keys(doc).length || !list.length) return { updated: 0, errors: 0, total: list.length };
  let updated = 0, errors = 0;
  for (let i = 0; i < list.length; i += 500) {
    const body = [];
    for (const id of list.slice(i, i + 500)) body.push({ update: { _index: INDEX, _id: id } }, { doc });
    const r = await client.bulk({ body, refresh: false });
    for (const it of (((r.body || r).items) || [])) { const u = it.update || {}; if (u.error || (u.status && u.status >= 400)) errors++; else updated++; }
  }
  try { await client.indices.refresh({ index: INDEX }); } catch (e) { /* */ }
  return { updated, errors, total: list.length };
}
async function updateOne(client, id, updates) {
  const _id = String(id || '').trim(); const doc = sanitizeUpdates(updates);
  if (!_id || !Object.keys(doc).length) return { updated: 0, errors: 0 };
  try { await client.update({ index: INDEX, id: _id, body: { doc } }); try { await client.indices.refresh({ index: INDEX }); } catch (e) {} return { updated: 1, errors: 0 }; }
  catch (e) { return { updated: 0, errors: 1, error: e.message }; }
}
async function bulkDelete(client, ids) {
  const list = [...new Set((ids || []).map((x) => String(x || '').trim()).filter(Boolean))];
  if (!list.length) return { deleted: 0, errors: 0 };
  let deleted = 0, errors = 0;
  for (let i = 0; i < list.length; i += 500) {
    const body = [];
    for (const id of list.slice(i, i + 500)) body.push({ delete: { _index: INDEX, _id: id } });
    const r = await client.bulk({ body, refresh: false });
    for (const it of (((r.body || r).items) || [])) { const d = it.delete || {}; if (d.error) errors++; else deleted++; }
  }
  try { await client.indices.refresh({ index: INDEX }); } catch (e) { /* */ }
  return { deleted, errors };
}

// ---- CSV export (respects the UI filters) ----
const EXPORT_COLS = [['name', 'Name'], ['website', 'Website'], ['country', 'Country'], ['spider', 'Spider'],
  ['source', 'Source'], ['count', 'Count'], ['type', 'Type'], ['link', 'All the places Most Recent Link'], ['loaded', 'Loaded']];
const csvCell = (v) => { const s = String(v == null ? '' : v); return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s; };
const csvHeader = () => EXPORT_COLS.map((c) => c[1]).join(',');
const rowToCsvLine = (d) => EXPORT_COLS.map(([k]) => csvCell(d[k])).join(',');
async function each(client, f, onRow, cap = 200000) {
  let after = null, n = 0;
  for (;;) {
    const body = { size: 1000, query: buildFilter(f), sort: [{ count: 'desc' }, { spider: 'asc' }] };
    if (after) body.search_after = after;
    const r = await client.search({ index: INDEX, body });
    const hits = (r.body || r).hits.hits || [];
    if (!hits.length) break;
    for (const h of hits) { await onRow(h._source); if (++n >= cap) return n; }
    after = hits[hits.length - 1].sort;
    if (hits.length < 1000) break;
  }
  return n;
}

module.exports = { INDEX, MAPPING, makeClient, ensureIndex, idFor, docFromRow, bulkUpsert, setIngestState,
  search, facets, stats, EDITABLE, bulkUpdate, updateOne, bulkDelete, csvHeader, rowToCsvLine, each };
