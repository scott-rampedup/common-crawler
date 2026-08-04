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
    },
  },
};

const makeClient = os.makeClient;

async function ensureIndex(client) {
  const ex = await client.indices.exists({ index: INDEX });
  if (!(ex.body === true || ex === true)) await client.indices.create({ index: INDEX, body: MAPPING });
}

// Build a Library doc from a cc-engine.discoverSitemaps watch, tagged with the source company's metadata.
function docFromWatch(w, extra = {}) {
  return {
    sitemap_url: w.sitemapUrl,
    domain: w.domain || '',
    parent_url: w.parentUrl || '',
    kind: w.kind || '',
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
      domain: d.domain || '', parent_url: d.parent_url || '', kind: d.kind || '', keyword: d.keyword || '',
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
  const r = await client.search({ index: INDEX, body: { size: 0, aggs: { kind: { terms: { field: 'kind', size: 10 } }, byname: { terms: { field: 'by_name', size: 2 } } } } });
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
  if (f.industry) filter.push({ term: { industry: f.industry } });
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
  const body = { from, size, query: buildFilter(f), sort: [{ [col]: dir === 'asc' ? 'asc' : 'desc' }] };
  const r = await client.search({ index: INDEX, body });
  const b = r.body || r;
  return { total: b.hits.total.value, rows: (b.hits.hits || []).map((h) => h._source) };
}

// Facet counts + the harvestable-page total for the current filter set (drives the sidebar + header).
async function facets(client, f = {}) {
  const r = await client.search({ index: INDEX, body: { size: 0, query: buildFilter(f), aggs: {
    kind: { terms: { field: 'kind', size: 5 } },
    industry: { terms: { field: 'industry', size: 40 } },
    items: { sum: { field: 'item_count' } },
  } } });
  const b = r.body || r; const a = b.aggregations;
  return {
    total: b.hits.total.value,
    harvestable: Math.round(a.items.value || 0),
    kind: (a.kind.buckets || []).map((x) => ({ key: x.key, count: x.doc_count })),
    industry: (a.industry.buckets || []).map((x) => ({ key: x.key, count: x.doc_count })),
  };
}

module.exports = { INDEX, MAPPING, makeClient, ensureIndex, docFromWatch, bulkUpsert, existingDomains, count, stats, search, facets };
