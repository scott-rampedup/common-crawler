/**
 * corporate-places.js — the "Corporate Places" index: individual store/branch/office locations parsed from
 * each brand's GeoJSON ("All the places Most Recent Link" in the ATP Library) and enriched with the BRAND
 * (spreadsheet Name) + TYPE (spreadsheet Type). One doc per place. Mirrors the other crawler stores:
 * SigV4 makeClient, ensureIndex, bulk index keyed by spider|ref, search + left-rail facets, CSV export.
 *
 *   _id = "<spider>|<ref>" (ref is the place's stable id/URL); falls back to coords/name when ref is blank.
 */
const os = require('./opensearch');

const INDEX = process.env.CORPORATE_PLACES_INDEX || 'corporate_places';

const MAPPING = {
  settings: { number_of_shards: 2, number_of_replicas: 0, 'index.mapping.total_fields.limit': 200, 'index.max_result_window': 50000 },
  mappings: {
    properties: {
      brand:        { type: 'text', fields: { kw: { type: 'keyword' } } },   // from spreadsheet Name (BRAND)
      type:         { type: 'keyword' },                                      // from spreadsheet Type (TYPE)
      spider:       { type: 'keyword' },
      brand_website:{ type: 'keyword' },
      name:         { type: 'text', fields: { kw: { type: 'keyword' } } },
      branch:       { type: 'text', fields: { kw: { type: 'keyword' } } },
      ref:          { type: 'keyword' },
      category:     { type: 'keyword' },                                      // OSM primary tag value (shop/amenity/…)
      addr_full:    { type: 'text' },
      housenumber:  { type: 'keyword' },
      street:       { type: 'text', fields: { kw: { type: 'keyword' } } },
      city:         { type: 'keyword' },
      state:        { type: 'keyword' },
      postcode:     { type: 'keyword' },
      country:      { type: 'keyword' },
      phone:        { type: 'keyword' },
      website:      { type: 'keyword' },
      email:        { type: 'keyword' },
      opening_hours:{ type: 'text' },
      source_uri:   { type: 'keyword' },
      location:     { type: 'geo_point' },
      lat:          { type: 'float' },
      lon:          { type: 'float' },
      loaded_at:    { type: 'date' },
    },
  },
};

const makeClient = os.makeClient;

async function ensureIndex(client) {
  const ex = await client.indices.exists({ index: INDEX });
  if (!(ex.body === true || ex === true)) await client.indices.create({ index: INDEX, body: MAPPING });
}

const CAT_KEYS = ['shop', 'amenity', 'office', 'leisure', 'tourism', 'craft', 'healthcare', 'man_made', 'industrial', 'club', 'cuisine'];
function firstTag(p) { for (const k of CAT_KEYS) { const v = p[k]; if (v && typeof v === 'string') return v; } return ''; }
const slug = (s) => String(s || '').toLowerCase().replace(/^https?:\/\//, '').replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '');

// meta = { brand, type, spider, brand_website, country } from the spreadsheet row.
function docFromFeature(feat, meta = {}) {
  const p = (feat && feat.properties) || {};
  const g = (feat && feat.geometry) || {};
  const coords = Array.isArray(g.coordinates) ? g.coordinates : [];
  const lon = Number(coords[0]), lat = Number(coords[1]);
  const ref = String(p.ref || p['@source_uri'] || '').trim();
  const country = String(p['addr:country'] || meta.country || '').trim().toUpperCase();
  const doc = {
    brand: meta.brand || p.brand || '', type: meta.type || '', spider: meta.spider || p['@spider'] || '',
    brand_website: meta.brand_website || '',
    name: p.name || meta.brand || '', branch: p.branch || '', ref, category: firstTag(p),
    addr_full: p['addr:full'] || p['addr:street_address'] || '', housenumber: p['addr:housenumber'] || '',
    street: p['addr:street'] || '', city: p['addr:city'] || '', state: p['addr:state'] || '',
    postcode: p['addr:postcode'] || '', country,
    phone: p.phone || '', website: p.website || '', email: p.email || '',
    opening_hours: p['opening_hours'] || '', source_uri: p['@source_uri'] || '',
    lat: Number.isFinite(lat) ? lat : null, lon: Number.isFinite(lon) ? lon : null,
  };
  if (Number.isFinite(lat) && Number.isFinite(lon)) doc.location = { lat, lon };
  return doc;
}
function idFor(doc) {
  const base = doc.spider || slug(doc.brand);
  const tail = doc.ref || (doc.lat != null && doc.lon != null ? `${doc.lat},${doc.lon}` : (doc.name || Math.random().toString(36).slice(2)));
  let id = `${base}|${tail}`;
  if (Buffer.byteLength(id) > 512) id = `${base}|${slug(tail).slice(0, 200)}`;
  return id;
}

// Bulk index (overwrite by _id → dedupe). docs already built by docFromFeature.
async function bulkIndex(client, docs, nowIso) {
  const now = nowIso || new Date().toISOString();
  const body = [];
  for (const d of docs) {
    const id = idFor(d);
    if (!id || Buffer.byteLength(id) > 512) continue;
    body.push({ index: { _index: INDEX, _id: id } }, { ...d, loaded_at: now });
  }
  if (!body.length) return { indexed: 0, errors: 0 };
  const res = await client.bulk({ body, refresh: false });
  const r = res.body || res; let errors = 0, indexed = 0;
  if (r.items) for (const it of r.items) { const u = it.index || {}; if (u.error) errors++; else indexed++; }
  return { indexed, errors };
}

const SORT_COLS = new Set(['brand.kw', 'name.kw', 'city', 'state', 'country', 'category', 'type', 'loaded_at']);
const listOf = (v) => (Array.isArray(v) ? v : String(v).split(',')).map((s) => s.trim()).filter(Boolean);

function buildFilter(f = {}) {
  const filter = [], must = [];
  if (f.brand) { const a = listOf(f.brand); a.length === 1 ? filter.push({ term: { 'brand.kw': a[0] } }) : filter.push({ terms: { 'brand.kw': a } }); }
  if (f.type) { const a = listOf(f.type); a.length === 1 ? filter.push({ term: { type: a[0] } }) : filter.push({ terms: { type: a } }); }
  if (f.category) { const a = listOf(f.category); a.length === 1 ? filter.push({ term: { category: a[0] } }) : filter.push({ terms: { category: a } }); }
  if (f.country) filter.push({ term: { country: String(f.country).toUpperCase() } });
  if (f.state) { const a = listOf(f.state); a.length === 1 ? filter.push({ term: { state: a[0] } }) : filter.push({ terms: { state: a } }); }
  if (f.city) filter.push({ term: { city: String(f.city) } });
  if (f.hasPhone === 'yes') filter.push({ exists: { field: 'phone' } });
  if (f.hasWebsite === 'yes') filter.push({ exists: { field: 'website' } });
  if (f.q) must.push({ query_string: { query: String(f.q), fields: ['name^2', 'brand^2', 'branch', 'addr_full', 'city', 'street'], default_operator: 'AND' } });
  const bool = {};
  if (filter.length) bool.filter = filter;
  if (must.length) bool.must = must;
  return Object.keys(bool).length ? { bool } : { match_all: {} };
}

async function search(client, f = {}, { from = 0, size = 50, sort = 'brand.kw', dir = 'asc' } = {}) {
  const col = SORT_COLS.has(sort) ? sort : 'brand.kw';
  const body = { from, size, track_total_hits: true, query: buildFilter(f), sort: [{ [col]: dir === 'desc' ? 'desc' : 'asc' }] };
  const r = await client.search({ index: INDEX, body });
  const b = r.body || r;
  return { total: b.hits.total.value, rows: (b.hits.hits || []).map((h) => h._source) };
}

async function facets(client, f = {}) {
  const r = await client.search({ index: INDEX, body: { size: 0, track_total_hits: true, query: buildFilter(f), aggs: {
    brand: { terms: { field: 'brand.kw', size: 40 } },
    type: { terms: { field: 'type', size: 40 } },
    category: { terms: { field: 'category', size: 40 } },
    country: { terms: { field: 'country', size: 60 } },
    state: { terms: { field: 'state', size: 60 } },
  } } });
  const b = r.body || r; const a = b.aggregations;
  const buckets = (x) => (x && x.buckets || []).map((z) => ({ key: z.key, count: z.doc_count }));
  return { total: b.hits.total.value, brand: buckets(a.brand), type: buckets(a.type), category: buckets(a.category), country: buckets(a.country), state: buckets(a.state) };
}

async function stats(client) {
  const r = await client.search({ index: INDEX, body: { size: 0, track_total_hits: true, aggs: { brands: { cardinality: { field: 'brand.kw' } } } } });
  const b = r.body || r;
  return { total: b.hits.total.value, brands: (b.aggregations.brands && b.aggregations.brands.value) || 0 };
}

// ---- CSV export (respects the UI filters) ----
const EXPORT_COLS = [['brand', 'Brand'], ['type', 'Type'], ['name', 'Name'], ['branch', 'Branch'], ['category', 'Category'],
  ['addr_full', 'Address'], ['city', 'City'], ['state', 'State'], ['postcode', 'Postcode'], ['country', 'Country'],
  ['phone', 'Phone'], ['website', 'Website'], ['email', 'Email'], ['opening_hours', 'Hours'], ['lat', 'Lat'], ['lon', 'Lon'], ['source_uri', 'Source URL']];
const csvCell = (v) => { const s = String(v == null ? '' : v); return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s; };
const csvHeader = () => EXPORT_COLS.map((c) => c[1]).join(',');
const rowToCsvLine = (d) => EXPORT_COLS.map(([k]) => csvCell(d[k])).join(',');
async function each(client, f, onRow, cap = 500000) {
  let after = null, n = 0;
  for (;;) {
    const body = { size: 1000, query: buildFilter(f), sort: [{ 'brand.kw': 'asc' }, { ref: 'asc' }] };
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

module.exports = { INDEX, MAPPING, makeClient, ensureIndex, docFromFeature, idFor, bulkIndex, search, facets, stats, csvHeader, rowToCsvLine, each };
