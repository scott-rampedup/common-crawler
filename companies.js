/**
 * companies.js — the `companies` OpenSearch index (People Data Labs free company dataset) + search.
 * -------------------------------------------------------------------------------------------------
 * A reference dataset of ~35.6M companies keyed by id, searchable by name / website / industry / size /
 * country / region / locality / founded. Powers the Company Crawler tab. Reuses the shared OpenSearch
 * client + endpoint (opensearch.js). Loaded once by load-companies.js from the gzip NDJSON (0 replicas —
 * it's static, re-loadable reference data, so we skip the replica to save disk).
 */
const os = require('./opensearch');

const INDEX = process.env.COMPANIES_INDEX || 'companies';
const OUT_COLS = ['name', 'website', 'domain', 'contact_count', 'sitemap_url', 'industry', 'size', 'founded',
  'locality', 'region', 'country', 'linkedin_url', 'phone', 'full_address', 'category', 'cid',
  'description', 'email', 'email_type', 'facebook', 'instagram', 'map', 'linkedin_contact', 'bio_url',
  'alternate_websites', 'contacts', 'contacts_count'];
// CC home-page enrichment fields (populated by cc-home-enrich via /api/companies/cc-enrich).
const CC_FIELDS = ['description', 'email', 'email_type', 'facebook', 'instagram', 'map', 'linkedin_contact', 'bio_url', 'alternate_websites', 'contacts', 'contacts_count'];

const MAPPING = {
  settings: { number_of_shards: 2, number_of_replicas: 0, 'index.max_result_window': 50000 },
  mappings: {
    properties: {
      id:           { type: 'keyword' },
      name:         { type: 'text', fields: { kw: { type: 'keyword' } } },
      website:      { type: 'keyword' },
      domain:       { type: 'keyword' },
      industry:     { type: 'keyword' },
      size:         { type: 'keyword' },
      founded:      { type: 'integer' },
      locality:     { type: 'text', fields: { kw: { type: 'keyword' } } },
      region:       { type: 'keyword' },
      country:      { type: 'keyword' },
      linkedin_url: { type: 'keyword' },
      contact_count: { type: 'integer' },   // # contacts sharing this root domain (set by count-sitemap batch)
      sitemap_url:   { type: 'keyword' },    // discovered bio sitemap (else render constructs {domain}/sitemap.xml)
    },
  },
};

function normDomain(w) {
  if (!w) return '';
  return String(w).toLowerCase().trim().replace(/^https?:\/\//, '').replace(/^www\./, '').split('/')[0].split('?')[0];
}

// One raw PDL record -> an index doc.
function recordToDoc(r) {
  return {
    id: r.id || '', name: r.name || '', website: r.website || '', domain: normDomain(r.website),
    industry: r.industry || '', size: r.size || '',
    founded: (r.founded != null && r.founded !== '') ? (Number(r.founded) || null) : null,
    locality: r.locality || '', region: r.region || '', country: r.country || '', linkedin_url: r.linkedin_url || '',
  };
}

async function ensureIndex(client) {
  const ex = await client.indices.exists({ index: INDEX });
  if (!(ex.body === true || ex === true)) await client.indices.create({ index: INDEX, body: MAPPING });
}

// Bulk index by id (plain index — reference data, last write wins; no score gating).
async function bulkIndex(client, docs) {
  if (!docs.length) return { errors: 0 };
  const body = [];
  for (const d of docs) { if (!d.id) continue; body.push({ index: { _index: INDEX, _id: d.id } }, d); }
  if (!body.length) return { errors: 0 };
  const res = await client.bulk({ body });
  const r = res.body || res;
  let errors = 0;
  if (r.errors) for (const it of r.items) if (it.index && it.index.error) errors++;
  return { errors };
}

// Build an OpenSearch query from the Company Crawler filters. Text fields (name, locality) use match;
// exact facets (domain, industry, size, region, country) use term (lowercased); founded is a range.
function buildQuery(f) {
  f = f || {};
  const must = [], filter = [];
  const kw = (v) => String(v).trim().toLowerCase();
  if (f.name) must.push({ match: { name: { query: String(f.name), operator: 'and' } } });
  if (f.locality) must.push({ match: { locality: { query: String(f.locality), operator: 'and' } } });
  // multi-value: a comma-separated string (or array) -> terms (OR) so several industries/sizes/countries match.
  const multi = (v) => (Array.isArray(v) ? v : String(v).split(',')).map((x) => String(x).trim()).filter(Boolean);
  if (f.domain) filter.push({ term: { domain: normDomain(f.domain) } });
  if (f.industry) filter.push({ terms: { industry: multi(f.industry).map((x) => x.toLowerCase()) } });
  if (f.size) filter.push({ terms: { size: multi(f.size) } });
  if (f.region) filter.push({ term: { region: kw(f.region) } });
  if (f.country) filter.push({ terms: { country: multi(f.country).map((x) => x.toLowerCase()) } });
  if (f.linkedin === 'yes') filter.push({ exists: { field: 'linkedin_url' } });
  const fr = {};
  if (f.founded_min) fr.gte = Number(f.founded_min);
  if (f.founded_max) fr.lte = Number(f.founded_max);
  if (fr.gte != null || fr.lte != null) filter.push({ range: { founded: fr } });
  if (f.contactMin) filter.push({ range: { contact_count: { gte: Number(f.contactMin) } } });
  // sitemap search: users type a sitemap URL or a domain — match on the (shared) root domain either way
  if (f.sitemap) { const d = normDomain(f.sitemap); if (d) filter.push({ term: { domain: d } }); }
  if (f.companyType) filter.push({ term: { 'company_type.keyword': String(f.companyType) } });   // HQ | Location (Google Maps)
  if (Array.isArray(f.ids) && f.ids.length) filter.push({ ids: { values: f.ids.slice(0, 10000) } });
  if (!must.length && !filter.length) return { match_all: {} };
  return { bool: { must: must.length ? must : undefined, filter: filter.length ? filter : undefined } };
}

const SORT_COLS = { name: 'name.kw', founded: 'founded', contact_count: 'contact_count', size: 'size', industry: 'industry', country: 'country', domain: 'domain' };
function sortFor(sort, dir) {
  const order = dir === 'desc' ? 'desc' : 'asc';
  if (SORT_COLS[sort]) return [{ [SORT_COLS[sort]]: { order, missing: '_last' } }];
  return [{ _score: 'desc' }, { 'name.kw': 'asc' }];
}
async function search(client, f, { from = 0, size = 50, sort = '', dir = 'asc' } = {}) {
  const res = await client.search({
    index: INDEX,
    body: {
      track_total_hits: true, from, size,
      query: buildQuery(f),
      sort: sortFor(sort, dir),
    },
  });
  const b = res.body || res;
  // return RAW rows (lowercased); the UI title-cases for display + Edit shows raw values (so exact-match
  // industry/size/country filters keep working). CSV title-cases server-side in rowToCsvLine.
  return { total: b.hits.total.value, rows: b.hits.hits.map((h) => h._source) };
}

async function count(client, f) {
  const res = await client.count({ index: INDEX, body: { query: buildQuery(f) } });
  return (res.body || res).count;
}

// Live facet counts. For each requested field the counts reflect all the OTHER active filters but NOT the
// field's own selection — so a facet shows what you'd get by adding each value (true faceted behavior).
const FACET_FIELDS = { industry: 'industry', size: 'size', country: 'country' };
async function facets(client, f, fields = ['industry', 'size', 'country'], topN = 25) {
  const out = {};
  await Promise.all(fields.map(async (field) => {
    if (!FACET_FIELDS[field]) { out[field] = []; return; }
    const sub = { ...(f || {}) }; delete sub[field];              // drop this facet's own selection
    const res = await client.search({
      index: INDEX,
      body: { size: 0, query: buildQuery(sub), aggs: { f: { terms: { field: FACET_FIELDS[field], size: topN } } } },
    });
    const b = res.body || res;
    out[field] = ((b.aggregations && b.aggregations.f.buckets) || []).map((x) => ({ key: x.key, count: x.doc_count }));
  }));
  return out;
}

// --- Alternate-Websites admin list (patterns whose match moves out of Website). Stored in OpenSearch so
// the Fly UI and the LOCAL enricher share one editable list; falls back to the default seed. ---
const CONFIG_INDEX = process.env.CC_CONFIG_INDEX || 'cc_config';
const DEFAULT_ALT = ['linktr.ee', 'youtube.com', 'crunchbase.com', 'twitter.com', 'x.com', 'yelp.com', 'etsy.com', 'about.me', 'sites.google.com', 'amazon.com/shop', 'wix.com', 'wixsite.com'];
async function getAltWebsites(client) {
  try { const g = await client.get({ index: CONFIG_INDEX, id: 'alt_websites' }); const p = (g.body || g)._source.patterns; return Array.isArray(p) && p.length ? p : DEFAULT_ALT; }
  catch (e) { return DEFAULT_ALT; }
}
async function setAltWebsites(client, patterns) {
  const list = (Array.isArray(patterns) ? patterns : []).map((p) => String(p).trim().toLowerCase().replace(/^https?:\/\//, '').replace(/^www\./, '')).filter(Boolean);
  await client.index({ index: CONFIG_INDEX, id: 'alt_websites', body: { patterns: list }, refresh: true });
  return { saved: list.length, patterns: list };
}

// Manual edit of a company doc (partial update by id). Only whitelisted fields; website edits keep domain
// in sync. NOTE: a full company re-load (from the source dataset) would overwrite manual edits.
const EDITABLE = new Set(['name', 'website', 'industry', 'size', 'country', 'region', 'locality', 'founded',
  'linkedin_url', 'phone', 'full_address', 'category', 'cid', 'sitemap_url',
  'description', 'email', 'email_type', 'facebook', 'instagram', 'map', 'linkedin_contact', 'bio_url', 'alternate_websites', 'contacts', 'contacts_count',
  'cc_refreshed_at', 'cc_crawl']);
async function update(client, id, updates) {
  if (!id) throw new Error('id required');
  const doc = {};
  for (const k in (updates || {})) {
    if (!EDITABLE.has(k)) continue;
    doc[k] = k === 'founded' ? (updates[k] === '' || updates[k] == null ? null : Number(updates[k]) || null) : String(updates[k] == null ? '' : updates[k]);
  }
  if ('website' in doc) doc.domain = normDomain(doc.website);
  if (!Object.keys(doc).length) return { updated: 0 };
  await client.update({ index: INDEX, id, body: { doc }, refresh: true });
  return { updated: 1, doc };
}

// Map a SERPER Places result onto company-field updates: title->name (only if name is blank),
// address->full_address, category, phoneNumber->phone, website->website(+domain), cid.
function placeUpdates(p, cur) {
  cur = cur || {}; const u = {};
  if (p.title && !String(cur.name || '').trim()) u.name = p.title;
  if (p.address) u.full_address = p.address;
  if (p.category) u.category = p.category;
  if (p.phoneNumber) u.phone = String(p.phoneNumber);
  if (p.website) { u.website = p.website; u.domain = normDomain(p.website); }
  if (p.cid != null && p.cid !== '') u.cid = String(p.cid);
  return u;
}

// Stream every matching company to onRow via search_after (for CSV export beyond the 50k window).
async function each(client, f, onRow, cap = 1000000) {
  let after = null, n = 0;
  for (;;) {
    const body = { size: 5000, query: buildQuery(f), sort: [{ 'name.kw': 'asc' }, { id: 'asc' }] };
    if (after) body.search_after = after;
    const res = await client.search({ index: INDEX, body });
    const hits = (res.body || res).hits.hits;
    if (!hits.length) break;
    for (const h of hits) { await onRow(h._source); if (++n >= cap) return n; }
    after = hits[hits.length - 1].sort;
  }
  return n;
}

// Effective sitemap: the discovered one if we have it, else the constructed {domain}/sitemap.xml default.
function effectiveSitemap(d) { return d.sitemap_url || (d.domain ? 'https://' + d.domain + '/sitemap.xml' : ''); }

// Proper/Title case for the lowercased PDL text fields (name/industry/locality/region/country). Keeps
// common acronyms/legal suffixes upper (LLC, USA…) and small joining words lower (of/and/the).
const TC_UP = new Set(['llc', 'inc', 'llp', 'plc', 'pllc', 'pc', 'ltd', 'usa', 'us', 'uk', 'uae', 'eu', 'ny', 'la', 'dc', 'it', 'hr', 'pr', 'ai', 'ii', 'iii', 'iv']);
const TC_LOW = new Set(['of', 'and', 'the', 'for', 'to', 'in', 'on', 'at', 'a', 'an', 'or', 'de', 'la', 'du']);
function titleCase(s) {
  const str = String(s == null ? '' : s);
  if (!str) return '';
  return str.toLowerCase().split(/(\s+|-|\/|,)/).map((w, i) => {
    if (/^(\s+|-|\/|,)$/.test(w) || !w) return w;
    if (TC_UP.has(w)) return w.toUpperCase();
    if (i > 0 && TC_LOW.has(w)) return w;
    return w.charAt(0).toUpperCase() + w.slice(1);
  }).join('');
}
// Apply proper case to the display/export text fields of a row (returns a shallow copy).
function prettyRow(d) {
  return { ...d, name: titleCase(d.name), industry: titleCase(d.industry), locality: titleCase(d.locality), region: titleCase(d.region), country: titleCase(d.country) };
}

const esc = (v) => `"${String(v == null ? '' : v).replace(/"/g, '""')}"`;
function rowToCsvLine(d0) {
  const d = prettyRow(d0);
  return [d.name, d.website, d.domain, d.contact_count || 0, effectiveSitemap(d0), d.industry, d.size, d.founded,
    d.locality, d.region, d.country, d.linkedin_url, d.phone, d.full_address, d.category, d.cid,
    d.description, d.email, d.email_type, d.facebook, d.instagram, d.map, d.linkedin_contact, d.bio_url,
    d.alternate_websites, d.contacts, d.contacts_count || 0].map(esc).join(',');
}
const csvHeader = () => OUT_COLS.join(',');

module.exports = { INDEX, MAPPING, OUT_COLS, CC_FIELDS, DEFAULT_ALT, normDomain, recordToDoc, ensureIndex, bulkIndex, buildQuery, search, count, facets, each, update, placeUpdates, getAltWebsites, setAltWebsites, effectiveSitemap, titleCase, prettyRow, rowToCsvLine, csvHeader, makeClient: os.makeClient };
