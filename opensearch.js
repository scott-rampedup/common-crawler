/**
 * opensearch.js — the PRODUCTION contacts search store (Phase 1 of the two-plane architecture).
 * ----------------------------------------------------------------------------------------------
 * The processing plane (worker fleet / Lambda) writes contacts to its own store; this module is the
 * read-optimized store the front-end UI queries (search / filter / facet / export). Decoupling the two
 * removes the shared-Postgres hotspot that melted under load.
 *
 * Access is IAM/SigV4 (the domain policy allows the cc-athena principal), so every request is signed
 * with the AWS creds already in env (Fly secrets on the app box, ~/.aws locally). Doc _id = normalized
 * email → re-indexing a contact upserts it (dedupe), and higher score wins via a scripted upsert.
 *
 *   const { makeClient, ensureIndex, bulkUpsert, rowToDoc, search } = require('./opensearch');
 *   const os = makeClient(process.env.OPENSEARCH_ENDPOINT);
 */
const { Client } = require('@opensearch-project/opensearch');
const { AwsSigv4Signer } = require('@opensearch-project/opensearch/aws');
const { defaultProvider } = require('@aws-sdk/credential-provider-node');

const INDEX = process.env.OPENSEARCH_INDEX || 'contacts';
const REGION = process.env.AWS_REGION || 'us-east-1';

function makeClient(endpoint) {
  const node = endpoint || process.env.OPENSEARCH_ENDPOINT;
  if (!node) throw new Error('OPENSEARCH_ENDPOINT (or endpoint arg) required');
  return new Client({
    ...AwsSigv4Signer({ region: REGION, service: 'es', getCredentials: () => defaultProvider()() }),
    node: node.startsWith('http') ? node : 'https://' + node,
  });
}

// Index mapping: text for search, keyword for filters/facets/export/sort, integer score for ranking.
const MAPPING = {
  settings: {
    number_of_shards: 2, number_of_replicas: 1,
    'index.mapping.total_fields.limit': 200,
    'index.max_result_window': 50000,   // allow deeper paging than the 10k default
  },
  mappings: { properties: {
    email:          { type: 'keyword' },
    first:          { type: 'text', fields: { kw: { type: 'keyword' } } },
    last:           { type: 'text', fields: { kw: { type: 'keyword' } } },
    name:           { type: 'text' },                                   // first + last, for search
    title:          { type: 'text', fields: { kw: { type: 'keyword' } } },
    position:       { type: 'keyword' },
    company:        { type: 'text', fields: { kw: { type: 'keyword' } } },
    employer:       { type: 'text', fields: { kw: { type: 'keyword' } } },   // schema.org worksFor (page-declared)
    work_location:  { type: 'keyword' },                                     // schema.org workLocation
    domain:         { type: 'keyword' },
    description:    { type: 'text' },
    path_id:        { type: 'keyword' },
    last_path:      { type: 'keyword' },
    email_type:     { type: 'keyword' },
    gender:         { type: 'keyword' },
    phone:          { type: 'keyword' },
    phone_type:     { type: 'keyword' },
    phone_location: { type: 'keyword' },
    phone_2:        { type: 'keyword' },
    phone_2_type:   { type: 'keyword' },
    phone_2_location:{ type: 'keyword' },
    linkedin_url:   { type: 'keyword' },
    facebook:       { type: 'keyword' },
    twitter:        { type: 'keyword' },
    whatsapp:       { type: 'keyword' },
    google_maps:    { type: 'keyword' },
    vcard:          { type: 'keyword' },
    web_source_url: { type: 'keyword' },
    image_url:      { type: 'keyword' },
    source:         { type: 'keyword' },
    directory:      { type: 'keyword' },
    bio_check:      { type: 'keyword' },
    type:           { type: 'keyword' },
    score:          { type: 'integer' },
    time_stamp:     { type: 'keyword' },
    updated_at:     { type: 'date' },
  } },
};

async function ensureIndex(client) {
  const exists = await client.indices.exists({ index: INDEX });
  if (exists.body) return false;
  await client.indices.create({ index: INDEX, body: MAPPING });
  return true;
}

// A CONTACT's linkedin_url must be a personal profile (linkedin.com/in/<slug>). Reject company pages and
// concatenated/garbled values (e.g. ".../in/https://www.linkedin.com/company-beta/…") so a bad value never
// sits in the field blocking a real /in URL from populating. Conservative: keeps clean /in + /pub profiles
// and person slugs that merely contain "company" (e.g. /in/the-ev-charging-company-ltd); clears only true
// company pages and concatenated junk.
function cleanContactLinkedin(u) {
  if (!u) return '';
  const s = String(u).trim();
  const low = s.toLowerCase();
  if (low.indexOf('linkedin.com', low.indexOf('linkedin.com') + 1) >= 0) return '';   // two linkedin.com -> concatenated
  if (low.indexOf('://', low.indexOf('://') + 1) >= 0) return '';                      // two schemes -> concatenated
  if (low.includes('linkedin.com/company')) return '';                                 // company page, not a person
  if (low.includes('linkedin.com/in/company/') || low.endsWith('linkedin.com/in/company')) return '';  // /in/company/… mis-path
  return s;
}

// Ensure every ingested contact gets a name + gender: derive First/Last from the email (firstname.lastname)
// when the name is blank, assign Gender from the names map (the premium real-person signal), and — when
// there is STILL no gender — fall back to the name implied by the person's linkedin.com/in slug.
// Lazily loads cc-home-enrich + li-name + the gender map; all cached. Never throws.
let _che = null, _gmap = null, _li = null;
const _cap = (s) => s ? s.charAt(0).toUpperCase() + s.slice(1).toLowerCase() : '';
const _gmapLoad = () => { if (!_gmap) { try { _gmap = require('./extractor').loadGenderMap(require('path').join(__dirname, 'names-genders.csv')); } catch (e) { _gmap = {}; } } return _gmap; };
const _liLoad = () => { if (_li === null) { try { _li = require('./li-name'); } catch (e) { _li = false; } } return _li; };
function ensureNameGender(doc) {
  try {
    // 1) blank name + a dotted email -> firstname.lastname. (The linkedin.com/in slug is handled in (3),
    //    which also knows when a slug name is good enough to REPLACE a name we already have.)
    if (!(doc.first && String(doc.first).trim()) && !(doc.last && String(doc.last).trim()) && doc.email) {
      if (!_che) { try { _che = require('./cc-home-enrich'); } catch (e) { _che = false; } }
      if (_che) {
        const ne = _che.nameFromEmail(doc.email || '');
        if (ne.first && ne.last) { doc.first = _cap(ne.first); doc.last = _cap(ne.last); doc.name = `${doc.first} ${doc.last}`.trim(); }
      }
    }
    // 2) gender from the name we have
    if ((!doc.gender || !String(doc.gender).trim()) && doc.first) {
      doc.gender = _gmapLoad()[String(doc.first).toLowerCase()] || '';
    }
    // 3) no name at all, or a name the map can't gender -> use the LinkedIn slug's name. li-name.resolve
    //    only overwrites an EXISTING name when the slug name resolves to a gender.
    const _noName = !(doc.first && String(doc.first).trim()) || !(doc.last && String(doc.last).trim());
    if ((_noName || !doc.gender || !String(doc.gender).trim()) && doc.linkedin_url) {
      const li = _liLoad();
      if (li) {
        const r = li.resolve({ first: doc.first, last: doc.last, gender: doc.gender, linkedinUrl: doc.linkedin_url });
        if (r) { doc.first = r.first; doc.last = r.last; doc.name = r.name; doc.gender = r.gender; }
      }
    }
  } catch (e) { /* best-effort */ }
  return doc;
}

// Map a Postgres contacts row (snake_case columns) to an OpenSearch document.
function rowToDoc(r) {
  const first = r.first || '', last = r.last || '';
  return ensureNameGender({
    email: r.email || r.email_address || '',
    first, last, name: `${first} ${last}`.trim(),
    title: r.title || '', position: r.position || '',
    company: r.company || r.domain || '', domain: r.domain || '',
    description: r.description || '',
    path_id: r.path_id || '', last_path: r.last_path || '',
    email_type: r.email_type || '', gender: r.gender || '',
    phone: r.phone || '', phone_type: r.phone_type || '', phone_location: r.phone_location || '',
    phone_2: r.phone_2 || '', phone_2_type: r.phone_2_type || '', phone_2_location: r.phone_2_location || '',
    linkedin_url: cleanContactLinkedin(r.linkedin_url), facebook: r.facebook || '', twitter: r.twitter || '',
    whatsapp: r.whatsapp || '', google_maps: r.google_maps || '', vcard: r.vcard || '',
    web_source_url: r.web_source_url || '', image_url: r.image_url || '',
    source: r.source || '', directory: r.directory || '', bio_check: r.bio_check || '', type: r.type || '',
    score: Number(r.score) || 0, time_stamp: r.time_stamp || '',
    updated_at: r.updated_at || null,
  });
}

// A usable OpenSearch _id: non-empty and within the 512-byte hard cap. A malformed contact (e.g. an email
// on a runaway "www.www.www…" domain) that exceeds it makes OpenSearch reject the WHOLE bulk request — which
// would stall the delta-syncer on that batch forever. Skip such docs so one bad row can't block everyone.
function validId(email) { const id = String(email || '').toLowerCase(); return id && Buffer.byteLength(id) <= 512 ? id : ''; }

// Bulk index docs, _id = email. Score-gated upsert: a re-indexed email only overwrites if its score is
// >= the stored score (mirrors the Postgres upsert), so the best record for a person wins.
// ---- opt-out suppression: emails a person confirmed removal for live in the SEPARATE `optout` index.
// Every ingestion path funnels through bulkUpsert, so filtering here means a re-crawled opted-out person is
// dropped again automatically (their "removed again if processed in the future"). Cached 5 min. ----
const OPTOUT_INDEX = process.env.OPTOUT_INDEX || 'optout';
let _suppress = { set: new Set(), at: 0 };
function invalidateSuppression() { _suppress.at = 0; }
async function suppressedSet(client) {
  if (Date.now() - _suppress.at < 300000) return _suppress.set;
  const set = new Set();
  try {
    const r = await client.search({ index: OPTOUT_INDEX, body: { size: 10000, _source: ['email'], query: { term: { status: 'confirmed' } } } });
    for (const h of ((r.body || r).hits.hits || [])) if (h._source && h._source.email) set.add(String(h._source.email).toLowerCase());
  } catch (e) { /* optout index absent -> nothing suppressed yet */ }
  _suppress = { set, at: Date.now() };
  return set;
}

async function bulkUpsert(client, docs) {
  const sup = await suppressedSet(client);
  const body = [];
  for (const d of docs) {
    const id = validId(d.email);
    if (!id) continue;
    if (sup.has(id)) continue;                    // opted out -> never (re)add

    body.push({ update: { _index: INDEX, _id: id } });
    body.push({
      scripted_upsert: true, upsert: d,
      script: { lang: 'painless',
        source: "if (params.doc.score >= ctx._source.score) { ctx._source = params.doc }",
        params: { doc: d } },
    });
  }
  if (!body.length) return { indexed: 0, errors: 0 };
  const res = await client.bulk({ body, refresh: false });
  let errors = 0;
  if (res.body.errors) for (const it of res.body.items) if (it.update && it.update.error) errors++;
  return { indexed: docs.length, errors };
}

// ---------------------------------------------------------------------------------------------------
// READ PATH — the UI's Master DB tab (search / filter / export / facets) served from OpenSearch.
// Mirrors db-pg.js query()/each()/facets()/stats() so ui-server can swap backends behind a flag:
// same filter semantics, same {rows,total,approx,page,pageSize} shape, same display-field records.
// ---------------------------------------------------------------------------------------------------
const colName = (f) => String(f).toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, '');

// doc (snake_case) -> display record ('Email Address', 'First', ...), the shape the front-end expects.
const FIELD_TO_DOC = {
  'Time Stamp': 'time_stamp', 'Source': 'source', 'Web Source URL': 'web_source_url', 'Directory': 'directory',
  'Path ID': 'path_id', 'Last Path': 'last_path', 'Bio Check': 'bio_check', 'First': 'first', 'Last': 'last',
  'Gender': 'gender', 'Title': 'title', 'Position': 'position', 'Employer': 'employer', 'Location': 'work_location',
  'Description': 'description', 'Image URL': 'image_url',
  'Email Address': 'email', 'Email Type': 'email_type', 'LinkedIn URL': 'linkedin_url', 'Facebook': 'facebook',
  'Twitter': 'twitter', 'WhatsApp': 'whatsapp', 'Google Maps': 'google_maps', 'vCard': 'vcard', 'Phone': 'phone',
  'Phone Type': 'phone_type', 'Phone Location': 'phone_location', 'Phone 2': 'phone_2', 'Phone 2 Type': 'phone_2_type',
  'Phone 2 Location': 'phone_2_location', 'Type': 'type',
};
function docToRecord(doc) {
  const rec = {};
  for (const f in FIELD_TO_DOC) rec[f] = doc[FIELD_TO_DOC[f]] || '';
  rec.Domain = doc.domain || '';
  // firmographics are READ-ONLY here (not in FIELD_TO_DOC, so recordToDoc never writes them back — a
  // re-load/edit can't wipe the enrichment).
  rec.Industry = doc.industry || '';
  rec['Company Size'] = doc.company_size || '';
  rec['Company HQ'] = doc.company_hq || '';
  rec.Founded = doc.company_founded || '';
  rec['Company Name'] = doc.company_name || '';
  rec['Company LinkedIn'] = doc.company_linkedin || '';
  rec['New Hire'] = (doc.source === 'Sitemap Monitor') ? 'Y' : '';   // derived flag for the Contact Crawler
  rec.updated_at = doc.updated_at || '';                             // Last Updated column (display + already sortable)
  return rec;
}

// Columns we can sort on server-side (keyword / .kw subfield). Mirrors db-pg SORT_COLS best-effort.
const SORTABLE = {
  first: 'first.kw', last: 'last.kw', title: 'title.kw', company: 'company.kw',
  directory: 'directory', email_type: 'email_type', phone_type: 'phone_type', gender: 'gender',
  domain: 'domain', position: 'position', phone: 'phone', type: 'type', source: 'source', email: 'email',
  updated_at: 'updated_at',   // Last Updated column (date) — sortable in the Contact Crawler
};

// Translate the UI filter opts into an OpenSearch query. Same semantics as db-pg whereFor():
// exact (case-insensitive) for facet dropdowns, substring for position/location, subdomain match for
// domains[], non-empty for linkedin, M/F buckets for gender, and free-text search across the person fields.
function buildQuery(o = {}) {
  const filter = [], must = [];
  const ciTerm = (field, value) => ({ term: { [field]: { value: String(value), case_insensitive: true } } });
  const ciWild = (field, value) => ({ wildcard: { [field]: { value: '*' + String(value).toLowerCase() + '*', case_insensitive: true } } });
  if (o.directory) filter.push(ciTerm('directory', o.directory));
  if (o.emailType) filter.push(ciTerm('email_type', o.emailType));
  if (o.phoneType) filter.push(ciTerm('phone_type', o.phoneType));
  if (o.type) filter.push(ciTerm('type', o.type));
  if (o.domain) filter.push({ term: { domain: String(o.domain).toLowerCase() } });
  if (o.position) filter.push(ciWild('position', o.position));
  if (o.location) filter.push({ bool: { minimum_should_match: 1, should: [ciWild('phone_location', o.location), ciWild('phone_2_location', o.location)] } });
  // firmographics (company-dataset enrichment): industry/size are exact facets, HQ is substring, founded a range
  if (o.industry) filter.push(ciTerm('industry.keyword', o.industry));
  if (o.companySize) filter.push({ term: { 'company_size.keyword': String(o.companySize) } });
  if (o.companyLocation) filter.push(ciWild('company_hq', o.companyLocation));
  { const fr = {}; if (o.foundedMin) fr.gte = Number(o.foundedMin); if (o.foundedMax) fr.lte = Number(o.foundedMax);
    if (fr.gte != null || fr.lte != null) filter.push({ range: { company_founded: fr } }); }
  if (Array.isArray(o.domains) && o.domains.length) {
    const should = [];
    for (const d of o.domains) {
      const dl = String(d || '').trim().toLowerCase().replace(/^www\./, '');
      if (!dl) continue;
      should.push({ term: { domain: dl } }, { wildcard: { domain: { value: '*.' + dl } } });
    }
    if (should.length) filter.push({ bool: { minimum_should_match: 1, should } });
  }
  if (o.linkedin) filter.push({ bool: { must_not: [{ term: { linkedin_url: '' } }] } });
  if (o.newHire) filter.push(ciTerm('source', 'Sitemap Monitor'));   // NEW HIRE = detected by the Sitemap Monitor
  switch (o.gender) {
    case 'male': filter.push(ciTerm('gender', 'M')); break;
    case 'female': filter.push(ciTerm('gender', 'F')); break;
    case 'all': filter.push({ terms: { gender: ['M', 'F', 'm', 'f'] } }); break;
    case 'none': filter.push({ bool: { must_not: [{ terms: { gender: ['M', 'F', 'm', 'f'] } }] } }); break;
    default: break;
  }
  if (o.search) must.push({ multi_match: { query: String(o.search), type: 'cross_fields', operator: 'and',
    fields: ['name^2', 'first', 'last', 'title', 'company', 'description', 'position', 'email', 'domain', 'phone'] } });
  if (!filter.length && !must.length) return { match_all: {} };
  const bool = {};
  if (filter.length) bool.filter = filter;
  if (must.length) bool.must = must;
  return { bool };
}

function sortFor(o = {}) {
  let col = colName(o.sort || ''); if (o.sort === 'Domain') col = 'domain';
  if (SORTABLE[col]) return [{ [SORTABLE[col]]: { order: Number(o.dir) === -1 ? 'desc' : 'asc', missing: '_last' } }];
  // Default view: most recently UPDATED first — newest "last updated date" at the top of the Contact
  // Crawler. new_hire is a secondary tiebreaker (fresh detections stamp a current updated_at, so they
  // still surface at the top), and email is a stable final tiebreaker for deterministic paging.
  return [
    { updated_at: { order: Number(o.dir) === -1 ? 'asc' : 'desc', missing: '_last' } },
    { new_hire: { order: 'desc', missing: '_last' } },
    { email: 'asc' },
  ];
}

async function search(client, o = {}) {
  const pageSize = Math.min(500, Math.max(1, Number(o.pageSize) || 50));
  const page = Math.max(1, Number(o.page) || 1);
  let from = (page - 1) * pageSize;
  const WINDOW = 50000;
  if (from + pageSize > WINDOW) from = Math.max(0, WINDOW - pageSize);   // stay within max_result_window
  // Exact total: OpenSearch counts matching docs cheaply (unlike the Postgres COUNT(*) full-scan that
  // forced the old 10k display cap), so show the real number. Deep paging past max_result_window is still
  // clamped above; the UI filters long before that, and export streams all matches via search_after.
  const res = await client.search({ index: INDEX, body: {
    track_total_hits: true, from, size: pageSize, query: buildQuery(o), sort: sortFor(o) } });
  const h = res.body.hits;
  const total = typeof h.total === 'object' ? h.total.value : h.total;
  const approx = (typeof h.total === 'object' && h.total.relation === 'gte') ? 'capped' : null;
  return { rows: h.hits.map((x) => docToRecord(x._source)), total, approx, page, pageSize };
}

// Stream every matching record to cb, keyset-paginated by email via search_after (efficient at scale).
async function each(client, o, cb) {
  const query = buildQuery(o || {});
  let after;
  for (;;) {
    const body = { size: 5000, query, sort: [{ email: 'asc' }] };
    if (after) body.search_after = after;
    const hits = (await client.search({ index: INDEX, body })).body.hits.hits;
    if (!hits.length) break;
    for (const x of hits) cb(docToRecord(x._source));
    if (hits.length < 5000) break;
    after = hits[hits.length - 1].sort;
  }
}

async function facets(client) {
  const agg = (field) => ({ terms: { field, size: 1000, order: { _key: 'asc' } } });
  const res = await client.search({ index: INDEX, body: { size: 0, aggs: {
    directory: agg('directory'), emailType: agg('email_type'), phoneType: agg('phone_type'), type: agg('type'),
    industry: agg('industry.keyword'), companySize: agg('company_size.keyword') } } });
  const a = res.body.aggregations;
  const vals = (k) => (a[k].buckets || []).map((b) => b.key).filter((v) => v !== '');
  return { directory: vals('directory'), emailType: vals('emailType'), phoneType: vals('phoneType'), type: vals('type'),
    industry: vals('industry'), companySize: vals('companySize') };
}

async function count(client, o) {
  const body = { query: o ? buildQuery(o) : { match_all: {} } };
  return (await client.count({ index: INDEX, body })).body.count;
}
async function stats(client) { return { total: await count(client) }; }

// ---- authoritative writes (UI edits / deletes) — bypass the score gate, the analyst's change wins ----
const SCORE_FIELDS_D = ['First', 'Last', 'Title', 'Position', 'Phone', 'Phone 2', 'LinkedIn URL',
  'Gender', 'Phone Location', 'Image URL', 'Description', 'Google Maps'];
function recordScore(rec) { let s = 0; for (const f of SCORE_FIELDS_D) if (String(rec[f] || '').trim()) s++; return s; }

// display-field record (rowToRecord shape) -> OpenSearch doc. Inverse of docToRecord.
function recordToDoc(rec, updatedAt) {
  const first = rec['First'] || '', last = rec['Last'] || '';
  const doc = {};
  for (const f in FIELD_TO_DOC) doc[FIELD_TO_DOC[f]] = rec[f] || '';
  doc.linkedin_url = cleanContactLinkedin(doc.linkedin_url);   // /in only — no company pages / concatenated junk
  doc.name = `${first} ${last}`.trim();
  doc.domain = rec['Domain'] || '';
  doc.company = rec['Domain'] || doc.company || '';
  doc.score = recordScore(rec);
  doc.new_hire = rec['Source'] === 'Sitemap Monitor';   // drives the default "new hires first" sort
  doc.updated_at = updatedAt || null;
  return ensureNameGender(doc);   // derive name from email/linkedin if blank + assign gender from name
}

// Unconditional bulk index (_id = email). Use for UI edits so they always overwrite regardless of score.
async function indexDocs(client, docs) {
  const body = [];
  for (const d of docs) { const id = validId(d.email); if (!id) continue; body.push({ index: { _index: INDEX, _id: id } }, d); }
  if (!body.length) return { indexed: 0, errors: 0 };
  const res = await client.bulk({ body, refresh: false });
  let errors = 0;
  if (res.body.errors) for (const it of res.body.items) if (it.index && it.index.error) errors++;
  return { indexed: docs.length, errors };
}

// Bulk delete by email (_id). Deletes can't be captured by an updated_at delta scan — the row is gone.
async function bulkDelete(client, emails) {
  const body = [];
  for (const e of emails) { const id = String(e || '').trim().toLowerCase(); if (id) body.push({ delete: { _index: INDEX, _id: id } }); }
  if (!body.length) return { deleted: 0 };
  const res = await client.bulk({ body, refresh: false });
  let deleted = 0;
  if (res.body.items) for (const it of res.body.items) if (it.delete && (it.delete.result === 'deleted' || it.delete.status === 200)) deleted++;
  return { deleted };
}

module.exports = {
  makeClient, ensureIndex, rowToDoc, bulkUpsert, INDEX, MAPPING,
  search, each, facets, count, stats, docToRecord, buildQuery,
  recordToDoc, indexDocs, bulkDelete, cleanContactLinkedin, invalidateSuppression, OPTOUT_INDEX,
};
