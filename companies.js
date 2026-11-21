/**
 * companies.js — the `companies` OpenSearch index (People Data Labs free company dataset) + search.
 * -------------------------------------------------------------------------------------------------
 * A reference dataset of ~35.6M companies keyed by id, searchable by name / website / industry / size /
 * country / region / locality / founded. Powers the Company Crawler tab. Reuses the shared OpenSearch
 * client + endpoint (opensearch.js). Loaded once by load-companies.js from the gzip NDJSON (0 replicas —
 * it's static, re-loadable reference data, so we skip the replica to save disk).
 */
const os = require('./opensearch');
const naics = require('./naics');

const INDEX = process.env.COMPANIES_INDEX || 'companies';
const OUT_COLS = ['name', 'website', 'domain', 'company_type', 'website_type', 'location_count',
  'contact_count', 'sitemap_url', 'industry', 'size', 'founded',
  'locality', 'region', 'country', 'linkedin_url', 'phone', 'phone_type', 'full_address', 'category', 'naics_code', 'naics_title', 'cid',
  'description', 'email', 'email_type', 'facebook', 'instagram', 'whatsapp', 'linkedin_contact', 'bio_url', 'team_page',
  'source_map', 'cc_refreshed_at', 'map', 'alternate_websites', 'contacts', 'contacts_count'];
// CC home-page enrichment fields (populated by cc-home-enrich via /api/companies/cc-enrich).
// Fields a Common Crawl enrichment is allowed to write. load-enriched.js filters every incoming update
// against this list, so a field missing here is parsed, carried, and then silently dropped at the door —
// which is what would have happened to logo/postal_code/full_address the first time the refresh ran.
const CC_FIELDS = ['description', 'email', 'email_type', 'facebook', 'instagram', 'map', 'linkedin_contact',
  'bio_url', 'alternate_websites', 'contacts', 'contacts_count',
  'logo', 'postal_code', 'full_address', 'locality', 'region', 'country', 'whatsapp', 'team_page'];

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
      // keyword, never text: a hash is matched whole or not at all, and an analyzer would tokenise it
      email_hash:    { type: 'keyword' },
      naics_code:   { type: 'keyword' },                                 // NAICS 6 code from category (or industry) via naics-enrich
      naics_title:  { type: 'text', fields: { kw: { type: 'keyword' } } },
      email_pattern: { type: 'keyword' },    // local-part template for modelled emails, e.g. {first}.{last}
      email_domain:  { type: 'keyword' },    // domain used for modelled emails (may differ from the website domain)
      // The Location's own page, e.g. …/locations/manchester-blood-donor-centre. A Location-scoped
      // crawl fetches THIS, not the shared homepage -- without it every branch of a chain returns
      // identical data. keyword: it is an identifier to match and filter on, never analysed text.
      page_url:      { type: 'keyword' },
      root_domain:   { type: 'keyword' },    // registrable domain the record groups under
      logo:          { type: 'keyword' },    // org logo from the home page (JSON-LD logo / og:image / touch icon)
      postal_code:   { type: 'keyword' },    // from a schema.org PostalAddress, or the Bing Zip column
    },
  },
};

function normDomain(w) {
  if (!w) return '';
  return String(w).toLowerCase().trim().replace(/^https?:\/\//, '').replace(/^www\./, '').split('/')[0].split('?')[0];
}

// Place/industry values are stored LOWER-CASE and title-cased for display (see shapeRow). That is not
// a cosmetic preference: buildQuery lower-cases the country/industry filter values, so a record stored
// as "United States" cannot be reached by any filter at all. The Google/Bing Location path used to pass
// the source casing straight through, which left 3.19M records invisible to the facets they populated
// — the facet showed "United States 2,400,640" and clicking it returned the 7.7M lower-case bucket.
// Every writer of country/region/locality/industry goes through this.
const canonPlace = (v) => String(v == null ? '' : v).trim().toLowerCase();
function canonPlaces(doc) {
  for (const f of ['country', 'region', 'locality', 'industry']) {
    if (doc[f] != null) doc[f] = canonPlace(doc[f]);
  }
  return doc;
}

// One raw PDL record -> an index doc.
function recordToDoc(r) {
  return {
    id: r.id || '', name: r.name || '', website: r.website || '', domain: normDomain(r.website),
    industry: canonPlace(r.industry), size: r.size || '',
    founded: (r.founded != null && r.founded !== '') ? (Number(r.founded) || null) : null,
    locality: canonPlace(r.locality), region: canonPlace(r.region), country: canonPlace(r.country), linkedin_url: r.linkedin_url || '',
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
  for (const d of docs) { if (!d.id) continue; naics.assignNaics(d); body.push({ index: { _index: INDEX, _id: d.id } }, d); }
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
  // Email Address type. email_type is written by the CC enricher and only dynamically mapped, so the
  // aggregatable/filterable field is the .keyword subfield -- filtering the analysed text field would match
  // "Role-Based" for a "Professional" query on token overlap. Live values: Professional 883,267,
  // Personal 456,721, Role-Based 986,556, empty 4,313,573.
  //   any  -> has any classified email      none -> has no email at all
  if (f.emailType) {
    const et = String(f.emailType);
    const nonEmpty = { bool: { filter: [{ exists: { field: 'email_type.keyword' } }], must_not: [{ term: { 'email_type.keyword': '' } }] } };
    if (et === 'any') filter.push(nonEmpty);
    else if (et === 'none') filter.push({ bool: { must_not: [nonEmpty] } });
    else filter.push({ terms: { 'email_type.keyword': multi(et) } });
  }
  // Has a team page. team_page is a text field, and an EMPTY STRING still satisfies `exists` on it --
  // that is how a "has team page" count once read 5,861,666 against a true 2,230,565. Test the .keyword
  // subfield and exclude '' explicitly, the same way emailType above does.
  if (f.teamPage) {
    const has = { bool: { filter: [{ exists: { field: 'team_page.keyword' } }], must_not: [{ term: { 'team_page.keyword': '' } }] } };
    if (String(f.teamPage) === 'no') filter.push({ bool: { must_not: [has] } });
    else filter.push(has);
  }
  if (f.companyType) filter.push({ term: { 'company_type.keyword': String(f.companyType) } });   // HQ | Location (Google Maps)
  if (f.websiteType) filter.push({ match_phrase: { website_type: String(f.websiteType) } });      // People | Location | BIO URL | Company | … (case-insensitive)
  if (f.naics) { const s = String(f.naics).trim(); filter.push({ bool: { should: [   // NAICS code (exact) OR title (phrase)
    { term: { naics_code: s } }, { term: { 'naics_code.keyword': s } }, { match_phrase: { naics_title: s } }], minimum_should_match: 1 } }); }
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
  // De-duplicated: the saved list already holds "about.me" twice, and a repeated pattern makes the
  // count reported back to the admin wrong while doing nothing useful.
  const list = [...new Set((Array.isArray(patterns) ? patterns : [])
    .map((p) => String(p).trim().toLowerCase().replace(/^https?:\/\//, '').replace(/^www\./, '').replace(/\/+$/, ''))
    .filter(Boolean))];
  await client.index({ index: CONFIG_INDEX, id: 'alt_websites', body: { patterns: list }, refresh: true });
  return { saved: list.length, patterns: list };
}

// --- Role-Based email terms (admin list). Lives here beside the other cc_config admin lists rather than
// in extractor.js, which is deliberately dependency-free and has no OpenSearch client. extractor owns the
// MATCHING (setAdminRoleTerms); this owns the STORAGE, and ui-server joins them on boot + on save.
// Empty by default: the built-in ROLE_LOCALS always applies, and this only ever adds to it. ---
async function getRoleEmailTerms(client) {
  try { const g = await client.get({ index: CONFIG_INDEX, id: 'role_email_terms' }); const t = (g.body || g)._source.terms; return Array.isArray(t) ? t : []; }
  catch (e) { return []; }                                     // never saved yet -> no extra terms
}
async function setRoleEmailTerms(client, terms) {
  // Same normalization extractor.normalizeRoleTerm applies, so what's stored is what will match: a bare
  // local-part token, lowercased, with any @domain and surrounding punctuation stripped.
  const list = [...new Set((Array.isArray(terms) ? terms : [])
    .map((t) => String(t == null ? '' : t).trim().toLowerCase().split('@')[0].replace(/^[^a-z0-9]+|[^a-z0-9]+$/g, ''))
    .filter(Boolean))];
  await client.index({ index: CONFIG_INDEX, id: 'role_email_terms', body: { terms: list }, refresh: true });
  return { saved: list.length, terms: list };
}

// Manual edit of a company doc (partial update by id). Only whitelisted fields; website edits keep domain
// in sync. NOTE: a full company re-load (from the source dataset) would overwrite manual edits.
const EDITABLE = new Set(['name', 'website', 'industry', 'size', 'country', 'region', 'locality', 'founded',
  'linkedin_url', 'phone', 'full_address', 'category', 'cid', 'sitemap_url',
  'description', 'email', 'email_type', 'facebook', 'instagram', 'map', 'linkedin_contact', 'bio_url', 'alternate_websites', 'contacts', 'contacts_count',
  'cc_refreshed_at', 'cc_crawl', 'email_pattern', 'email_domain',
  // home-page enrichment outputs; without these co.update() drops them without a word
  'logo', 'postal_code', 'whatsapp', 'team_page',
  // SHA-256 over the Public Prospects canonical address form; see email-hash.js
  'email_hash',
  // registrable domain + the parent company's name. Both were in the mapping at 0.0% with nothing
  // writing them; without them here co.update() would drop the refresh's root_domain in silence.
  'root_domain', 'company_name']);
// ---- root_domain -------------------------------------------------------------------------------
// The REGISTRABLE domain (eTLD+1), which is what makes this field worth having: domain already holds
// the host, so a root_domain that merely repeats it is a duplicate column. Reduced properly,
// shadyside.pizzaparma.us and pizzaparma.us share one root_domain and the locations of a chain group
// under their company.
//
// Suffixes come from data/tld.json, which carries 299 multi-part entries (co.uk, com.au, ac.jp) beside
// the 294 single ones. It is not the full public suffix list, so this is approximate in the same way
// Public Prospects' pp_registrable is approximate -- the long tail of private suffixes
// (something.s3.amazonaws.com) reduces one label too far. For company domains that is rare enough to
// accept; what it must never do is return something SHORTER than a real registrable domain for the
// common cases, and the longest-suffix match below is what guarantees that.
let TLD_SUFFIXES = null;
function tldSuffixes() {
  if (!TLD_SUFFIXES) {
    try { TLD_SUFFIXES = new Set(Object.keys(require('./data/tld.json'))); }
    catch (e) { TLD_SUFFIXES = new Set(['com', 'org', 'net', 'edu', 'gov', 'io', 'co']); }
  }
  return TLD_SUFFIXES;
}
function registrableDomain(host) {
  const h = String(host == null ? '' : host).trim().toLowerCase()
    .replace(/^https?:\/\//, '').replace(/[\/:?#].*$/, '').replace(/^www\./, '').replace(/\.+$/, '');
  if (!h || !h.includes('.')) return '';
  const labels = h.split('.').filter(Boolean);
  const sfx = tldSuffixes();
  // longest matching suffix wins, so co.uk beats uk and the label kept is the real registrant's
  for (let i = Math.max(0, labels.length - 3); i < labels.length; i++) {
    if (sfx.has(labels.slice(i).join('.'))) return i > 0 ? labels.slice(i - 1).join('.') : h;
  }
  return labels.slice(-2).join('.');     // unknown suffix: assume a single-label TLD
}

// ---- field shape ---------------------------------------------------------------------------------
// Fields that legitimately hold MORE THAN ONE value, stored as arrays. bio_url carries >1 on 30.2% of
// records and reaches 114; linkedin_contact 26.5% and reaches 64. Joined into one '; ' string they
// blew past the keyword subfield's ignore_above:256 and were silently dropped from the keyword index
// altogether -- unmatchable and unaggregatable. As arrays each element is indexed on its own.
const MULTI_VALUE = new Set(['bio_url', 'linkedin_contact', 'linkedin_url', 'alternate_websites']);

// Reads either shape, so records written before the conversion still come back as lists.
function asList(v) {
  if (v == null) return [];
  const arr = Array.isArray(v) ? v : String(v).split(/[;\s]+/);
  return [...new Set(arr.map((x) => String(x == null ? '' : x).trim()).filter(Boolean))];
}

// THE one place that decides how a value is written to the index. update() and bulkUpdate() carried
// identical copies of this line; a second writer drifting from the first is exactly how a field ends
// up in two shapes at once.
function coerceField(k, v) {
  if (k === 'founded') return (v === '' || v == null) ? null : (Number(v) || null);
  if (MULTI_VALUE.has(k)) return asList(v);          // [] is a deliberate clear, not an accident
  return String(v == null ? '' : v);
}

async function update(client, id, updates) {
  if (!id) throw new Error('id required');
  const doc = {};
  for (const k in (updates || {})) {
    if (!EDITABLE.has(k)) continue;
    doc[k] = coerceField(k, updates[k]);
  }
  if ('website' in doc) doc.domain = normDomain(doc.website);
  if (!Object.keys(doc).length) return { updated: 0 };
  await client.update({ index: INDEX, id, body: { doc }, refresh: true });
  return { updated: 1, doc };
}

// Bulk variant of update(): apply the SAME validated field(s) to many ids in one _bulk round-trip per
// chunk, refreshing once at the end — far cheaper than N single refresh:true updates. Reuses the EDITABLE
// whitelist + founded/website coercion. Returns { updated, errors, total }.
async function bulkUpdate(client, ids, updates) {
  const doc = {};
  for (const k in (updates || {})) {
    if (!EDITABLE.has(k)) continue;
    doc[k] = coerceField(k, updates[k]);
  }
  if ('website' in doc) doc.domain = normDomain(doc.website);
  const list = [...new Set((ids || []).map((x) => String(x == null ? '' : x).trim()).filter(Boolean))];
  if (!Object.keys(doc).length || !list.length) return { updated: 0, errors: 0, total: list.length };
  let updated = 0, errors = 0;
  for (let i = 0; i < list.length; i += 500) {
    const body = [];
    for (const id of list.slice(i, i + 500)) body.push({ update: { _index: INDEX, _id: id } }, { doc });
    const r = await client.bulk({ body, refresh: false });
    for (const it of (((r.body || r).items) || [])) { const u = it.update || {}; if (u.error || (u.status && u.status >= 400)) errors++; else updated++; }
  }
  try { await client.indices.refresh({ index: INDEX }); } catch (e) { /* refresh is best-effort */ }
  return { updated, errors, total: list.length };
}

// Read a website domain's stored email model (admin-set or learned) for future modelling, or null.
async function getEmailModel(client, domain) {
  const d = normDomain(domain); if (!d) return null;
  try {
    const r = await client.search({ index: INDEX, body: {
      size: 1, _source: ['email_pattern', 'email_domain'],
      query: { bool: { filter: [{ term: { domain: d } }, { exists: { field: 'email_pattern' } }] } } } });
    const s = (((r.body || r).hits.hits || [])[0] || {})._source;
    if (s && s.email_pattern) return { pattern: s.email_pattern, email_domain: s.email_domain || d };
  } catch (e) { /* best-effort */ }
  return null;
}

// Persist the email model (pattern and/or domain) onto every company sharing this website domain.
async function setEmailModelByDomain(client, domain, updates) {
  const d = normDomain(domain); if (!d) return { updated: 0 };
  const doc = {};
  if (updates && updates.pattern) doc.email_pattern = String(updates.pattern);
  if (updates && updates.email_domain) doc.email_domain = normDomain(updates.email_domain);
  if (!Object.keys(doc).length) return { updated: 0 };
  let ids = [];
  try {
    const r = await client.search({ index: INDEX, body: { size: 1000, _source: false, query: { term: { domain: d } } } });
    ids = ((r.body || r).hits.hits || []).map((h) => h._id);
  } catch (e) { return { updated: 0 }; }
  if (!ids.length) return { updated: 0 };
  return bulkUpdate(client, ids, doc);
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

// An array renders with the SAME '; ' separator these fields used when they were joined strings, so
// every downstream consumer of the CSV is unaffected by the array conversion. String(['a','b']) would
// have given 'a,b' -- commas, inside a comma-separated file.
const esc = (v) => `"${(Array.isArray(v) ? v.join('; ') : String(v == null ? '' : v)).replace(/"/g, '""')}"`;
function rowToCsvLine(d0) {
  const d = prettyRow(d0);
  return [d.name, d.website, d.domain, d.company_type, d.website_type, d.location_count || 0,
    d.contact_count || 0, effectiveSitemap(d0), d.industry, d.size, d.founded,
    d.locality, d.region, d.country, d.linkedin_url, d.phone, d.phone_type, d.full_address, d.category, d.naics_code, d.naics_title, d.cid,
    d.description, d.email, d.email_type, d.facebook, d.instagram, d.whatsapp, d.linkedin_contact, d.bio_url, d.team_page,
    d.source_map, d.cc_refreshed_at, d.map, d.alternate_websites, d.contacts, d.contacts_count || 0].map(esc).join(',');
}
const csvHeader = () => OUT_COLS.join(',');

// Hosts that must NEVER be a company's primary domain: free email, social profiles, video, directories,
// and shared site-builders / free hosts. A Google-Maps / AllThePlaces "website" pointing at one of these
// is NOT a real web source, so the record is treated as website-less (skipped) rather than merging
// thousands of unrelated businesses under one junk host (e.g. domain=facebook.com).
// The list itself moved to non-company-domains.js on 2026-11-19 so the CONTACTS path could apply the
// same rule: opensearch.js cannot require this module (this module already requires it), so a shared
// leaf module is the only place both can reach. Re-exported below unchanged, and the set grew by the
// 23 hosts from Scott's alternate-domain list — so the loaders below now skip those too.
const { NON_COMPANY_DOMAINS, isBadCompanyDomain } = require('./non-company-domains');

// Which write blocks are set on an index, from the body of
// GET /<index>/_settings?filter_path=**.blocks -- pure, so it can be tested against the real payload
// without taking a production index read-only to produce one.
//
// Values come back as the STRINGS "true"/"false", not booleans, which is why this compares to 'true'
// rather than trusting truthiness: the string "false" is truthy and would report a block that is off.
// Observed shape, captured from the cluster on 2026-11-21 while the flood-stage block was on:
//   {"companies":{"settings":{"index":{"blocks":{"read_only_allow_delete":"true","write":"true"}}}}}
// An index with no blocks is absent from the body entirely, so a missing key means writable.
function blockedIndexWrites(body, index) {
  const blocks = ((((((body || {})[index] || {}).settings || {}).index || {}).blocks) || {});
  return Object.keys(blocks).filter((k) => String(blocks[k]) === 'true').sort();
}

// Least free disk across the DATA nodes, from the body of
// GET /_cat/allocation?h=node,disk.avail&bytes=b -- in bytes, or null when nothing parseable is there.
//
// _cat/allocation prints an UNASSIGNED row with no node and no numbers whenever shards are unassigned,
// and the header row is present when v is passed. Both must be skipped rather than parsed as 0, which
// would read as "no disk left" and stop every scheduled job.
function leastFreeBytes(catText) {
  let min = Infinity;
  for (const line of String(catText == null ? '' : catText).trim().split('\n')) {
    const cols = line.trim().split(/\s+/);
    if (!cols.length) continue;
    const v = Number(cols[1]);
    if (cols[1] === undefined || cols[1] === '' || !Number.isFinite(v)) continue;  // header / UNASSIGNED
    min = Math.min(min, v);
  }
  return Number.isFinite(min) ? min : null;
}

module.exports = { INDEX, MAPPING, OUT_COLS, MULTI_VALUE, asList, blockedIndexWrites, leastFreeBytes, coerceField, registrableDomain, CC_FIELDS, DEFAULT_ALT, NON_COMPANY_DOMAINS, isBadCompanyDomain, normDomain, canonPlace, canonPlaces, recordToDoc, ensureIndex, bulkIndex, buildQuery, search, count, facets, each, update, placeUpdates, getAltWebsites, setAltWebsites, getRoleEmailTerms, setRoleEmailTerms, effectiveSitemap, titleCase, prettyRow, rowToCsvLine, csvHeader, bulkUpdate, getEmailModel, setEmailModelByDomain, makeClient: os.makeClient };
