/**
 * company-name-waterfall.js — give every contact a real COMPANY NAME instead of a website.
 *
 *   OPENSEARCH_ENDPOINT=… node company-name-waterfall.js [--dry] [--limit N]
 *
 * The Contact Crawler's Company column reads `company_name`. 42% of contacts have none at all, and a
 * further slice carry a website where a name belongs, because the contact write path sets the internal
 * `company` field to the domain unconditionally (recordToDoc) and only the firmographic join ever fills
 * a real name.
 *
 * THE WATERFALL, per contact domain:
 *   1. company records on the EXACT domain, HQ-typed
 *   2. company records on the ROOT domain, HQ-typed        (news.acme.com -> acme.com)
 *   3. company records on the exact domain, any type
 *   4. company records on the root domain, any type
 * and within whichever tier answers first the name is CHOSEN, not taken at random.
 *
 * Why choosing matters: a shared brand domain holds hundreds of HQ records, one per franchise office
 * and per agent. remax.com has 303, coldwellbanker.com 1,982. Taking the largest `location_count`
 * returned "mariwood real estate" for remax.com and "Samia Akodu - Compass Real Estate Associate
 * Broker" for compass.com.
 *
 * So: a domain with ONE candidate takes it (that is how wvu.edu gets "west virginia university", a name
 * sharing no text with its label). A domain with MANY takes only a name that normalises exactly to the
 * domain label — "remax", "compass", "Coldwell Banker", "century 21®", "Exit Realty" — and otherwise is
 * LEFT ALONE. Looser matching was tried and picks confident strangers: on "starts with", memphis.edu
 * returns "Memphis Music School", because the label is a CITY and 26 businesses there begin with it.
 * kw.com holds 60 records, none of them Keller Williams. A blank Company is recoverable and a wrong
 * employer is not, so this trades recall for precision. Candidate names that are themselves domains are
 * discarded — swapping one website for another is not an improvement.
 */
const os = require('./opensearch');
const co = require('./companies');
const { rootDomain } = require('./enrich-firmographics');

const arg = (f, d) => { const i = process.argv.indexOf(f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const DRY = process.argv.includes('--dry');
const LIMIT = Number(arg('--limit', '0')) || 0;
const PAGE = 2000;
const CAND_CAP = 2000;        // deepest we page a single domain's company records

const norm = (s) => String(s || '').toLowerCase().replace(/[^a-z0-9]/g, '');
const labelOf = (d) => norm(String(rootDomain(d) || d).split('.')[0]);
const looksLikeDomain = (s) => {
  const t = String(s || '').trim();
  return !/\s/.test(t) && /^[a-z0-9][a-z0-9-]*(\.[a-z0-9-]+)+$/i.test(t);
};

function score(name, dom) {
  const n = norm(name), l = labelOf(dom);
  if (!n || !l) return 0;
  if (n === l) return 4;
  // A 2-3 character label ("kw", "ge") prefix-matches far too much: "kw" would claim "kw privilege".
  if (l.length >= 4 && (n.startsWith(l) || l.startsWith(n))) return 3;
  if (l.length >= 4 && n.includes(l)) return 2;
  return 1;
}

// One candidate is unambiguous, so take it: that is how wvu.edu resolves to "west virginia university",
// a name with no textual relation to its label.
//
// With MANY candidates only an exact label match is trusted. Anything looser picks a plausible stranger:
// scored on "starts with", memphis.edu returns "Memphis Music School" (the label is a CITY, and 26
// businesses on that domain begin with it) and exitrealty.com returns a single franchise office. A blank
// Company is recoverable; a confidently wrong employer is not, and nothing downstream can tell them
// apart. So precision over recall here.
function choose(cands, dom) {
  const usable = (cands || []).filter((x) => x && x.name && String(x.name).trim() && !looksLikeDomain(x.name));
  if (!usable.length) return null;
  if (usable.length === 1) return usable[0].name;
  const exact = usable
    .map((x) => ({ x, s: score(x.name, dom), lc: Number(x.location_count) || 0 }))
    .filter((r) => r.s === 4)
    .sort((a, b) => b.lc - a.lc || norm(a.x.name).length - norm(b.x.name).length);
  return exact.length ? exact[0].x.name : null;
}

// Every company record on one domain, paged. Domains are small (median 1); the cap guards the handful
// of brand domains holding thousands.
async function candidatesFor(cc, dom) {
  const out = [];
  let after = null;
  for (;;) {
    const body = {
      size: 500, query: { term: { domain: dom } },
      _source: ['name', 'company_type', 'location_count'], sort: [{ _doc: 'asc' }],
    };
    if (after) body.search_after = after;
    let hits;
    try { hits = ((await cc.search({ index: co.INDEX, body })).body || {}).hits.hits; }
    catch (e) { break; }
    if (!hits || !hits.length) break;
    for (const h of hits) out.push(h._source);
    if (out.length >= CAND_CAP) break;
    after = hits[hits.length - 1].sort;
  }
  return out;
}

const tiers = { hqExact: 0, hqRoot: 0, anyExact: 0, anyRoot: 0, none: 0 };

// Resolve a whole page of domains in one round of msearch rather than one search per domain. Doing it
// serially cost 291s per 20,000 contacts (~25h for the index); the work is entirely round-trip latency,
// not cluster time. A domain that comes back full is re-fetched properly by candidatesFor, which pages.
const MS_CHUNK = 120;
const MS_SIZE = 500;
async function prefetch(cc, keys) {
  const store = new Map();
  const want = [...new Set(keys.filter(Boolean))];
  for (let i = 0; i < want.length; i += MS_CHUNK) {
    const chunk = want.slice(i, i + MS_CHUNK);
    const body = [];
    for (const d of chunk) {
      body.push({ index: co.INDEX });
      body.push({ size: MS_SIZE, query: { term: { domain: d } }, _source: ['name', 'company_type', 'location_count'] });
    }
    let resp = null;
    for (let a = 0; a < 4; a++) {
      try { resp = ((await cc.msearch({ body })).body || {}).responses || []; break; }
      catch (e) { if (a === 3) { resp = []; break; } await new Promise((r) => setTimeout(r, 400 * 2 ** a)); }
    }
    for (let j = 0; j < chunk.length; j++) {
      const r = resp[j];
      const hits = (r && r.hits && r.hits.hits) || [];
      store.set(chunk[j], hits.map((h) => h._source));
      // hit the window: page it out so a brand domain's real record is not missed
      if (hits.length >= MS_SIZE) store.set(chunk[j], await candidatesFor(cc, chunk[j]));
    }
  }
  return store;
}

function resolveFrom(store, dom) {
  const root = rootDomain(dom);
  const exact = store.get(dom) || [];
  let p = choose(exact.filter((x) => String(x.company_type || '') === 'HQ'), dom);
  if (p) { tiers.hqExact++; return p; }
  const atRoot = (root && root !== dom) ? (store.get(root) || []) : null;
  if (atRoot) {
    p = choose(atRoot.filter((x) => String(x.company_type || '') === 'HQ'), dom);
    if (p) { tiers.hqRoot++; return p; }
  }
  p = choose(exact, dom);
  if (p) { tiers.anyExact++; return p; }
  if (atRoot) {
    p = choose(atRoot, dom);
    if (p) { tiers.anyRoot++; return p; }
  }
  tiers.none++;
  return '';
}

module.exports = { score, choose, resolveFrom, prefetch, looksLikeDomain, labelOf };

if (require.main === module) (async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  const c = os.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const cc = co.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const t0 = Date.now();

  // Default pass: contacts whose Company column is empty (42% of the index).
  // --domainish: the smaller, separate case the blank pass does not touch -- a Company that IS a
  // website ("realting.com"), which reads as a name but is not one. Anchored regexp on the keyword
  // subfield, so "Morris B. Silver, MD" (spaces, comma) does not match.
  const DOMAINISH = process.argv.includes('--domainish');
  const query = DOMAINISH
    ? {
      bool: {
        must: [
          { exists: { field: 'domain' } },
          { regexp: { 'company_name.keyword': { value: '[a-zA-Z0-9][a-zA-Z0-9-]*(\\.[a-zA-Z0-9-]+)+', flags: 'ALL' } } },
        ],
        must_not: [{ term: { domain: '' } }],
      },
    }
    : {
      bool: {
        must: [{ exists: { field: 'domain' } }],
        must_not: [{ term: { domain: '' } }],
        should: [
          { bool: { must_not: [{ exists: { field: 'company_name' } }] } },
          { term: { company_name: '' } },
        ],
        minimum_should_match: 1,
      },
    };

  const cache = new Map();
  let after = null, scanned = 0, resolved = 0, updated = 0, cacheHits = 0;
  const samples = [];
  for (;;) {
    const body = { size: PAGE, query, _source: ['domain'], sort: [{ email: 'asc' }] };
    if (after) body.search_after = after;
    const hits = ((await c.search({ index: os.INDEX, body })).body || {}).hits.hits;
    if (!hits || !hits.length) break;
    // one batched lookup for every domain on this page that we have not already resolved
    const pageDoms = [...new Set(hits.map((h) => String(h._source.domain || '').toLowerCase()).filter(Boolean))]
      .filter((d) => !cache.has(d));
    const need = [];
    for (const d of pageDoms) { need.push(d); const r = rootDomain(d); if (r && r !== d) need.push(r); }
    const store = need.length ? await prefetch(cc, need) : new Map();
    for (const d of pageDoms) cache.set(d, resolveFrom(store, d));

    const actions = [];
    for (const h of hits) {
      scanned++;
      const dom = String(h._source.domain || '').toLowerCase();
      if (!dom) continue;
      const name = cache.get(dom) || '';
      if (cache.has(dom) && !pageDoms.includes(dom)) cacheHits++;
      if (!name) continue;
      resolved++;
      if (samples.length < 10) samples.push(dom.padEnd(32) + ' -> ' + name);
      if (!DRY) {
        actions.push({ update: { _index: os.INDEX, _id: h._id } }, { doc: { company_name: name, company: name } });
        updated++;
      }
    }
    if (actions.length) {
      try { await c.bulk({ body: actions, refresh: false }); }
      catch (e) { console.error('  bulk failed:', e.message); }
    }
    after = hits[hits.length - 1].sort;
    if (scanned % 50000 < PAGE) {
      const s = Math.max(1, (Date.now() - t0) / 1000);
      console.error(`  ${scanned.toLocaleString()} scanned | ${resolved.toLocaleString()} resolved | ${cache.size.toLocaleString()} domains cached | ${Math.round(scanned / s)}/s`);
    }
    if (LIMIT && scanned >= LIMIT) break;
  }
  if (!DRY) { try { await c.indices.refresh({ index: os.INDEX }); } catch (e) { /* */ } }

  console.error(`\n====== ${DRY ? 'DRY RUN' : 'DONE'} · ${Math.round((Date.now() - t0) / 1000)}s ======`);
  console.error(`  contacts scanned : ${scanned.toLocaleString()}`);
  console.error(`  name resolved    : ${resolved.toLocaleString()} (${(100 * resolved / Math.max(1, scanned)).toFixed(1)}%)`);
  console.error(`  ${DRY ? 'would update' : 'updated'}     : ${(DRY ? resolved : updated).toLocaleString()}`);
  console.error(`  distinct domains : ${cache.size.toLocaleString()} (${cacheHits.toLocaleString()} cache hits)`);
  console.error(`  tiers: HQ/exact ${tiers.hqExact.toLocaleString()} | HQ/root ${tiers.hqRoot.toLocaleString()} | any/exact ${tiers.anyExact.toLocaleString()} | any/root ${tiers.anyRoot.toLocaleString()} | unresolved ${tiers.none.toLocaleString()}`);
  console.error('  samples:');
  for (const s of samples) console.error('    ' + s);
})().catch((e) => { console.error('ERR', (e && e.stack) || e); process.exit(1); });
