/**
 * hq-resolve-dryrun.js — report what HQ resolution WOULD do. Writes nothing to the index.
 * ---------------------------------------------------------------------------------------------
 * The rules being tested, as agreed:
 *   1. One HQ per REGISTRABLE root domain (locations.kw.com and kw.com are one group).
 *   2. A PDL record for that root is the HQ. PDL records are the ones carrying `size`.
 *   3. No PDL record -> promote the most complete remaining record.
 *   4. Everything else on the root becomes a Location and counts into location_count.
 *   5. A record on a social/platform host is its OWN record: never a parent, never merged.
 *      Collapsing those would turn 149,890 Instagram-only businesses into one company.
 *
 * Two stages, because the index holds ~30.7M records over ~20M distinct domains:
 *
 *   node hq-resolve-dryrun.js --dump <file>    # stage A: scan the index, one line per domain
 *   node hq-resolve-dryrun.js --fold <file>    # stage B: fold domains into roots, print the report
 *   node hq-resolve-dryrun.js --dump f --fold f   # both
 *
 * Stage A writes a cursor beside the dump after every page, so an interrupted scan resumes instead of
 * costing another hour. Stage B is re-runnable for free once the dump exists.
 *
 * Stage B does NOT keep a Map of roots. The first version did, and died at 16,717,807 roots with
 * "RangeError: Map maximum size exceeded" 44 minutes into the scan — V8 caps a Map at ~2^24 entries.
 * It now folds into open-addressed typed arrays keyed by a 64-bit hash of the root, which stores no
 * strings at all: ~570MB of flat memory for 33.5M slots, and no per-entry object overhead. Two
 * independent 32-bit hashes give a 64-bit key, so the chance of any collision across 17M roots is
 * about 8e-6 — and a collision would merge two roots, not corrupt the run.
 */
const co = require('./companies');
const fs = require('fs');
const path = require('path');
const readline = require('readline');

const arg = (f, d) => { const i = process.argv.indexOf('--' + f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const DUMP = arg('dump', '');
const FOLD = arg('fold', '');
const PAGE = Number(arg('page', '')) || 10000;
const MAX_DOMAINS = Number(arg('max-domains', '')) || 0;
const EMIT_CANDIDATES = arg('emit-candidates', '');       // write the "needs a decision" hosts to this file
const CARDINALITY = arg('cardinality', '');               // read hq-name-cardinality.js output back in
const N = (x) => Number(x || 0).toLocaleString();

// --- registrable root domain, via the Public Suffix List (see psl-suffix.js for why not the CSV) --
const psl = require('./psl-suffix');
const registrable = (host) => psl.registrable(host);

// Hosts that are somebody else's platform, not a company's own site.
const PLATFORM = new Set(['instagram.com', 'facebook.com', 'fb.com', 'linkedin.com', 'twitter.com', 'x.com',
  'youtube.com', 'linktr.ee', 'yelp.com', 'sites.google.com', 'etsy.com', 'amazon.com', 'wixsite.com', 'wix.com',
  'about.me', 'crunchbase.com', 'indiamart.com', 'behance.net', 'open.spotify.com', 'wa.me', 'tiktok.com',
  'blogger.com', 'draft.blogger.com', 'wordpress.com', 'blogspot.com', 'shopify.com', 'square.site', 'godaddysites.com']);

// ===== stage A: index -> one line per domain ====================================================
async function dump(file) {
  const c = co.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const cursorFile = file + '.cursor';
  let after = null, domains = 0;
  if (fs.existsSync(cursorFile) && fs.existsSync(file)) {
    try {
      const st = JSON.parse(fs.readFileSync(cursorFile, 'utf8'));
      after = st.after; domains = st.domains || 0;
      console.error('resuming after ' + N(domains) + ' domains');
    } catch { after = null; }
  }
  const out = fs.createWriteStream(file, { flags: after ? 'a' : 'w' });
  const write = (s) => (out.write(s) ? Promise.resolve() : new Promise((r) => out.once('drain', r)));

  const t0 = Date.now();
  for (;;) {
    const body = {
      size: 0,
      aggs: { d: {
        composite: { size: PAGE, sources: [{ dom: { terms: { field: 'domain' } } }], ...(after ? { after } : {}) },
        aggs: {
          // PDL records are the ones carrying `size`; `exists` alone also matches an empty string on a
          // keyword field, so the empty term has to be excluded explicitly.
          pdl: { filter: { bool: { must: [{ exists: { field: 'size' } }], must_not: [{ term: { size: '' } }] } } },
          hq: { filter: { term: { 'company_type.keyword': 'HQ' } } },
        },
      } },
    };
    // A partial page must never be written — one missing shard silently under-reports every root on
    // it. But a shed search is usually transient contention (a concurrent update_by_query was enough
    // to kill a 4M-domain run), so retry the SAME page rather than abandoning the scan. The page is
    // re-fetched from the same `after` key, so a retry repeats no work and skips none.
    let r = null;
    for (let attempt = 1; ; attempt++) {
      const res = await c.search({ index: co.INDEX, body }, { requestTimeout: 300000 })
        .then((x) => (x.body || x))
        .catch((e) => ({ _error: e }));
      const failed = res._error ? -1 : ((res._shards && res._shards.failed) || 0);
      if (!failed) { r = res; break; }
      const why = res._error ? (res._error.message || String(res._error)).slice(0, 140) : failed + ' shard(s) failed';
      if (attempt >= 8) throw new Error('giving up after ' + attempt + ' attempts on the same page (' + why + ') — cursor kept, re-run to resume');
      const wait = Math.min(60000, 3000 * 2 ** (attempt - 1));
      console.error('  retry ' + attempt + '/8 at ' + N(domains) + ' domains in ' + Math.round(wait / 1000) + 's: ' + why);
      await new Promise((s) => setTimeout(s, wait));
    }
    const agg = r.aggregations.d, buckets = agg.buckets || [];
    if (!buckets.length) break;

    let chunk = '';
    for (const b of buckets) {
      const dom = String(b.key.dom == null ? '' : b.key.dom);
      if (!dom) continue;
      domains++;
      const root = registrable(dom);
      // The FULL domain goes in the dump next to its root, so stage B can re-fold under a different
      // rule without another hour-long scan. Keeping only the root is what made the first version a
      // one-shot: there was no way to reconsider a grouping after seeing what it produced.
      chunk += dom + '\t' + root + '\t' + b.doc_count + '\t' + b.pdl.doc_count + '\t' + b.hq.doc_count +
               '\t' + (PLATFORM.has(root) ? '1' : '0') + '\n';
    }
    await write(chunk);
    after = agg.after_key;
    if (after) fs.writeFileSync(cursorFile, JSON.stringify({ after, domains }));
    if (domains % 1000000 < PAGE) console.error('  ' + N(domains) + ' domains  (' + Math.round((Date.now() - t0) / 1000) + 's)');
    if (!after) break;
    if (MAX_DOMAINS && domains >= MAX_DOMAINS) { console.error('  --max-domains reached'); break; }
  }
  await new Promise((r) => out.end(r));
  if (!after) { try { fs.unlinkSync(cursorFile); } catch {} }
  console.error('dumped ' + N(domains) + ' domains -> ' + file + '  (' + Math.round((Date.now() - t0) / 1000) + 's)');
}

// ===== stage B: fold domains into roots =========================================================
const CAP = 1 << 25;                 // 33,554,432 slots for ~17M roots: load factor 0.5
const MASK = CAP - 1;
const h1 = (s) => { let h = 0x811c9dc5; for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 0x01000193); } return (h >>> 0) || 1; };
const h2 = (s) => { let h = 0xdeadbeef; for (let i = 0; i < s.length; i++) { h = Math.imul(h ^ s.charCodeAt(i), 0x85ebca6b); h = (h << 13) | (h >>> 19); } return (h >>> 0) || 1; };

function makeTable(extra = false) {
  const t = { k1: new Uint32Array(CAP), k2: new Uint32Array(CAP), rec: new Uint32Array(CAP),
              hq: new Uint32Array(CAP), pdl: new Uint8Array(CAP), n: 0 };
  if (extra) {                       // only the first pass needs the multi-tenant detectors
    t.doms = new Uint32Array(CAP);    // distinct domains seen under this root
    t.barePdl = new Uint8Array(CAP);  // the root itself (not a subdomain) has a PDL record
  }
  return t;
}
// Returns the slot for `root`, creating it if `create`. -1 when absent and not creating.
function slot(t, root, create) {
  const a = h1(root), b = h2(root);
  let i = a & MASK;
  for (let probe = 0; probe < CAP; probe++) {
    if (t.k1[i] === 0) {
      if (!create) return -1;
      t.k1[i] = a; t.k2[i] = b; t.n++;
      return i;
    }
    if (t.k1[i] === a && t.k2[i] === b) return i;
    i = (i + 1) & MASK;
  }
  throw new Error('hash table full');
}

// A root is MULTI-TENANT when many unrelated businesses sit on subdomains of one host — business.site,
// myshopify.com, ueniweb.com, netlify.app, net.cn, clayton.k12.ga.us. The TLD table cannot tell those
// from a company with branches: folding by root alone turned 1,105 separate businesses on business.site
// into one company with 1,104 "locations". Detecting it from the data beats maintaining a list, because
// the list is never finished. Two conditions, because either alone is wrong: MANY distinct subdomains
// (a company rarely runs 25+), and NO PDL record on the bare root (harvard.edu has one and keeps its
// departments; business.site does not).
const TENANT_MIN = Number(arg('tenant-min', '')) || 25;
// ...but the "no PDL on the bare root" half of that test exempts exactly the platforms that are
// themselves real companies. glossgenius.com is in PDL, so its 13,510 subdomain records — 12,916
// distinct business names, no name holding even 1% — were about to merge into one company. Same for
// alibaba.com (10,099 records / 10,092 names) and taobao.com (2,838 / 2,838).
// Above TENANT_HARD distinct subdomains the PDL record stops meaning anything: no single business
// runs hundreds of subdomains each holding one OTHER business's record. harvard.edu, the case the
// barePdl guard exists for, has its departments in the tens, nowhere near this.
// Measured contrast on the live index, distinct names per record:
//   franchise branches on ONE bare domain  mcdonalds.com 0.006, cvs.com 0.006, chick-fil-a.com 0.004
//   tenants on MANY subdomains             business.site 1.000, alibaba.com 0.999, glossgenius 0.956
const TENANT_HARD = Number(arg('tenant-hard', '')) || 300;
const isTenantHost = (t, i) => t.doms[i] >= TENANT_MIN && (t.barePdl[i] === 0 || t.doms[i] >= TENANT_HARD);

async function fold(file) {
  const t0 = Date.now();
  // Hosts the name-cardinality pass judged to be aggregators rather than one brand: each record on
  // them stays its own company, exactly like a platform host. See hq-name-cardinality.js.
  const CARD = CARDINALITY && fs.existsSync(CARDINALITY) ? JSON.parse(fs.readFileSync(CARDINALITY, 'utf8')) : null;
  const cardSplit = new Set();     // strangers sharing a website: each record is its own company
  const cardReview = new Set();    // ambiguous: NEITHER merged nor split, left exactly as it is
  if (CARD) {
    for (const [dom, v] of Object.entries(CARD)) {
      if (!v) continue;
      if (v.decision === 'split') cardSplit.add(dom);
      else if (v.decision === 'review') cardReview.add(dom);
    }
    console.error('  verdict file: ' + Object.keys(CARD).length + ' host(s), ' + cardSplit.size +
                  ' aggregator, ' + cardReview.size + ' awaiting a human call (left alone)');
  }
  const stream = () => readline.createInterface({ input: fs.createReadStream(file, { highWaterMark: 1 << 20 }), crlfDelay: Infinity });
  // dump columns: domain, root, records, pdl, hq, isPlatform
  // The root is RECOMPUTED here from the domain rather than read out of column 2. That keeps the dump
  // independent of the grouping rule: the hour-long scan is paid once, and the rule can then be
  // revised and re-folded in minutes. Column 2 is kept in the file only for eyeballing.
  const parse = (line) => {
    const p = line.split('\t');
    if (p.length < 6) return null;
    const root = registrable(p[0]);
    return { dom: p[0], root, rec: Number(p[2]) || 0, pdl: Number(p[3]) || 0, hq: Number(p[4]) || 0,
             plat: PLATFORM.has(root), bad: !psl.knownSuffix(p[0]),
             // An aggregator with everything on ONE domain cannot be split BY domain — every record
             // carries the same one. Splitting it means not grouping it at all: each record stays its
             // own company, which is the platform treatment, not a re-keying.
             aggr: cardSplit.has(root) || cardSplit.has(p[0]),
             review: cardReview.has(root) || cardReview.has(p[0]) };
  };
  // Records that take part in no grouping at all: not a domain, a bare public suffix, a platform host,
  // an aggregator, or a host held back for review. All three passes must agree on this or the totals
  // drift between them. `review` is here for the conservative reason, not a technical one: leaving a
  // host ungrouped preserves exactly what it has today, which is the only reversible answer available.
  const ungrouped = (r) => r.bad || !r.root || r.plat || r.aggr || r.review;

  // --- pass 1: per-root stats, including what makes a host look multi-tenant --------------------
  const A = makeTable(true);
  let lines = 0, platformRecs = 0, platformDomains = 0, badRootRecs = 0, badRootDomains = 0;
  let sufOnlyRecs = 0, sufOnlyDomains = 0, aggrDomains = 0, aggrRecs = 0, revDomains = 0, revRecs = 0;
  const badEx = [], sufEx = [], aggrEx = [], revEx = [];
  for await (const line of stream()) {
    const r = parse(line);
    if (!r) continue;
    lines++;
    // not a domain at all: the TLD is in no PSL rule ("164.53", "0.5"). Junk that survived the purge.
    if (r.bad) {
      badRootDomains++; badRootRecs += r.rec;
      if (badEx.length < 10) badEx.push({ root: r.dom, rec: r.rec });
      continue;
    }
    // the value IS a bare public suffix ("sh.cn", "co.uk"): a real host but nobody's company, so it
    // can neither be an HQ nor be folded into one.
    if (!r.root) {
      sufOnlyDomains++; sufOnlyRecs += r.rec;
      if (sufEx.length < 10) sufEx.push({ root: r.dom, rec: r.rec });
      continue;
    }
    if (r.plat) { platformDomains++; platformRecs += r.rec; continue; }
    if (r.aggr) { aggrDomains++; aggrRecs += r.rec; if (aggrEx.length < 10) aggrEx.push({ root: r.dom, rec: r.rec }); continue; }
    if (r.review) { revDomains++; revRecs += r.rec; if (revEx.length < 10) revEx.push({ root: r.dom, rec: r.rec }); continue; }
    const i = slot(A, r.root, true);
    A.rec[i] += r.rec; A.hq[i] += r.hq; A.doms[i]++;
    if (r.pdl > 0) A.pdl[i] = 1;
    if (r.dom === r.root && r.pdl > 0) A.barePdl[i] = 1;
    if (lines % 4000000 === 0) console.error('  pass 1: ' + N(lines) + ' domains -> ' + N(A.n) + ' roots  (' + Math.round((Date.now() - t0) / 1000) + 's)');
  }

  // --- pass 2: re-fold, splitting the multi-tenant hosts back into one group per tenant ---------
  const B = makeTable();
  let tenantHosts = 0, tenantRecs = 0, tenantGroups = 0;
  const tenantEx = [];
  const counted = new Uint8Array(CAP);
  for await (const line of stream()) {
    const r = parse(line);
    if (!r || ungrouped(r)) continue;
    const a = slot(A, r.root, false);
    const split = a >= 0 && isTenantHost(A, a);
    if (split && !counted[a]) {
      counted[a] = 1; tenantHosts++; tenantRecs += A.rec[a];
      if (tenantEx.length < 10) tenantEx.push({ root: r.root, doms: A.doms[a], rec: A.rec[a], hq: A.hq[a] });
    }
    const key = split ? r.dom : r.root;
    const i = slot(B, key, true);
    B.rec[i] += r.rec; B.hq[i] += r.hq;
    if (r.pdl > 0) B.pdl[i] = 1;
  }
  tenantGroups = B.n;   // provisional; recomputed below as the real group count

  // --- classify over the final grouping --------------------------------------------------------
  let roots = 0, rootsPdl = 0, rootsPromote = 0, rootsSingle = 0, rootsMulti = 0;
  let records = 0, keepHQ = 0, demote = 0, promote = 0, dupeHQ = 0, rootsNoHQ = 0;
  for (let i = 0; i < CAP; i++) {
    if (B.k1[i] === 0) continue;
    const rec = B.rec[i], hq = B.hq[i], hasPdl = B.pdl[i];
    roots++; records += rec;
    if (rec === 1) rootsSingle++; else rootsMulti++;
    if (hasPdl) rootsPdl++; else rootsPromote++;
    keepHQ++;                            // exactly one HQ survives per group
    demote += rec - 1;                   // everything else in the group becomes a Location
    if (!hasPdl) promote++;
    if (hq > 1) dupeHQ += hq - 1;
    if (hq === 0) rootsNoHQ++;
  }
  tenantGroups = roots;

  // --- worked examples: one more streaming pass, first time each group is seen ------------------
  const seen = new Uint8Array(CAP);
  const ex = { pdlHQ: [], promoted: [], bigMulti: [], dupes: [] };
  // Marketplace/directory candidates: MANY records all on the one bare domain, no subdomains to tell
  // them apart (1688.com, 11467.com, netsolhost.com). Subdomain splitting cannot touch these, so
  // one-HQ-per-root WOULD merge them — but unlike business.site there is nothing in the data to
  // separate them by, so this is a judgement call, not something to decide automatically.
  const SHARED_MIN = Number(arg('shared-min', '')) || 20;
  const shared = [];
  for await (const line of stream()) {
    const r = parse(line);
    if (!r || ungrouped(r)) continue;
    const a = slot(A, r.root, false);
    const key = (a >= 0 && isTenantHost(A, a)) ? r.dom : r.root;
    const i = slot(B, key, false);
    if (i < 0 || seen[i]) continue;
    seen[i] = 1;
    const row = { root: key, rec: B.rec[i], hq: B.hq[i], pdl: B.pdl[i] };
    if (a >= 0 && A.doms[a] === 1 && row.rec >= SHARED_MIN) shared.push(row);
    if (row.pdl && row.rec > 1 && ex.pdlHQ.length < 8) ex.pdlHQ.push(row);
    if (!row.pdl && row.rec > 1 && ex.promoted.length < 8) ex.promoted.push(row);
    if (row.rec >= 50 && ex.bigMulti.length < 8) ex.bigMulti.push(row);
    if (row.hq > 1 && ex.dupes.length < 8) ex.dupes.push(row);
  }

  const pc = (x) => (roots ? (x / roots * 100).toFixed(1) + '%' : '-');
  console.log('\n================ HQ RESOLUTION — DRY RUN ================');
  console.log('domains read from dump          : ' + N(lines));
  console.log('company GROUPS after grouping   : ' + N(roots) + '   (= HQ records that would exist)');
  console.log('records on those roots          : ' + N(records));
  console.log('  single-record roots           : ' + N(rootsSingle) + '   (' + pc(rootsSingle) + ')');
  console.log('  multi-record roots            : ' + N(rootsMulti) + '   (' + pc(rootsMulti) + ')');
  console.log('');
  console.log('HQ source');
  console.log('  roots where PDL supplies HQ   : ' + N(rootsPdl) + '   (' + pc(rootsPdl) + ')');
  console.log('  roots needing a PROMOTION     : ' + N(rootsPromote) + '   (' + pc(rootsPromote) + ')');
  console.log('  roots with NO HQ record today : ' + N(rootsNoHQ) + '   (' + pc(rootsNoHQ) + ')');
  console.log('');
  console.log('what would change');
  console.log('  HQ records after resolution   : ' + N(keepHQ) + '   (one per root, by construction)');
  console.log('  records DEMOTED to Location   : ' + N(demote));
  console.log('  duplicate HQs collapsed       : ' + N(dupeHQ));
  console.log('  records promoted to HQ        : ' + N(promote));
  console.log('');
  console.log('left alone (own record, never merged)');
  console.log('  platform/social domains       : ' + N(platformDomains));
  console.log('  records on them               : ' + N(platformRecs));
  console.log('');
  if (CARD) {
    console.log('aggregator hosts (strangers sharing one website)');
    console.log('  domains / records             : ' + N(aggrDomains) + ' / ' + N(aggrRecs) + '   each record kept as its own company');
    for (const x of aggrEx) console.log('    ' + x.root.padEnd(34) + N(x.rec).padStart(8) + ' records');
    console.log('');
    console.log('HELD FOR REVIEW — neither merged nor split, left exactly as they are today');
    console.log('  domains / records             : ' + N(revDomains) + ' / ' + N(revRecs));
    console.log('  (high name variety, brand on few records: a franchisor with sub-brands and a');
    console.log('   directory of other businesses look the same from here)');
    for (const x of revEx) console.log('    ' + x.root.padEnd(34) + N(x.rec).padStart(8) + ' records');
    console.log('');
  }
  console.log('multi-tenant hosts split back apart (>=' + TENANT_MIN + ' subdomains and no PDL on the root, or >=' + TENANT_HARD + ' subdomains regardless)');
  console.log('  hosts detected                : ' + N(tenantHosts));
  console.log('  records that would have been  : ' + N(tenantRecs) + ' merged into ' + N(tenantHosts) + ' companies');
  for (const x of tenantEx) console.log('    ' + x.root.padEnd(30) + N(x.doms).padStart(7) + ' subdomains, ' + N(x.rec).padStart(7) + ' records');
  console.log('');
  console.log('not a domain at all — TLD in no PSL rule (junk that survived the domain purge)');
  console.log('  domains / records             : ' + N(badRootDomains) + ' / ' + N(badRootRecs));
  if (badEx.length) console.log('    ' + badEx.map((x) => x.root + ' (' + N(x.rec) + ')').join(', '));
  console.log('');
  console.log('domain IS a bare public suffix — a real host but nobody\'s company');
  console.log('  domains / records             : ' + N(sufOnlyDomains) + ' / ' + N(sufOnlyRecs));
  if (sufEx.length) console.log('    ' + sufEx.map((x) => x.root + ' (' + N(x.rec) + ')').join(', '));
  console.log('');
  shared.sort((x, y) => y.rec - x.rec);
  // Hand the candidate list to hq-name-cardinality.js, which asks the index how many DISTINCT company
  // names each of these hosts carries. That is what separates a franchise network from a marketplace:
  //   mcdonalds.com   9,285 records / 60 names,    "McDonald's" on 98%  -> one brand, merge
  //   br.bluepillow.com 14,113 records / 13,731 names, top name 0%      -> an aggregator, do not merge
  if (EMIT_CANDIDATES) {
    fs.writeFileSync(EMIT_CANDIDATES, JSON.stringify(shared.map((x) => ({ domain: x.root, rec: x.rec, hq: x.hq, pdl: x.pdl })), null, 1));
    console.log('wrote ' + N(shared.length) + ' candidate host(s) -> ' + EMIT_CANDIDATES);
  }
  console.log('NEEDS A DECISION — ' + N(shared.length) + ' host(s) carry ' + N(shared.reduce((s, x) => s + x.rec, 0)) +
              ' records all on the ONE bare domain (>=' + SHARED_MIN + ' each, no subdomains).');
  console.log('  Merging them gives one company with hundreds of "locations"; not merging them leaves');
  console.log('  hundreds of companies sharing a website. Nothing in the data separates them.');
  for (const x of shared.slice(0, 15)) console.log('    ' + x.root.padEnd(34) + N(x.rec).padStart(7) + ' records, ' + N(x.hq).padStart(6) + ' HQ' + (x.pdl ? ', has PDL' : ''));
  console.log('');
  const show = (label, arr) => {
    console.log(label);
    if (!arr.length) { console.log('    (none)'); return; }
    for (const r of arr) console.log('    ' + r.root.padEnd(34) + N(r.rec).padStart(8) + ' records, ' + N(r.hq).padStart(6) + ' HQ');
  };
  show('examples — PDL supplies the HQ', ex.pdlHQ);
  show('examples — no PDL, a record must be promoted', ex.promoted);
  show('examples — 50+ records on one root', ex.bigMulti);
  show('examples — more than one HQ today', ex.dupes);
  console.log('=========================================================');
  console.log('\nNothing was written.');
}

(async () => {
  if (!DUMP && !FOLD) { console.error('need --dump <file> and/or --fold <file>'); process.exit(1); }
  if (DUMP) {
    if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
    await dump(DUMP);
  }
  if (FOLD) await fold(FOLD);
  process.exit(0);
})().catch((e) => { console.error('ERR', (e && e.stack) || e); process.exit(1); });
