/**
 * extract-from-pointers.js — Hop 2 of the universe-refresh routine: turn discovered BIO URLs (people/team
 * pages) into PERSON contacts in the OpenSearch `contacts` index (the Contact Crawler / Master DB).
 *
 *   OPENSEARCH_ENDPOINT=… node extract-from-pointers.js --ptr bio-ptr.jsonl [--live bio-miss-urls.txt] [--tag slice]
 *
 * --ptr   : WARC pointers for bio pages resolved in Common Crawl (from cc-athena-miner --resolve-urls);
 *           fetched S3-direct, no live traffic. Source = "Common Crawl".
 * --live  : plain URL list (one per line) of bio pages NOT found in CC — fetched live through the NetNut
 *           proxy chain (cc-engine.liveFetchPage). Source = "Live Crawl". Optional.
 *
 * Mirrors the proven lambda-extract → load-extracted recipe: extractRecord → modelMissingEmails →
 * analyzePhones → opensearch.recordToDoc → opensearch.bulkUpsert (score-gated, _id = email).
 */
const fs = require('fs');
const readline = require('readline');
const path = require('path');
const extractor = require('./extractor');
const os = require('./opensearch');
const { makeCcS3 } = require('./cc-s3');
const { modelMissingEmails } = require('./email-model');
const liName = require('./li-name');
const wbc = require('./wireless-block-classifier');
const vcard = require('./vcard');
let ccEngine = null; try { ccEngine = require('./cc-engine'); } catch (e) { /* live fallback optional */ }
// Phone-block table: extractRecord captures NO phone unless `wireless` is passed (the line-type gate),
// so load it up front — without it every extracted contact would have a blank phone.
let wireless = null;
try { wireless = wbc.loadWirelessBlocks(wbc.PHONE_BLOCKS_CSV); console.error('phone-blocks loaded (phone capture ON)'); }
catch (e) { console.error('WARN phone-blocks NOT loaded -> phones will be blank:', e.message); }

function argOf(flag) { const i = process.argv.indexOf(flag); return i > -1 ? process.argv[i + 1] : ''; }

// ---- quality gates: keep real people, reject page-title junk + generic firm emails ----
// A generic inbox (info@, contact@, careers@ …) on a person's bio page is the FIRM's address, not theirs —
// treat it as no-email so we model a personal one from the domain pattern instead.
const GENERIC_RE = /^(info|contact|admin|hello|enquir|office|sales|support|team|general|reception|careers?|jobs?|marketing|press|media|help|service|account|billing|privacy|legal|webmaster|postmaster|no-?reply|do-?not-?reply|newsletter|subscribe|feedback|hr|mail|e-?mail|ask|hi|hey|welcome|clients?|customerservice|studio|bookings?|appointments?)([._+-]|$|[0-9])/;
function isGenericEmail(email) { const l = String(email || '').toLowerCase().split('@')[0].trim(); return !l || GENERIC_RE.test(l); }
// Page titles that the extractor sometimes mistakes for names (from contact / service / news pages).
const JUNK_NAME = new Set(['contact', 'form', 'general', 'enquiry', 'enquiries', 'administration', 'team', 'about',
  'service', 'services', 'news', 'privacy', 'estate', 'probate', 'school', 'get', 'meet', 'our', 'home', 'blog',
  'careers', 'welcome', 'page', 'search', 'menu', 'login', 'sign', 'subscribe', 'newsletter', 'cookie', 'cookies',
  'terms', 'policy', 'the', 'and', 'staff', 'people', 'profile', 'profiles', 'directory', 'overview', 'summary', 'hire', 'employee',
  // job titles / role words the extractor sometimes captures as a fake first or last name
  'paralegal', 'attorney', 'attorneys', 'solicitor', 'solicitors', 'lawyer', 'lawyers', 'barrister', 'barristers',
  'partner', 'partners', 'associate', 'associates', 'counsel', 'adviser', 'advisor', 'advisors', 'consultant', 'consultants',
  'manager', 'director', 'directors', 'officer', 'assistant', 'executive', 'analyst', 'clerk', 'secretary', 'trainee',
  'principal', 'founder', 'chairman', 'president', 'what', 'who', 'why', 'how', 'more', 'view', 'read', 'all', 'list']);
const NAME_RE = /^[a-zà-ÿ][a-zà-ÿ'’.-]{1,23}$/i;   // one token: letters (+ '.- ), sane length
function looksLikePerson(first, last) {
  const f = String(first || '').trim(), l = String(last || '').trim();
  if (!f || !l) return false;
  if (!NAME_RE.test(f) || !NAME_RE.test(l)) return false;
  if (JUNK_NAME.has(f.toLowerCase()) || JUNK_NAME.has(l.toLowerCase())) return false;
  return true;
}
// URL-slug name fallback: many bio pages name the person only in the URL (m-spencer-cook, team/detail/fei-du,
// staffMembers/view/ector-galindo) — content extraction returns no name. Take the LAST path segment (then the
// second-to-last) and, to avoid false positives on product/service pages (collections/personal-massagers),
// only accept it when the URL sits under a bio directory OR the derived first name is a known first name.
const { nameFromPath } = require('./name-from-path');
const BIO_SEG = /^(teams?|staff|staffmembers|people|our-?team|attorneys?|lawyers?|solicitors?|agents?|advisors?|providers?|doctors?|physicians?|profiles?|bios?|members?|employees?|associates?|partners?|meet|about-?us|our-)/i;
function urlBioName(url, genderMap) {
  let segs; try { segs = new URL(url).pathname.replace(/\/+$/, '').split('/').filter(Boolean); } catch (e) { return null; }
  const bioDir = segs.some((s) => BIO_SEG.test(s));
  for (const seg of segs.slice(-2).reverse()) {
    const nm = nameFromPath(seg);
    if (looksLikePerson(nm.first, nm.last) && (bioDir || genderMap[nm.first.toLowerCase()])) return nm;
  }
  return null;
}

// Resilience: the https-proxy-agent throws an UNCAUGHT AssertionError (index.js:155) when a proxy
// CONNECT returns a malformed response (frequent on flaky/rotating gateways under load) — it escapes the
// per-request error handlers and would kill the whole multi-million-URL run. Swallow ONLY those (proxy-
// agent / socket asserts) and keep crawling; real bugs still crash. Paired with the hard-timeout race
// in pump() so the request that triggered it can't leave a worker hung.
let _swallowed = 0;
process.on('uncaughtException', (e) => {
  const s = (e && (e.stack || e.message)) || '';
  if (/https?-proxy-agent|ERR_ASSERTION|ECONNRESET|socket hang up|EPIPE/i.test(s)) {
    _swallowed++; if (_swallowed % 200 === 1) console.error(`[resilient] swallowed proxy/socket fault #${_swallowed}`); return;
  }
  console.error('FATAL uncaughtException:', s); process.exit(1);
});
process.on('unhandledRejection', () => {});   // a raced/abandoned fetch rejecting late must not crash

(async () => {
  const ptrFile = argOf('--ptr');
  const liveFile = argOf('--live');
  const tag = argOf('--tag') || 'bio';
  if (!ptrFile && !liveFile) { console.error('need --ptr <ptr.jsonl> and/or --live <urls.txt>'); process.exit(1); }

  const client = os.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const genderMap = extractor.loadGenderMap(path.join(__dirname, 'names-genders.csv'));
  const fetchWarc = makeCcS3();
  const now = new Date().toISOString();
  const CONC = Number(process.env.CONC) || 12;
  const LIVE_CONC = Number(process.env.LIVE_CONC) || 4;
  const CHUNK = Number(process.env.CHUNK) || 5000;
  const tsOf = (t) => String(t || '').slice(0, 8).replace(/(\d{4})(\d{2})(\d{2})/, '$1-$2-$3');

  const TRANSIENT = /429|rejected_execution|too_many_requests|timeout|ETIMEDOUT|ECONN|socket|Connection|hang up|EAI_AGAIN|502|503|throttl/i;
  async function withRetry(fn, tries = 5) {
    let last; for (let a = 0; a < tries; a++) { try { return await fn(); } catch (e) { last = e; if (!TRANSIENT.test(String((e && e.message) || e))) throw e; await new Promise((r) => setTimeout(r, 250 * Math.pow(2, a))); } }
    throw last;
  }

  // Learn each firm's Professional-email pattern from the central contacts index (cached per domain) so
  // named people with no personal email on the page get firstname.lastname@firm synthesized.
  const patternCache = new Map();
  async function dbQuery(domain) {
    if (patternCache.has(domain)) return patternCache.get(domain);
    let rows = [];
    try { const r = await os.search(client, { domain, emailType: 'Professional', pageSize: 200 }); rows = r.rows || []; } catch (e) { /* best-effort */ }
    patternCache.set(domain, rows);
    return rows;
  }

  // ---- accumulate extracted records; flush in chunks (filter -> model emails -> upsert) ----
  let pending = [];
  let submitted = 0, withEmail = 0, upErrs = 0, modelled = 0, dropJunk = 0, dropNoEmail = 0, vcardApplied = 0, liNamed = 0;
  async function flush() {
    if (!pending.length) return;
    const recs = pending; pending = [];
    // (a0) LinkedIn-slug name recovery FIRST: a bio page that yielded no name (or a name the map can't
    // gender) but did yield a linkedin.com/in URL becomes a real, gendered person here. Runs before the
    // person filter (so those records aren't dropped as junk) and before modelling (so the modelled
    // address is built from the corrected name instead of needing a re-model afterwards).
    try { const n = liName.applyToRecords(recs); liNamed += n.changed; } catch (e) { /* best-effort */ }
    // (a) keep only records that read as a real person (drops page-title "names" from contact/service pages)
    const people = recs.filter((r) => looksLikePerson(r['First'], r['Last']));
    dropJunk += recs.length - people.length;
    // (b) a generic firm inbox on a bio page isn't the person's address — clear it FIRST so the vCard
    // or the modeler can supply a real personal one into the now-empty field.
    for (const r of people) { if (isGenericEmail(r['Email Address'])) { r['Email Address'] = ''; r['Email Type'] = ''; } }
    // (c) vCard: download the .vcf linked on the bio page (through the SAME proxy) and merge the person's
    // DIRECT email + CELL phone (TEL;TYPE=CELL -> Mobile). Premium source; augments, never clobbers.
    if (ccEngine && typeof ccEngine.fetchDoc === 'function') {
      try { const v = await vcard.enrichRecords(people, { genderMap, _fetch: ccEngine.fetchDoc }); vcardApplied += (v && v.applied) || 0; }
      catch (e) { /* best-effort */ }
    }
    // (d) synthesize personal emails from the domain pattern for anyone STILL missing one
    try { modelled += await modelMissingEmails(people, { dbQuery }); } catch (e) { /* best-effort */ }
    let out = people; try { out = extractor.analyzePhones(people) || people; } catch (e) { /* best-effort */ }
    // (d) keep only real people with a real (personal or modelled) email
    const docs = out.map((r) => os.recordToDoc(r, now)).filter((d) => d && d.first && d.last && d.email && !isGenericEmail(d.email));
    dropNoEmail += out.length - docs.length;
    withEmail += docs.length;
    for (let i = 0; i < docs.length; i += 2000) {
      const batch = docs.slice(i, i + 2000);
      try { const res = await withRetry(() => os.bulkUpsert(client, batch)); const b = res && (res.body || res); if (b && b.items) for (const it of b.items) if (it.update && it.update.error) upErrs++; submitted += batch.length; }
      catch (e) { upErrs += batch.length; }
    }
  }

  // ---- generic worker pool over a job list; each job returns {html, url, source} ----
  async function pump(jobs, conc, run, label) {
    let i = 0, fetched = 0, extracted = 0, ferr = 0; const t0 = Date.now();
    const HARD_MS = (Number(process.env.LIVE_FETCH_TIMEOUT) || 6000) * 2 + 4000;  // hard ceiling: a hung fetch (flaky-proxy socket whose assert we swallow) can't stall a worker
    async function worker() {
      for (;;) {
        const k = i++; if (k >= jobs.length) return;
        try {
          const { html, url, source } = await Promise.race([
            run(jobs[k]),
            new Promise((_, rej) => setTimeout(() => rej(new Error('hard-timeout')), HARD_MS)),
          ]);
          if (!html) continue; fetched++;
          const rec = extractor.extractRecord(html, url, { wireless, genderMap, directoryRules: {}, source, timestamp: jobs[k].ts || '', allowNoEmail: true });
          if (rec && !looksLikePerson(rec['First'], rec['Last'])) { const nm = urlBioName(url, genderMap); if (nm) { rec['First'] = nm.first; rec['Last'] = nm.last; } }   // name from URL slug when the page has none
          if (rec) { extracted++; pending.push(rec); if (pending.length >= CHUNK) await flush(); }
        } catch (e) { ferr++; }
        if ((k + 1) % 500 === 0) console.error(`  [${label}] ${k + 1}/${jobs.length} | fetched ${fetched} | extracted ${extracted} | ${ferr} err | ${Math.round((k + 1) / ((Date.now() - t0) / 1000))}/s`);
      }
    }
    await Promise.all(Array.from({ length: conc }, worker));
    console.error(`  [${label}] done: ${fetched} fetched, ${extracted} extracted, ${ferr} fetch-err`);
    return { fetched, extracted, ferr };
  }

  const readLines = async (f, map) => { const out = []; const rl = readline.createInterface({ input: fs.createReadStream(f), crlfDelay: Infinity }); for await (const l of rl) { if (!l.trim()) continue; const v = map(l.trim()); if (v) out.push(v); } return out; };

  const t0 = Date.now();
  // CC pointers
  if (ptrFile) {
    const ptrs = await readLines(ptrFile, (l) => { let o; try { o = JSON.parse(l); } catch { return null; } return (o && o.url && o.filename) ? { ...o, ts: tsOf(o.timestamp) } : null; });
    console.error(`CC bio pointers: ${ptrs.length.toLocaleString()}`);
    await pump(ptrs, CONC, async (p) => ({ html: await withRetry(() => fetchWarc({ url: p.url, filename: p.filename, offset: p.offset, length: p.length })), url: p.url, source: 'Common Crawl' }), 'cc');
  }
  // Live fallback
  if (liveFile) {
    if (!ccEngine || typeof ccEngine.liveFetchPage !== 'function') {
      console.error('live fallback requested but cc-engine.liveFetchPage unavailable — skipping live');
    } else {
      const urls = await readLines(liveFile, (l) => (/^https?:\/\//i.test(l) ? { url: l } : { url: 'https://' + l }));
      console.error(`live bio URLs: ${urls.length.toLocaleString()}`);
      await pump(urls, LIVE_CONC, async (u) => ({ html: await ccEngine.liveFetchPage(u.url), url: u.url, source: 'Live Crawl' }), 'live');
    }
  }
  await flush();
  console.error(`DONE: ${submitted.toLocaleString()} real contacts upserted (${modelled.toLocaleString()} emails modelled, ${vcardApplied.toLocaleString()} vCards merged, ${liNamed.toLocaleString()} named from LinkedIn) | dropped ${dropJunk.toLocaleString()} non-person + ${dropNoEmail.toLocaleString()} no-usable-email | ${upErrs} upsert-err | ${Math.round((Date.now() - t0) / 1000)}s`);
})().catch((e) => { console.error('ERR', e.message); process.exit(1); });
