/**
 * gm-build.js — Phases 2/3/5 of the Google-Maps -> Company Crawler ETL. Reads the Phase-1
 * gm-locations.ndjson, groups Locations by root domain, resolves the HQ (existing company or a
 * synthesized one), merges same-City/Region/Country Locations into the HQ, rolls up bio/LinkedIn/email
 * to the HQ (stripped from the children), counts Locations, and builds contacts. Emits:
 *   gm-hq.ndjson       {id, isNew, doc}        -> HQ upserts (update existing / create synthesized)
 *   gm-loc.ndjson      {id, doc}               -> child Location upserts
 *   gm-contacts.ndjson <extractor display rec> -> person contacts for the Master DB
 *
 *   OPENSEARCH_ENDPOINT=… node gm-build.js --in <dir>/gm-locations.ndjson --out <dir> [--limit-domains N]
 */
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const { spawnSync } = require('child_process');
const co = require('./companies');
const ex = require('./extractor');
const che = require('./cc-home-enrich');

const arg = (f, d) => { const i = process.argv.indexOf(f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const IN = arg('--in', ''); const OUT = arg('--out', path.dirname(IN || '.'));
const LIMIT_DOMAINS = Number(arg('--limit-domains', '0')) || 0;
if (!IN || !process.env.OPENSEARCH_ENDPOINT) { console.error('need --in <gm-locations.ndjson> + OPENSEARCH_ENDPOINT'); process.exit(1); }

let client = co.makeClient(process.env.OPENSEARCH_ENDPOINT);
let lastResolveErr = null;   // the most recent msearch failure, so a give-up can say WHY
const genderMap = ex.loadGenderMap(path.join(__dirname, 'names-genders.csv'));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---- address -> {city,region,country} (US-biased heuristic) ----
const COUNTRIES = new Set(['united states', 'usa', 'us', 'united kingdom', 'uk', 'canada', 'australia', 'ireland', 'new zealand', 'india', 'germany']);
function parseAddr(addr) {
  const parts = String(addr || '').split(',').map((s) => s.trim()).filter(Boolean);
  let country = '', region = '', city = '';
  if (parts.length && COUNTRIES.has(parts[parts.length - 1].toLowerCase())) country = parts.pop();
  const stzip = parts[parts.length - 1] || '';
  const m = stzip.match(/^([A-Za-z]{2})\s+\w/); if (m) { region = m[1]; if (!country) country = 'United States'; }
  city = parts[parts.length - 2] || '';
  return { city: city.toLowerCase(), region: region.toLowerCase(), country: (country || '').toLowerCase() };
}
// prefer a record's componentized city/region/country (Bing UK) over parsing the free-text address (Google US)
const crcOf = (r) => (r && (r.region || r.locality)) ? { city: (r.locality || '').toLowerCase(), region: (r.region || '').toLowerCase(), country: (r.country || '').toLowerCase() } : parseAddr(r ? r.full_address : '');
// Reads BOTH shapes. hq0 comes straight out of OpenSearch, where bio_url / linkedin_contact /
// linkedin_url are arrays now, and String(['a','b']) is 'a,b' -- splitting that on [;\s]+ returned
// ONE fused entry and silently merged two URLs into a string that is neither.
const splitL = (s) => co.asList(s);
const LI_IN = /linkedin\.com\/in\//i;                                 // personal profile  -> linkedin_contact
const LI_CO = /linkedin\.com\/(company|school|showcase)\//i;          // company page      -> linkedin_url
const score = (r) => (r.email ? 2 : 0) + (r.website ? 1 : 0) + (r.phone ? 1 : 0) + (r.bio_url ? 2 : 0) + (r.name ? 1 : 0) + (r.full_address ? 1 : 0);

const HQ_SRC = ['id', 'name', 'website', 'domain', 'locality', 'region', 'country', 'phone', 'full_address', 'linkedin_url', 'email', 'facebook', 'instagram', 'bio_url', 'linkedin_contact'];
// Resolve one CHUNK of domains via msearch (one HTTP round-trip). Retries+reconnects on a link blip;
// returns false if the chunk still fails after retries so the caller can DEFER it (never treat a
// connection error as "no match" — that would false-synthesize an HQ).
async function resolveChunk(chunk, map) {
  const body = [];
  for (const d of chunk) { body.push({ index: co.INDEX }); body.push({ size: 1, query: { term: { domain: d } }, _source: HQ_SRC }); }
  for (let a = 0; a < 6; a++) {
    try {
      const r = await client.msearch({ body });
      const resps = (r.body || r).responses || [];
      for (let j = 0; j < chunk.length; j++) {
        const h = resps[j] && resps[j].hits && resps[j].hits.hits && resps[j].hits.hits[0];
        if (h) map.set(chunk[j], { ...h._source, _id: h._id });
      }
      return true;
    } catch (e) {
      lastResolveErr = e;
      // A dead credential is not a flaky link. 401/403 will never succeed on retry, and deferring it
      // made a hard auth failure indistinguishable from a blip: the 20-11-2026 GM ingest spent 90
      // minutes reporting "matched 0 | deferred 90,300" while every single request was coming back
      //   403 {"message":"The security token included in the request is invalid."}
      // and would have ground through 6 retries x 8 rounds before giving up with a message that did
      // not name the cause. Stop on the first one and say what it was.
      const code = (e && e.meta && e.meta.statusCode) || (e && e.statusCode) || 0;
      if (code === 401 || code === 403) {
        const raw = (e.meta && e.meta.body && (e.meta.body.message || e.meta.body.error)) || e.message;
        throw new Error('OpenSearch rejected the credentials (HTTP ' + code + '): '
          + (typeof raw === 'string' ? raw : JSON.stringify(raw))
          + ' -- check the AWS keys this process is using; retrying cannot fix it.');
      }
      if (a === 1 || a === 3) { try { client = co.makeClient(process.env.OPENSEARCH_ENDPOINT); } catch (x) {} }
      await sleep(Math.min(8000, 300 * 2 ** a));
    }
  }
  return false;
}

// Batch-resolve HQs with msearch (~6.5k requests for 1.9M domains vs 1.9M sequential lookups = the 34h fix).
// Resilient to the flaky local->managed link: a chunk that can't resolve is DEFERRED and retried in a later
// round once the link recovers, so a transient blip no longer aborts a multi-hour run. Only a persistent
// outage (domains still unresolved after all rounds) throws — a partial resolve would false-synthesize HQs.
async function batchResolve(domains) {
  const map = new Map();
  const CHUNK = 300;
  let pending = domains.slice();
  for (let round = 0; round < 8 && pending.length; round++) {
    const deferred = [];
    for (let i = 0; i < pending.length; i += CHUNK) {
      const chunk = pending.slice(i, i + CHUNK);
      if (!(await resolveChunk(chunk, map))) deferred.push(...chunk);   // defer, don't die
      if ((i / CHUNK) % 50 === 0) console.error(`  round ${round}: ${Math.min(i + CHUNK, pending.length).toLocaleString()}/${pending.length.toLocaleString()} | matched ${map.size.toLocaleString()} | deferred ${deferred.length.toLocaleString()}`);
    }
    if (deferred.length) { console.error(`  round ${round}: ${deferred.length.toLocaleString()} domains deferred (link blip) — pausing 20s, rebuilding client, retrying`); await sleep(20000); try { client = co.makeClient(process.env.OPENSEARCH_ENDPOINT); } catch (x) {} }
    pending = deferred;
  }
  if (pending.length) throw new Error(`batchResolve: ${pending.length} domains unresolved after 8 rounds — aborting (would false-synthesize)`
    + (lastResolveErr ? ` | last error: ${lastResolveErr.message}` : ''));
  return map;
}

function processGroup(domain, locs, hq0) {
  const rep = locs.reduce((a, b) => (score(b) > score(a) ? b : a), locs[0]);   // most complete Location
  const hqCRC = hq0 && (hq0.locality || hq0.region)
    ? { city: (hq0.locality || '').toLowerCase(), region: (hq0.region || '').toLowerCase(), country: (hq0.country || '').toLowerCase() }
    : crcOf(rep);

  // rollup sets (from HQ + every Location). Keep the two LinkedIn kinds STRICTLY apart:
  //   li   = personal profiles (linkedin.com/in)     -> linkedin_contact  (+ person contacts)
  //   coLi = company pages     (linkedin.com/company) -> linkedin_url      (company-level, NOT a contact)
  const bio = new Set(), li = new Set(), coLi = new Set(), emails = new Set();
  const collect = (r) => {
    splitL(r.bio_url).forEach((u) => bio.add(u));
    splitL(r.linkedin_contact).forEach((u) => { if (LI_IN.test(u)) li.add(u); });
    splitL(r.linkedin_url).forEach((u) => { if (LI_CO.test(u)) coLi.add(u); });   // hq0 (PDL) or GM company page
    (r.emails && r.emails.length ? r.emails : (r.email ? [r.email] : [])).forEach((e) => emails.add(e));  // ALL emails

  };
  if (hq0) collect(hq0);
  locs.forEach(collect);

  // merge (same City+Region as HQ) vs keep as child
  const children = [];
  for (const loc of locs) {
    if (hq0 == null && loc === rep) continue;                 // rep becomes the synthesized HQ, not a child
    const crc = crcOf(loc);
    const same = crc.city && crc.city === hqCRC.city && crc.region === hqCRC.region;
    if (!same) children.push(loc);
  }

  // HQ doc — update existing, or synthesize from the rep Location
  const hqDoc = hq0
    ? { company_type: 'HQ' }                                   // enrich the existing company
    : { company_type: 'HQ', name: rep.name, website: rep.website, domain, category: rep.category, full_address: rep.full_address, phone: rep.phone, phone_type: rep.phone_type,
        website_type: rep.website_type, facebook: rep.facebook, instagram: rep.instagram, whatsapp: rep.whatsapp, cid: rep.cid, time_stamp: rep.time_stamp, source_map: rep.source_map,
        locality: hqCRC.city, region: hqCRC.region, country: hqCRC.country };
  // rolled-up contact fields onto the HQ (dedup already via sets)
  hqDoc.bio_url = [...bio];                          // arrays, as the index holds them now
  hqDoc.linkedin_contact = [...li];                  // personal profiles (linkedin.com/in)
  hqDoc.linkedin_url = [...coLi];                    // company page (linkedin.com/company)
  // root_domain and company_name, which until now were in the mapping at 0.0% with nothing writing
  // them. root_domain is the REGISTRABLE domain, so a chain's locations on shadyside.pizzaparma.us and
  // pizzaparma.us group together -- domain already holds the host, so anything less is a duplicate
  // column. company_name is the PARENT's name: on an HQ its own, on a Location the HQ it belongs to,
  // which is the same sense the contacts index uses the name in.
  const hqName = String((hq0 && hq0.name) || rep.name || '').trim();
  const rootDom = co.registrableDomain(domain);
  if (hqName) hqDoc.company_name = hqName;
  if (rootDom) hqDoc.root_domain = rootDom;
  if (emails.size) { hqDoc.email = [...emails][0]; hqDoc.email_type = ex.classifyEmail([...emails][0]); }
  hqDoc.location_count = locs.length;

  // strip the rolled-up fields off the child Location records (all roll up to the HQ)
  // [] not '' for the multi-value fields: these are deliberate CLEARS (everything rolls up to the HQ),
  // and locToDoc writes the document directly rather than through coerceField.
  const childDocs = children.map((c) => ({ ...c, bio_url: [], linkedin_contact: [], linkedin_url: [], email: '', email_type: '',
    ...(hqName ? { company_name: hqName } : {}), ...(rootDom ? { root_domain: rootDom } : {}) }));

  // Phase 5 — contacts from the HQ's rolled-up signals
  let contacts = [];
  try { const b = che.buildEmployees({ linkedin: [...li], emails: [...emails], bio: [...bio] }, { genderMap, address: hqDoc.full_address || (hq0 && hq0.full_address) || '' }); contacts = b.structured || []; }
  catch (e) { /* best-effort */ }
  // Provenance for an employee with no team-profile URL of its own. These people were read off the
  // business's team page, which the export hands us, so fall back to it (then the site) rather than
  // leaving the record with no source: 94.9% of employee contacts had a blank Web Source URL even
  // though the originating page was known all along.
  const srcFallback = rep.team_page || rep.website || "";
  if (srcFallback) contacts = contacts.map((c) => (c.bio ? c : Object.assign({}, c, { src: srcFallback })));
  // The enrichment phone for the business, with the line type the export already classified. This is
  // the company line shared by everyone at it -- for the one- and two-person businesses that dominate
  // this data it is effectively the person's, but for a larger employer it is the switchboard, not a DDI.
  const phoneF = rep.xb_phone || "", phoneTypeF = rep.xb_phone_type || "";
  if (phoneF) contacts = contacts.map((c) => Object.assign({}, c, { phone: phoneF, phone_type: phoneTypeF }));

  return { hqId: hq0 ? hq0._id : 'gm:' + domain, isNew: !hq0, hqDoc, childDocs, contacts, domain };
}

(async () => {
  // Windows ships its own sort.exe whose flags are unrelated to GNU's — it reads `tmp -o sorted` as two
  // input files and dies with "Input file specified two times". Both binaries are usually on PATH and
  // which one wins depends on the launching shell (PowerShell puts System32 first, Git Bash puts Git's
  // usr/bin first), so the ETL silently worked or failed depending on how it was started. Resolve the
  // GNU binary explicitly; on Linux, where this normally runs, nothing changes.
  function gnuSort() {
    if (process.platform !== 'win32') return 'sort';
    for (const c of [process.env.GNU_SORT, 'C:/Program Files/Git/usr/bin/sort.exe',
                     'C:/Program Files (x86)/Git/usr/bin/sort.exe', 'C:/msys64/usr/bin/sort.exe']) {
      if (c && fs.existsSync(c)) return c;
    }
    console.error('WARNING: no GNU sort found; falling back to PATH "sort", which on Windows is probably the wrong one');
    return 'sort';
  }

  // ---- external sort by root_domain: write "domain\tline" -> sort -> stream grouped ----
  const tmp = path.join(OUT, '_gm-keyed.tsv'), sorted = path.join(OUT, '_gm-sorted.tsv');
  if (process.argv.includes('--resume') && fs.existsSync(sorted) && fs.statSync(sorted).size > 0) {
    console.error(`resuming from existing sorted file (${(fs.statSync(sorted).size / 1e9).toFixed(1)}GB) — skipping key+sort`);
  } else {
    console.error('keying by root_domain…');
    { const w = fs.createWriteStream(tmp); const rl = readline.createInterface({ input: fs.createReadStream(IN), crlfDelay: Infinity });
      for await (const l of rl) { if (!l.trim()) continue; let o; try { o = JSON.parse(l); } catch { continue; } if (o.root_domain) w.write(o.root_domain + '\t' + l + '\n'); }
      await new Promise((r) => w.end(r)); }
    console.error('sorting…');
    // GNU sort with no -S sizes its buffer against FREE memory, so on a shared box it expands to fill
    // whatever node has not taken yet. Partition 0 of the Overture run finished with 0.18GB available
    // on an 8GB machine that also serves the UI -- it fit, but nothing was holding it back. GM_SORT_MEM
    // caps the buffer and spills the overflow to -T (the volume): slower, and cannot be OOM-killed.
    // Unset = previous behaviour exactly, so the nightly gm pipeline is unchanged.
    const sortArgs = ['-t', '\t', '-k1,1', '-T', OUT];
    if (process.env.GM_SORT_MEM) sortArgs.push('-S', process.env.GM_SORT_MEM);
    const s = spawnSync(gnuSort(), [...sortArgs, tmp, '-o', sorted], { stdio: 'inherit' });
    if (s.status !== 0) { console.error('sort failed'); process.exit(1); }
  }

  // ---- pass 1: collect the unique (already-sorted) domain list ----
  // Read the first field of each line as BYTES, not with line.slice(0, tab).
  //
  // V8 represents s.slice(a, b) on a string of 13+ chars as a SlicedString: a pointer into the PARENT
  // string, not a copy. Every domain kept in domainList therefore pinned the entire JSON Location line
  // it was cut from, so the list was not ~50MB of hostnames but the whole sorted file, one retained line
  // at a time. Partition 1 of the Overture run died right here -- a Mark-Compact freed 18MB out of
  // 2400MB on a 2.46GB sorted file, then FATAL heap OOM. buf.toString(enc, start, end) builds a fresh
  // flat string from the bytes and pins nothing.
  //
  // This was never Overture-specific; every gm-build run has paid it in proportion to its input.
  function uniqueDomainsFromSorted(file) {
    const fd = fs.openSync(file, 'r');
    try {
      const CAP = 1 << 20;
      const buf = Buffer.allocUnsafe(CAP);
      const out = [];
      let last = null, pend = [], inKey = true, read;
      while ((read = fs.readSync(fd, buf, 0, CAP, null)) > 0) {
        let i = 0;
        while (i < read) {
          if (inKey) {
            let j = i;
            while (j < read && buf[j] !== 9 && buf[j] !== 10) j++;
            if (j < read && buf[j] === 9) {                 // tab: the key is complete
              let d;
              if (pend.length) { pend.push(Buffer.from(buf.subarray(i, j))); d = Buffer.concat(pend).toString('utf8'); pend = []; }
              else d = buf.toString('utf8', i, j);
              if (d !== last) { out.push(d); last = d; }
              inKey = false; i = j + 1;
            } else if (j < read && buf[j] === 10) {          // a line with no tab -- skip it
              pend = []; i = j + 1;
            } else { pend.push(Buffer.from(buf.subarray(i, read))); i = read; }   // key spans the read
          } else {
            const nl = buf.indexOf(10, i);
            if (nl === -1) i = read; else { i = nl + 1; inKey = true; }
          }
        }
      }
      return out;
    } finally { fs.closeSync(fd); }
  }
  console.error('collecting domains...');
  const domainList = uniqueDomainsFromSorted(sorted);
  const capped = LIMIT_DOMAINS ? domainList.slice(0, LIMIT_DOMAINS) : domainList;
  console.error(`${capped.length.toLocaleString()} unique domains — batch-resolving existing HQs (msearch)…`);
  const hqMap = await batchResolve(capped);
  console.error(`resolved ${hqMap.size.toLocaleString()} existing HQs | ${(capped.length - hqMap.size).toLocaleString()} will synthesize`);

  // ---- pass 2: stream groups, process each with its pre-resolved HQ (CPU-only, no per-group network) ----
  const hqW = fs.createWriteStream(path.join(OUT, 'gm-hq.ndjson'));
  const locW = fs.createWriteStream(path.join(OUT, 'gm-loc.ndjson'));
  const conW = fs.createWriteStream(path.join(OUT, 'gm-contacts.ndjson'));
  let domains = 0, hqNew = 0, hqUpd = 0, childN = 0, conN = 0, failed = 0; const t0 = Date.now();
  const flush = (dom, locs) => {
    try {
      const r = processGroup(dom, locs, hqMap.get(dom) || null);
      hqW.write(JSON.stringify({ id: r.hqId, isNew: r.isNew, doc: r.hqDoc }) + '\n'); r.isNew ? hqNew++ : hqUpd++;
      for (const c of r.childDocs) { locW.write(JSON.stringify({ id: c.cid || ('gm:' + r.domain + ':' + childN), doc: c }) + '\n'); childN++; }
      for (const ct of r.contacts) { conW.write(JSON.stringify(ct) + '\n'); conN++; }
    } catch (e) { failed++; }
  };

  const rl = readline.createInterface({ input: fs.createReadStream(sorted), crlfDelay: Infinity });
  let curDom = null, group = []; let stop = false;
  for await (const line of rl) {
    if (stop) break;
    const tab = line.indexOf('\t'); if (tab < 0) continue;
    const dom = line.slice(0, tab); const json = line.slice(tab + 1);
    let o; try { o = JSON.parse(json); } catch { continue; }
    if (dom !== curDom) {
      if (curDom && group.length) { flush(curDom, group); domains++; if (domains % 50000 === 0) console.error(`  ${domains.toLocaleString()} domains | ${hqUpd} upd + ${hqNew} new HQ | ${childN} loc | ${conN} contacts | ${failed} failed | ${Math.round(domains / ((Date.now() - t0) / 1000))}/s`); if (LIMIT_DOMAINS && domains >= LIMIT_DOMAINS) { stop = true; break; } }
      curDom = dom; group = [];
    }
    group.push(o);
  }
  if (!stop && curDom && group.length) { flush(curDom, group); domains++; }
  for (const w of [hqW, locW, conW]) await new Promise((r) => w.end(r));
  try { fs.unlinkSync(tmp); if (!LIMIT_DOMAINS) fs.unlinkSync(sorted); } catch (e) { /* keep for debug */ }
  console.error(`DONE: ${domains.toLocaleString()} domains -> ${hqUpd.toLocaleString()} HQ updates + ${hqNew.toLocaleString()} synthesized HQ | ${childN.toLocaleString()} child Locations | ${conN.toLocaleString()} contacts | ${failed.toLocaleString()} skipped | ${Math.round((Date.now() - t0) / 1000)}s`);
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
