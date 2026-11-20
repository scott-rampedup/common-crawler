/**
 * overture-load.js — Phase 1 of the Overture Maps -> Company Crawler ETL.
 *
 * Emits the SAME Location record shape gm-load.js emits, on purpose: gm-build.js (group by root domain,
 * resolve the HQ, roll up, build contacts) and gm-upsert.js (apply to OpenSearch) are then reused
 * unchanged. Only the reading end is new.
 *
 *   overture-locations.ndjson   one JSON Location per line  -> gm-build
 *   overture-bio-urls.txt       person pages found in the website column -> Hop-2 extraction
 *
 * WHY DUCKDB AND NOT A DOWNLOAD. Overture publishes to an anonymous public S3 bucket with no
 * requester-pays, and DuckDB's httpfs reads parquet by range request. The release is 11.2GB across 16
 * shards; we never fetch most of it, because the column pruning and the gate below happen server-side.
 * Nothing is staged, so the monthly job has no dependency on anyone's workstation. The CLI binary is used
 * rather than the npm package: that one is a native module and would drag node-gyp into the image.
 *
 * THE GATE (measured against release 2026-11-18.0, shard 1 of 16, scaled):
 *   90,295,748 places -> 2,208,881/shard have a website -> 1,662,098 at confidence >= 0.5
 *                     -> 1,642,003 still operating  ==  ~26.3M Locations to ingest.
 * confidence is Overture's own agreement score across contributing datasets; below 0.5 the record is
 * usually one unreviewed source. operating_status is honoured because a closed place is not a business.
 *
 * DEDUPE KEYS ON THE GERS `id`, not name+address. That id is stable across monthly releases, which is
 * what makes step 10 (re-run monthly) an update rather than a re-import. It lands in the Location's
 * `cid` field, which is what gm-build/gm-upsert already dedupe Locations by.
 *
 * ATTRIBUTION. Overture is CDLA-Permissive-2.0 / Apache-2.0 with no ODbL share-alike (it carries no OSM
 * data), but the licence still expects the source to travel with the record, so `sources` is preserved
 * into source_map rather than dropped.
 *
 *   node overture-load.js --sql                      # print the extraction SQL and exit
 *   node overture-load.js --release 2026-11-18.0 --out <dir> [--shards 2] [--limit N]
 *   node overture-load.js --in raw.ndjson --out <dir>    # transform only, from an earlier extract
 */
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const { spawn } = require('child_process');
const ex = require('./extractor');
const naics = require('./naics');
const psl = require('./psl-suffix');
const { employerDomainFor, normHost, isFreemail, isBadCompanyDomain } = require('./non-company-domains');

const arg = (f, d) => { const i = process.argv.indexOf('--' + f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const has = (f) => process.argv.includes('--' + f);

const RELEASE = arg('release', '2026-11-18.0');
const OUT = arg('out', '/tmp/overture');
const IN = arg('in', '');
const SHARDS = Number(arg('shards', '0')) || 0;          // 0 = every shard in the release
const LIMIT = Number(arg('limit', '0')) || 0;
const MIN_CONF = Number(arg('min-confidence', '0.5'));
const DUCKDB = process.env.DUCKDB_BIN || 'duckdb';
// Scratch for the per-shard extracts. Defaults under DATA_DIR because / is only 7.4GB on the Fly
// machine while the mounted volume has 23GB, and one shard's JSON runs to roughly a gigabyte.
const TMP = arg('tmp', path.join(process.env.DATA_DIR || '/tmp', 'overture-scratch'));
const S3_HOST = 'overturemaps-us-west-2.s3.us-west-2.amazonaws.com';

// ---------------------------------------------------------------- lookups from the workbook
function loadJson(file, fallback) {
  try { return JSON.parse(fs.readFileSync(path.join(__dirname, 'data', file), 'utf8')); }
  catch (e) { return fallback; }
}
// Step 6: the Domain tab. host -> Social | Directory | Host | Shortener | App | Phone. A populated
// domain_type is what step 22 keys on to decide the website cannot stand as the company's own.
const DOMAIN_TYPES = (() => {
  const raw = loadJson('domain-types.json', {});
  const m = new Map();
  for (const k of Object.keys(raw)) m.set(normHost(k), String(raw[k] || '').trim());
  return m;
})();
// Steps 7/8: the TLD tab. Longest suffix wins, so 'ac.uk' beats 'uk'.
const TLD = (() => {
  const raw = loadJson('tld.json', {});
  const m = new Map();
  for (const k of Object.keys(raw)) m.set(String(k).toLowerCase().replace(/^\./, ''), raw[k]);
  return m;
})();
// Steps 10-16: the Path ID tab, keyed on the normalized last path segment.
const PATH_ID = (() => {
  const rows = loadJson('path-id.json', []);
  const m = new Map();
  for (const r of rows) {
    const k = String(r && r.path_id || '').trim().toLowerCase();
    if (k && !m.has(k)) m.set(k, r);
  }
  return m;
})();
// Steps 31-33: the Overture crosswalk FIRST, the shared lookup only on a miss. See the header of
// data-overture-naics.csv for why these are two files and not one.
const OVERTURE_NAICS = (() => {
  const m = new Map();
  try {
    const text = fs.readFileSync(path.join(__dirname, 'data-overture-naics.csv'), 'utf8');
    const rows = text.split(/\r?\n/).slice(1);
    for (const line of rows) {
      if (!line.trim()) continue;
      const c = parseCsvRow(line);
      const key = String(c[1] || '').trim().toLowerCase();
      if (key && c[2] && !m.has(key)) m.set(key, { code: String(c[2]).trim(), title: String(c[3] || '').trim(), hierarchy: String(c[4] || '').trim() });
    }
  } catch (e) { /* bundled file absent -> naics.assignNaics still runs below */ }
  return m;
})();

// The person-name lexicon, for the bio-URL confirmation below. Loaded once; classifyDirectory needs it
// to tell a person slug from an ordinary one.
const GENDER_MAP = (() => {
  try { return ex.loadGenderMap(path.join(__dirname, 'names-genders.csv')); }
  catch (e) { return {}; }
})();

function parseCsvRow(line) {
  const out = []; let f = '', q = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (q) { if (ch === '"') { if (line[i + 1] === '"') { f += '"'; i++; } else q = false; } else f += ch; }
    else if (ch === '"') q = true;
    else if (ch === ',') { out.push(f); f = ''; }
    else f += ch;
  }
  out.push(f);
  return out;
}

// ---------------------------------------------------------------- the extraction SQL
// Column-pruned and gated so the range reads stay small. Everything that needs a lexicon (email type,
// gender, path ID, NAICS) is left to Node below; SQL only flattens and filters.
//
// REQUIRES DUCKDB >= 2.0. The `lambda s: ...` form below is the new lambda syntax; 2.0 deprecated the
// old `s -> ...` arrow and errors on it outright, so this SQL will not run on a 1.x CLI. The Dockerfile
// pins the version for that reason.
// One shard per call, into a real file.
//
// DuckDB 2.0 cannot COPY to stdout: '/dev/stdout', '/dev/fd/1' and the OVERWRITE / OVERWRITE_OR_IGNORE
// flags all fail with `Cannot open file: File exists`, and the bare 'stdout' token silently writes a
// file called "stdout" in the working directory. Verified on the deployed image, not assumed.
//
// Per-shard is the better shape regardless: 16 separate queries give the job a checkpoint every shard,
// where one query over all of them would be a single long-running statement with nothing to resume from.
// Each file is streamed and then deleted, so peak disk is one shard's worth (~1GB), not the whole run's.
function buildSql(keys, outPath) {
  const files = keys.map((k) => `'https://${S3_HOST}/${k}'`).join(', ');
  return `INSTALL httpfs; LOAD httpfs;
COPY (
  SELECT id,
         names.primary                       AS name,
         brand.names.primary                 AS brand,
         taxonomy.primary                    AS category,
         taxonomy.hierarchy                  AS category_hierarchy,
         confidence,
         operating_status,
         websites, socials, emails, phones,
         addresses[1].freeform               AS addr_freeform,
         addresses[1].locality               AS locality,
         addresses[1].region                 AS region,
         addresses[1].postcode               AS postcode,
         addresses[1].country                AS country,
         list_transform(sources, lambda s: s.dataset) AS source_datasets
  FROM read_parquet([${files}])
  WHERE websites IS NOT NULL AND len(websites) > 0
    AND confidence >= ${MIN_CONF}
    AND (operating_status IS NULL OR operating_status = 'open')
  ${LIMIT ? 'LIMIT ' + LIMIT : ''}
) TO '${outPath}' (FORMAT JSON);`;
}

// ---------------------------------------------------------------- per-record transform
const clean = (s) => String(s == null ? '' : s).trim();

// Overture ships 4.3% of its website values with no scheme -- "www.americansamoa.gov", measured over a
// 40,000-place sample. Everything downstream must get an absolute URL, because extractor.classifyDirectory
// calls `new URL(url).pathname` WITHOUT a try/catch and takes the whole run down on a throw. That is
// exactly how the first full-shard run on Fly died ("ERR Invalid URL") after the local sample passed:
// the sample held scheme-less websites, but none that ALSO matched a Path ID, which is the only route to
// that call. Normalising once here is safer than guarding each call site.
function absUrl(u) {
  const s = clean(u);
  if (!s) return '';
  const withScheme = /^https?:\/\//i.test(s) ? s : 'http://' + s.replace(/^\/+/, '');
  try { return new URL(withScheme).toString(); } catch (e) { return ''; }
}
const hostOf = (u) => { try { return normHost(new URL(/^https?:\/\//i.test(u) ? u : 'http://' + u).hostname); } catch (e) { return ''; } };
const rootOf = (h) => (h ? (psl.registrableOrHost(h) || h) : '');

// Step 2 of Scott's plan: a social URL is not a website. Route it to its own bucket instead of letting
// it stand as the company's domain.
const SOCIAL_BUCKET = [
  [/(^|\.)linkedin\.com$/i, (u) => (/\/company\//i.test(u) ? 'linkedin_url' : 'linkedin_contact')],
  [/(^|\.)facebook\.com$/i, () => 'facebook'],
  [/(^|\.)instagram\.com$/i, () => 'instagram'],
  [/(^|\.)(twitter|x)\.com$/i, () => 'twitter'],
  [/(^|\.)wa\.me$/i, () => 'whatsapp'],
  [/(^|\.)youtube\.com$/i, () => 'youtube'],
];
function socialBucket(url) {
  const h = hostOf(url);
  for (const [re, pick] of SOCIAL_BUCKET) if (re.test(h)) return pick(url);
  return '';
}

// Steps 9-16: the last meaningful path segment of a URL, under the workbook's Last Path Logic.
const LAST_PATH_STRIP = /^(dr|mr|mrs|ms|phd|esq|cpa|prof|sr|jr|iii|mba|hon|rev|father|sgt|lt|cfm|lcsw|about|the|team|agency)$/i;
function lastPath(url) {
  let u;
  try { u = new URL(/^https?:\/\//i.test(url) ? url : 'http://' + url); } catch (e) { return ''; }
  const segs = u.pathname.split('/').filter(Boolean)
    .map((s) => s.replace(/\.(aspx|html?|php|pdf)$/i, ''));
  // Rule 2: a purely numeric last segment is a record id, not a name -- step back to the one before it.
  let i = segs.length - 1;
  while (i >= 0 && /^\d+$/.test(segs[i])) i--;
  if (i < 0) return '';
  // Rule 4: strip honorifics and filler so "dr-jane-doe" reads as "jane doe".
  const words = segs[i].split(/[-_+]+/).filter((w) => w && !LAST_PATH_STRIP.test(w));
  return words.join(' ').toLowerCase().trim();
}

function pathIdFor(url) {
  let u;
  try { u = new URL(/^https?:\/\//i.test(url) ? url : 'http://' + url); } catch (e) { return null; }
  // Scan every segment, not just the last: /agents/detail/jane-doe carries its directory term early.
  for (const raw of u.pathname.split('/').filter(Boolean)) {
    const seg = raw.replace(/\.(aspx|html?|php)$/i, '').toLowerCase();
    const hit = PATH_ID.get(seg);
    if (hit) return hit;
  }
  return null;
}

function tldInfo(host) {
  const parts = String(host || '').split('.').filter(Boolean);
  for (let i = 1; i < parts.length; i++) {           // longest suffix first: ac.uk before uk
    const suf = parts.slice(i).join('.');
    if (TLD.has(suf)) return TLD.get(suf);
  }
  return null;
}

function assignNaics(doc) {
  const key = clean(doc.category).toLowerCase();
  const hit = OVERTURE_NAICS.get(key);
  if (hit) { doc.naics_code = hit.code; doc.naics_title = hit.title; return doc; }
  return naics.assignNaics(doc);                     // shared lookup as the fallback
}

function toLocation(r) {
  const name = clean(r.name) || clean(r.brand);
  if (!name) return null;

  // Steps 1-3 + the alternate/social filter. The first website that is NOT a social, shortener, host or
  // directory wins; the rest are kept as alternates so nothing is silently discarded.
  // Normalised to absolute URLs up front: a scheme-less value otherwise reaches classifyDirectory and
  // throws. Anything that will not parse at all is dropped here rather than carried as a bad website.
  const sites = (r.websites || []).map(absUrl).filter(Boolean);
  const socials = {};
  let website = '', alternates = [];
  for (const u of sites) {
    const bucket = socialBucket(u);
    if (bucket) { if (!socials[bucket]) socials[bucket] = u; continue; }
    const h = hostOf(u);
    const type = DOMAIN_TYPES.get(rootOf(h)) || DOMAIN_TYPES.get(h) || '';
    // Scott's Domain tab OR our own platform/freemail list -- the union, because neither is complete.
    // Measured on a 40,000-place sample: the Domain tab alone let goo.gl through as the employer domain
    // of 516 places, since it lists tinyurl.com and lnk.bio but not that one.
    if (type || isBadCompanyDomain(h) || isBadCompanyDomain(rootOf(h))) { alternates.push(u); continue; }
    if (!website) website = u; else alternates.push(u);
  }
  // Overture's own socials column, routed the same way.
  for (const u of (r.socials || []).map(clean).filter(Boolean)) {
    const bucket = socialBucket(u);
    if (bucket && !socials[bucket]) socials[bucket] = u;
  }

  // Steps 20-22. Email type decides whether the mailbox may stand in for a missing website.
  const emails = [...new Set((r.emails || []).map((e) => ex.cleanEmail(clean(e)).toLowerCase()).filter(Boolean))];
  const email = emails[0] || '';
  const emailType = email ? ex.classifyEmail(email) : '';
  if (!website && email && emailType !== 'Personal') {
    // Step 4 of the plan: a role-based or professional mailbox identifies the company even when the
    // website column held nothing usable. employerDomainFor still refuses a freemail or platform host.
    const fromEmail = employerDomainFor(rootOf(hostOf('http://' + (email.split('@')[1] || ''))), email);
    if (fromEmail) website = 'https://' + fromEmail + '/';
  }
  const host = hostOf(website);
  const root = rootOf(host);
  if (!root) return null;                            // nothing usable to join on

  const phone = clean((r.phones || [])[0]);
  const tld = tldInfo(host) || {};
  const pid = pathIdFor(website);
  const lp = lastPath(website);

  const doc = {
    company_type: 'Location',
    root_domain: root,
    // The GERS id. Stable across monthly releases, which is what makes the re-run an update.
    cid: clean(r.id),
    company_status: clean(r.operating_status) || 'Open',
    name,
    brand: clean(r.brand),
    full_address: clean(r.addr_freeform),
    locality: clean(r.locality),
    region: clean(r.region),
    postal_code: clean(r.postcode),
    country: clean(r.country),
    website: website,
    page_url: website,
    website_type: host && host !== root ? 'Sub-Domain' : 'Root',
    category: clean(r.category),
    category_hierarchy: (r.category_hierarchy || []).join(' > '),
    email,
    emails,
    email_type: emailType,
    phone,
    phone_type: phone ? (ex.intlLineType ? '' : '') : '',
    facebook: socials.facebook || '',
    instagram: socials.instagram || '',
    twitter: socials.twitter || '',
    whatsapp: socials.whatsapp || '',
    linkedin_url: socials.linkedin_url || '',
    linkedin_contact: socials.linkedin_contact || '',
    alternate_websites: alternates.join('; '),
    confidence: Number(r.confidence) || 0,
    tld_country: clean(tld.country),
    tld_industry: clean(tld.industry),
    // Licence attribution travels with the record (CDLA-Permissive-2.0).
    source_map: 'overture:' + RELEASE + (r.source_datasets && r.source_datasets.length ? ' (' + [...new Set(r.source_datasets)].join(',') + ')' : ''),
    time_stamp: new Date().toISOString(),
  };

  // Steps 10-16: when the website itself is a person page, carry what the Path ID tab says about it.
  if (pid) {
    doc.path_id = pid.path_id;
    doc.directory = pid.directory;
    if (pid.title) doc.title = pid.title;
    if (pid.department) doc.job_department = pid.department;
    if (pid.job_level) doc.job_level = pid.job_level;
    if (pid.leadership_type) doc.leadership_type = pid.leadership_type;
    // Is BIO alone is NOT enough, for two reasons found on real data.
    //
    // The workbook marks 90 Locations rows, 12 Company, 10 News, 5 Contact and 4 Investor Relations rows
    // as Is BIO too, so the flag by itself sent branch-office pages to the contact crawler as person
    // pages -- /locations/south-pacific/niue/scenic-matavai-resort was flagged in the first 40,000-row
    // run. Hence the Directory check.
    //
    // And a People path ID can still sit on a page with no person on it: "personas" is a legitimate
    // people term that on Spanish-language bank and telco sites means the RETAIL-CUSTOMER section
    // (telcel.com/personas/..., bbva.mx/personas/..., scotiabank.com.mx/es-mx/Personas/), which was 5 of
    // the 32 URLs flagged in that run. So the shared classifier has to agree: it applies the person-name
    // lexicon to the slug, which is the same gate gm-load's bio URLs pass through.
    const bioOk = pid.is_bio && pid.directory === 'People'
      && ex.classifyDirectory(website, '', {}, GENDER_MAP) === 'BIO URL';
    if (bioOk) { doc.bio_url = website; doc.last_path = lp; }
  }
  return assignNaics(doc);
}

module.exports = { toLocation, lastPath, pathIdFor, socialBucket, tldInfo, buildSql };

// ---------------------------------------------------------------- selftest
if (require.main === module && has('selftest')) {
  let pass = 0, fail = 0;
  const eq = (label, got, want) => {
    const g = JSON.stringify(got), w = JSON.stringify(want);
    if (g === w) { pass++; return; }
    fail++; console.error('  FAIL ' + label + ': got ' + g + ' want ' + w);
  };

  eq('last path strips an honorific', lastPath('https://a.com/agents/dr-jane-doe'), 'jane doe');
  eq('last path steps over a record id', lastPath('https://a.com/agents/jane-doe/71955'), 'jane doe');
  eq('last path drops an extension', lastPath('https://a.com/team/john-smith.aspx'), 'john smith');
  eq('last path drops filler words', lastPath('https://a.com/about-the-team/ann-lee'), 'ann lee');

  eq('linkedin company page is a company url', socialBucket('https://linkedin.com/company/acme'), 'linkedin_url');
  eq('linkedin profile is a contact url', socialBucket('https://linkedin.com/in/jane-doe'), 'linkedin_contact');
  eq('facebook routes to facebook', socialBucket('https://www.facebook.com/acme'), 'facebook');
  eq('an ordinary site is not social', socialBucket('https://acme.com/'), '');

  eq('tld longest suffix wins', (tldInfo('dept.ac.uk') || {}).industry, 'Education');

  const base = { id: 'g1', name: 'Acme Realty', confidence: 0.8, websites: ['https://acmerealty.com/'], emails: [], phones: ['+15551234'], addresses: [] };
  const l = toLocation(base);
  eq('a plain place becomes a Location', l && l.company_type, 'Location');
  eq('root domain is derived', l && l.root_domain, 'acmerealty.com');
  eq('GERS id lands in cid', l && l.cid, 'g1');

  // A social-only website must not become the company's domain; it goes to its bucket instead.
  const s = toLocation({ ...base, websites: ['https://www.facebook.com/acme'] });
  eq('a facebook-only place yields no domain', s, null);

  // Step 4: a professional mailbox stands in for a missing website.
  const e = toLocation({ ...base, websites: ['https://linktr.ee/acme'], emails: ['info@acmerealty.com'] });
  eq('role mailbox supplies the domain', e && e.root_domain, 'acmerealty.com');
  eq('...and the platform url is kept as an alternate', e && e.alternate_websites, 'https://linktr.ee/acme');

  // ...but a personal mailbox never does.
  const g = toLocation({ ...base, websites: ['https://linktr.ee/acme'], emails: ['jane@gmail.com'] });
  eq('gmail never supplies a domain', g, null);

  // 449110 (Furniture Retailers), the current code -- not the retired 2017 442110. Proves the Overture
  // crosswalk is the one being consulted, since the shared lookup has no underscore-keyed entry at all.
  const n = toLocation({ ...base, category: 'furniture_store' });
  eq('overture category resolves NAICS', n && n.naics_code, '449110');
  eq('...with its title', n && n.naics_title, 'Furniture Retailers');

  const b = toLocation({ ...base, websites: ['https://acmerealty.com/agents/jane-doe'] });
  eq('a bio website is flagged', b && b.bio_url, 'https://acmerealty.com/agents/jane-doe');
  eq('...and carries its directory', b && b.directory, 'People');

  // 4.3% of Overture websites arrive with no scheme. One of those that ALSO matches a Path ID is what
  // killed the first full-shard run: classifyDirectory calls new URL(url).pathname with no try/catch.
  const ns = toLocation({ ...base, websites: ['www.acmerealty.com/agents/jane-doe'] });
  eq('scheme-less website still parses', ns && ns.root_domain, 'acmerealty.com');
  eq('...and still flags the bio', ns && ns.bio_url, 'http://www.acmerealty.com/agents/jane-doe');
  eq('unparseable website yields nothing', toLocation({ ...base, websites: ['http://'] }), null);
  eq('empty website yields nothing', toLocation({ ...base, websites: [''] }), null);

  console.log('overture-load selftest: ' + pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
}

// ---------------------------------------------------------------- run
async function listShards() {
  const url = `https://${S3_HOST}/?list-type=2&prefix=${encodeURIComponent('release/' + RELEASE + '/theme=places/type=place/')}`;
  const xml = await (await fetch(url)).text();
  const keys = [...xml.matchAll(/<Key>(.*?)<\/Key>/g)].map((m) => m[1]).filter((k) => k.endsWith('.parquet'));
  return SHARDS ? keys.slice(0, SHARDS) : keys;
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const locOut = fs.createWriteStream(path.join(OUT, 'overture-locations.ndjson'));
  const bio = new Set();
  let seen = 0, kept = 0, noDomain = 0, bioFound = 0, errored = 0;
  const t0 = Date.now();

  const onLine = (line) => {
    if (!line.trim()) return;
    let r; try { r = JSON.parse(line); } catch (e) { return; }
    seen++;
    // One malformed record must not end a 26M-row run. The first full-shard attempt died on a single
    // unparseable URL after 37 seconds, losing the whole shard's work; the count is reported at the end
    // so a rising number is visible rather than silent.
    let doc;
    try { doc = toLocation(r); }
    catch (e) {
      errored++;
      if (errored <= 3) console.error(`  skipped a record: ${e.message} (${clean(r && r.id)})`);
      return;
    }
    if (!doc) { noDomain++; return; }
    if (doc.bio_url) { bio.add(doc.bio_url); bioFound++; }
    locOut.write(JSON.stringify(doc) + '\n');
    kept++;
    if (kept % 100000 === 0) {
      console.error(`  seen ${seen.toLocaleString()} | kept ${kept.toLocaleString()} | bio ${bioFound.toLocaleString()} | ${Math.round(kept / ((Date.now() - t0) / 1000))}/s`);
    }
  };

  if (IN) {
    const rl = readline.createInterface({ input: fs.createReadStream(IN), crlfDelay: Infinity });
    for await (const line of rl) onLine(line);
  } else {
    const keys = await listShards();
    console.error(`release ${RELEASE}: ${keys.length} shard(s), confidence >= ${MIN_CONF}, scratch ${TMP}`);
    fs.mkdirSync(TMP, { recursive: true });
    for (let i = 0; i < keys.length; i++) {
      const raw = path.join(TMP, `overture-shard-${i}.json`);
      try { fs.unlinkSync(raw); } catch (e) { /* not there, which is the normal case */ }
      const t1 = Date.now();
      await new Promise((res, rej) => {
        const p = spawn(DUCKDB, ['-c', buildSql([keys[i]], raw)], { stdio: ['ignore', 'inherit', 'inherit'] });
        p.on('exit', (c) => (c === 0 ? res() : rej(new Error(`duckdb exited ${c} on shard ${i}`))));
      });
      const before = kept;
      const rl = readline.createInterface({ input: fs.createReadStream(raw), crlfDelay: Infinity });
      for await (const line of rl) onLine(line);
      // Delete as we go: peak disk stays one shard, not the whole release.
      try { fs.unlinkSync(raw); } catch (e) { /* already gone */ }
      console.error(`  shard ${i + 1}/${keys.length}: +${(kept - before).toLocaleString()} Locations `
        + `(${Math.round((Date.now() - t1) / 1000)}s)`);
      if (LIMIT && seen >= LIMIT) break;
    }
  }

  locOut.end();
  fs.writeFileSync(path.join(OUT, 'overture-bio-urls.txt'), [...bio].join('\n') + '\n');
  console.error(`\nDONE in ${Math.round((Date.now() - t0) / 1000)}s`);
  console.error(`  places read        ${seen.toLocaleString()}`);
  console.error(`  Locations written  ${kept.toLocaleString()}`);
  console.error(`  no usable domain   ${noDomain.toLocaleString()}`);
  console.error(`  bio URLs           ${bio.size.toLocaleString()}  -> overture-bio-urls.txt`);
  if (errored) console.error(`  SKIPPED on error   ${errored.toLocaleString()}`);
}

if (require.main === module && !has('selftest')) {
  if (has('sql')) { listShards().then((k) => console.log(buildSql(k.slice(0, 1), '<shard>.json'))).catch((e) => { console.error(e.message); process.exit(1); }); }
  else main().catch((e) => { console.error('ERR', e.message); process.exit(1); });
}
