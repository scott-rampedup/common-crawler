/**
 * preprocess-bio-urls.js — write a Pre-Process contact record for every discovered BIO URL.
 *
 *   OPENSEARCH_ENDPOINT=… node preprocess-bio-urls.js --in s3://bucket/prefix/ [--conc 24] [--limit N]
 *                                                     [--dry-run] [--no-company]
 *
 * WHY: the nightly sweep asks the contacts index "do we have a contact for this URL?" and re-queues every
 * URL where the answer is no. Nothing converts them quickly enough, so the answer stays no and the same
 * URLs are re-discovered every night -- 6,326,337 on 23 Aug, of which the overwhelming majority had been
 * queued on previous nights. The queue is the accumulated difference between discovery and conversion.
 *
 * A Pre-Process record makes the answer yes the moment a URL is found. It carries everything derivable
 * WITHOUT fetching the page, and a status saying so:
 *
 *   source, web_source_url, updated_at, domain, last_path, path_id   -- from the URL
 *   first, last, gender, position                                    -- from the URL slug + lexicons
 *   company fields                                                   -- joined by root domain
 *   status = "Pre-Process"                                           -- this is modelled, not crawled
 *
 * Phase 0 (opensearch.js contactId) is what makes this possible: a Pre-Process record keys on the BIO URL
 * rather than an email, so it can exist without one, and processing later REPLACES it instead of creating
 * a second record for the same person.
 *
 * PRIORITY. Records are written with a `pp_priority` so the processing queue can work the most complete
 * ones first: gender + position, then gender, then position, then neither.
 */
const fs = require('fs');
const path = require('path');
const os = require('./opensearch');
const co = require('./companies');
const ccEngine = require('./cc-engine');
const { resolveDomains } = require('./enrich-firmographics');

const arg = (f, d) => { const i = process.argv.indexOf(f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const num = (f, d) => Number(arg(f, '')) || d;
const IN = arg('--in', '');
const CONC = num('--conc', 24);
const LIMIT = num('--limit', 0);
const DRY = process.argv.includes('--dry-run');
const NO_COMPANY = process.argv.includes('--no-company');
const BATCH = num('--batch', 1000);

let GENDER_MAP = {};
try {
  const csv = fs.readFileSync(path.join(__dirname, 'names-genders.csv'), 'utf8');
  for (const line of csv.split(/\r?\n/).slice(1)) {
    const [n, g] = line.split(',');
    if (n && g) GENDER_MAP[n.trim().toLowerCase()] = g.trim().charAt(0).toUpperCase();
  }
} catch (e) { /* optional */ }

// Directory/role words that appear as a path segment on bio URLs and name the person's function.
const ROLE_WORDS = new Map(Object.entries({
  agent: 'Agent', agents: 'Agent', attorney: 'Attorney', attorneys: 'Attorney', lawyer: 'Attorney',
  doctor: 'Doctor', doctors: 'Doctor', physician: 'Physician', provider: 'Provider', providers: 'Provider',
  broker: 'Broker', brokers: 'Broker', realtor: 'Realtor', realtors: 'Realtor',
  advisor: 'Advisor', advisors: 'Advisor', adviser: 'Advisor', consultant: 'Consultant', consultants: 'Consultant',
  dentist: 'Dentist', dentists: 'Dentist', therapist: 'Therapist', therapists: 'Therapist',
  professor: 'Professor', faculty: 'Faculty', staff: 'Staff', team: 'Team Member', people: 'Team Member',
  'loan-officer': 'Loan Officer', 'loan-officers': 'Loan Officer', officer: 'Officer',
  engineer: 'Engineer', nurse: 'Nurse', partner: 'Partner', partners: 'Partner', associate: 'Associate',
}));

const STOP = new Set(['www', 'com', 'net', 'org', 'html', 'htm', 'php', 'aspx', 'index', 'profile', 'bio', 'about', 'en', 'us']);
const cap = (w) => w ? w.charAt(0).toUpperCase() + w.slice(1).toLowerCase() : '';

/**
 * Everything a BIO URL yields without fetching it.
 * Conservative on names: a slug that does not look like a person's name leaves first/last blank rather
 * than inventing one. A Pre-Process record with no name is still useful -- it stops re-discovery and
 * carries the domain and company -- whereas a wrong name is worse than none.
 */
function fromUrl(rawUrl) {
  let u;
  try { u = new URL(rawUrl); } catch (e) { return null; }
  const host = u.hostname.replace(/^www\./, '').toLowerCase();
  const segs = u.pathname.split('/').map((s) => decodeURIComponent(s).trim()).filter(Boolean);
  if (!segs.length) return null;
  const lastRaw = segs[segs.length - 1].replace(/\.(html?|php|aspx?)$/i, '');
  const parent = segs.length > 1 ? segs[segs.length - 2] : '';

  // position: a role word anywhere in the path, nearest the leaf first
  let position = '';
  for (let i = segs.length - 2; i >= 0 && !position; i--) {
    const k = segs[i].toLowerCase();
    if (ROLE_WORDS.has(k)) position = ROLE_WORDS.get(k);
  }

  // name: the leaf segment split on - or _ ; keep 2-3 alphabetic parts
  const parts = lastRaw.split(/[-_.+]/).map((s) => s.trim()).filter((s) => s && /^[a-z][a-z'’]*$/i.test(s) && !STOP.has(s.toLowerCase()));
  let first = '', last = '';
  if (parts.length >= 2 && parts.length <= 4) {
    // drop a trailing numeric id or role word that survived
    const clean = parts.filter((p) => !ROLE_WORDS.has(p.toLowerCase()));
    if (clean.length >= 2) { first = cap(clean[0]); last = cap(clean[clean.length - 1]); }
  }
  const gender = first ? (GENDER_MAP[first.toLowerCase()] || '') : '';
  return {
    web_source_url: rawUrl,
    domain: host,
    last_path: lastRaw,
    path_id: parent,
    first, last, gender, position,
    name: [first, last].filter(Boolean).join(' '),
  };
}

// gender + position = the most complete modelling, so it is processed first.
function priority(r) {
  if (r.gender && r.position) return 1;
  if (r.gender) return 2;
  if (r.position) return 3;
  return 4;
}

async function readList(src) {
  const { S3Client, ListObjectsV2Command, GetObjectCommand } = require('@aws-sdk/client-s3');
  const region = process.env.AWS_REGION || 'us-east-1';
  const m = /^s3:\/\/([^/]+)\/(.*)$/i.exec(src);
  if (!m) return { local: src };
  const s3 = new S3Client({ region });
  let tok = null; const keys = [];
  do {
    const r = await s3.send(new ListObjectsV2Command({ Bucket: m[1], Prefix: m[2], ContinuationToken: tok }));
    for (const o of (r.Contents || [])) keys.push(o.Key);
    tok = r.IsTruncated ? r.NextContinuationToken : null;
  } while (tok);
  return { bucket: m[1], keys, s3, GetObjectCommand };
}

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  if (!IN) { console.error('need --in <s3://prefix/ | file>'); process.exit(1); }
  const t0 = Date.now();
  const client = os.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const coClient = co.makeClient(process.env.OPENSEARCH_ENDPOINT);
  await os.ensureIndex(client);

  const src = await readList(IN);
  const sum = { seen: 0, parsed: 0, unparsed: 0, alreadyContact: 0, written: 0, errors: 0, skipped: 0,
    byPriority: { 1: 0, 2: 0, 3: 0, 4: 0 }, companyFilled: 0 };
  let buf = [];

  const flush = async () => {
    if (!buf.length) return;
    const batch = buf; buf = [];
    // Company data by root domain, so the record is useful the moment it exists.
    if (!NO_COMPANY) {
      try {
        const map = await resolveDomains(coClient, [...new Set(batch.map((b) => b.domain))]);
        for (const b of batch) {
          const c = map.get(b.domain);
          if (!c) continue;
          sum.companyFilled++;
          b.industry = c.industry || ''; b.company_size = c.size || '';
          b.company_hq = [c.locality, c.region, c.country].filter(Boolean).join(', ');
          b.company_country = c.country || ''; b.company_founded = c.founded || null;
          b.company_linkedin = c.linkedin_url || ''; b.company_name = c.name || '';
          if (c.name) b.company = c.name;         // the NAME, not the website
        }
      } catch (e) { console.error('  company join failed for this batch:', e.message); }
    }
    if (DRY) return;
    try {
      const r = await os.bulkUpsert(client, batch, { clearPlaceholder: false });   // these ARE the placeholders
      sum.written += r.indexed; sum.skipped += (r.skipped || 0); sum.errors += r.errors;
    } catch (e) { sum.errors += batch.length; console.error('  bulk failed:', e.message); }
  };

  const handle = async (urls) => {
    // Never overwrite a real contact: only URLs with no record at all become placeholders.
    const { knownSet } = require('./skip-known');
    let have = new Set();
    try { have = await knownSet(urls, { client }); } catch (e) { /* treat as unknown; upsert is score-gated */ }
    for (const u of urls) {
      sum.seen++;
      if (have.has(u)) { sum.alreadyContact++; continue; }
      const rec = fromUrl(u);
      if (!rec) { sum.unparsed++; continue; }
      sum.parsed++;
      const p = priority(rec);
      sum.byPriority[p]++;
      buf.push({
        ...rec,
        source: 'Sitemap Monitor',
        status: os.PRE_STATUS,
        pp_priority: p,
        updated_at: new Date().toISOString(),
        time_stamp: new Date().toISOString().slice(0, 10),
        score: 0,                       // lowest score: any real extraction outranks it
      });
      if (buf.length >= BATCH) await flush();
      if (LIMIT && sum.seen >= LIMIT) return;
    }
  };

  if (src.local) {
    const readline = require('readline');
    const rl = readline.createInterface({ input: fs.createReadStream(src.local), crlfDelay: Infinity });
    let chunk = [];
    for await (const line of rl) {
      const t = line.trim(); if (!t) continue;
      chunk.push(t);
      if (chunk.length >= 1024) { await handle(chunk); chunk = []; if (LIMIT && sum.seen >= LIMIT) break; }
    }
    if (chunk.length) await handle(chunk);
  } else {
    console.error(`${src.keys.length.toLocaleString()} object(s) under ${IN}`);
    for (let i = 0; i < src.keys.length; i += CONC) {
      await Promise.all(src.keys.slice(i, i + CONC).map(async (Key) => {
        try {
          const r = await src.s3.send(new src.GetObjectCommand({ Bucket: src.bucket, Key }));
          const ch = []; for await (const x of r.Body) ch.push(x);
          const urls = Buffer.concat(ch).toString('utf8').split(/\r?\n/).map((s) => s.trim()).filter(Boolean);
          for (let j = 0; j < urls.length; j += 1024) await handle(urls.slice(j, j + 1024));
        } catch (e) { sum.errors++; }
      }));
      if (LIMIT && sum.seen >= LIMIT) break;
      if ((i / CONC) % 20 === 0) {
        const rate = Math.round(sum.seen / Math.max(1, (Date.now() - t0) / 1000));
        console.error(`  ${Math.min(i + CONC, src.keys.length)}/${src.keys.length} objects | ${sum.seen.toLocaleString()} URLs | ${sum.written.toLocaleString()} written | ${rate}/s`);
      }
    }
  }
  await flush();
  try { await client.indices.refresh({ index: os.INDEX }); } catch (e) { /* */ }

  console.error(`\n══════ PRE-PROCESS DONE · ${Math.round((Date.now() - t0) / 1000)}s ══════${DRY ? ' [dry-run]' : ''}`);
  console.error(`  URLs seen              : ${sum.seen.toLocaleString()}`);
  console.error(`  already a contact      : ${sum.alreadyContact.toLocaleString()}`);
  console.error(`  URL unparseable        : ${sum.unparsed.toLocaleString()}`);
  console.error(`  records written        : ${sum.written.toLocaleString()} (${sum.skipped.toLocaleString()} skipped, ${sum.errors} error(s))`);
  console.error(`  company data attached  : ${sum.companyFilled.toLocaleString()}`);
  console.error(`  priority 1 gender+role : ${sum.byPriority[1].toLocaleString()}`);
  console.error(`  priority 2 gender only : ${sum.byPriority[2].toLocaleString()}`);
  console.error(`  priority 3 role only   : ${sum.byPriority[3].toLocaleString()}`);
  console.error(`  priority 4 neither     : ${sum.byPriority[4].toLocaleString()}`);
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
