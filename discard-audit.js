/**
 * discard-audit.js — measure what the extraction pipeline THROWS AWAY, and what it is worth.
 *
 *   OPENSEARCH_ENDPOINT=… node discard-audit.js --discover 20000
 *   OPENSEARCH_ENDPOINT=… node discard-audit.js --ptr /tmp/sample.jsonl
 *
 * extract-from-pointers keeps a record only if it survives BOTH gates:
 *
 *   (a) looksLikePerson(First, Last)          — drops anything without a person-shaped name
 *   (b) .filter(d => d.first && d.last && d.email)
 *
 * Under a named-contact product that is correct. Under a hashed-email (HEM) product it is not: a page
 * with a good personal address and no parseable name is inventory, and today it is discarded without ever
 * being counted — the run summary lumps "no email" and "no name" into one dropNoEmail number.
 *
 * This fetches a real sample, runs the real extractRecord, and buckets every outcome so the size of that
 * blind spot is a measured number rather than an assumption. It writes nothing.
 */
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const { spawnSync } = require('child_process');
const extractor = require('./extractor');
const { makeCcS3 } = require('./cc-s3');

const arg = (n, d) => { const i = process.argv.indexOf('--' + n); return i > 0 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const DISCOVER = Number(arg('discover', '0')) || 0;
let PTR = arg('ptr', '/tmp/discard-sample.jsonl');
const CONC = Number(process.env.CONC) || 24;

// Same person-shape gate extract-from-pointers applies (copied, not imported — that file is a CLI).
const JUNK_NAME = new Set(['contact', 'form', 'general', 'enquiry', 'enquiries', 'administration', 'team', 'about',
  'service', 'services', 'news', 'privacy', 'estate', 'probate', 'school', 'get', 'meet', 'our', 'home', 'blog',
  'careers', 'welcome', 'page', 'search', 'menu', 'login', 'sign', 'subscribe', 'newsletter', 'cookie', 'cookies',
  'terms', 'policy', 'the', 'and', 'staff', 'people', 'profile', 'profiles', 'directory', 'overview', 'summary']);
const NAME_RE = /^[a-zà-ÿ][a-zà-ÿ'’.-]{1,23}$/i;
const looksLikePerson = (f, l) => {
  f = String(f || '').trim(); l = String(l || '').trim();
  if (!f || !l) return false;
  if (!NAME_RE.test(f) || !NAME_RE.test(l)) return false;
  return !(JUNK_NAME.has(f.toLowerCase()) || JUNK_NAME.has(l.toLowerCase()));
};

(async () => {
  const genderMap = extractor.loadGenderMap(path.join(__dirname, 'names-genders.csv'));
  let wireless = null;
  try { const wbc = require('./wireless-block-classifier'); wireless = wbc.loadWirelessBlocks(wbc.PHONE_BLOCKS_CSV); } catch (e) { /* phones blank */ }

  if (DISCOVER) {
    console.error(`discovering a ${DISCOVER.toLocaleString()}-page sample…`);
    const r = spawnSync(process.execPath, [path.join(__dirname, 'cc-athena-miner.js'),
      '--bio-terms-file', path.join(__dirname, 'data', 'bio-path-terms.txt'),
      '--per-domain', '3', '--limit', String(DISCOVER), '--warc-out', PTR], { stdio: 'inherit', cwd: __dirname });
    if (r.status !== 0) process.exit(r.status || 1);
  }
  if (!fs.existsSync(PTR)) { console.error('no pointer file — pass --ptr or --discover N'); process.exit(1); }

  const jobs = [];
  const rl = readline.createInterface({ input: fs.createReadStream(PTR), crlfDelay: Infinity });
  for await (const l of rl) { if (!l.trim()) continue; try { const p = JSON.parse(l); if (p.url && p.filename) jobs.push(p); } catch (e) { /* */ } }
  console.error(`\nauditing ${jobs.length.toLocaleString()} page(s)…\n`);

  const fetchWarc = makeCcS3();
  const t = { pages: 0, noHtml: 0, noRecord: 0, kept: 0, emailNoName: 0, nameNoEmail: 0, neither: 0 };
  const emailTypes = {};            // for the email-but-no-name bucket — the HEM inventory
  const nameNoEmailGendered = { yes: 0, no: 0 };
  let i = 0;

  const worker = async () => {
    for (;;) {
      const k = i++; if (k >= jobs.length) return;
      let html = '';
      try { html = await fetchWarc(jobs[k]); } catch (e) { continue; }
      t.pages++;
      if (!html) { t.noHtml++; continue; }
      let rec = null;
      try { rec = extractor.extractRecord(html, jobs[k].url, { wireless, genderMap, directoryRules: {}, source: 'Audit', allowNoEmail: true }); }
      catch (e) { continue; }
      if (!rec) { t.noRecord++; continue; }
      const email = String(rec['Email Address'] || '').trim();
      const named = looksLikePerson(rec['First'], rec['Last']);
      if (email && named) { t.kept++; continue; }
      if (email && !named) {
        t.emailNoName++;
        const ty = extractor.classifyEmail(email) || '(none)';
        emailTypes[ty] = (emailTypes[ty] || 0) + 1;
        continue;
      }
      if (!email && named) {
        t.nameNoEmail++;
        if (String(rec['Gender'] || '').trim()) nameNoEmailGendered.yes++; else nameNoEmailGendered.no++;
        continue;
      }
      t.neither++;
      if (t.pages % 2000 === 0) console.error(`  ${t.pages.toLocaleString()} pages | kept ${t.kept.toLocaleString()} | email-no-name ${t.emailNoName.toLocaleString()}`);
    }
  };
  const t0 = Date.now();
  await Promise.all(Array.from({ length: Math.min(CONC, jobs.length) }, worker));

  const N = (n) => Number(n || 0).toLocaleString();
  const pct = (n) => ((n / Math.max(1, t.pages)) * 100).toFixed(1) + '%';
  console.log(`\n=== ${N(t.pages)} pages fetched in ${Math.round((Date.now() - t0) / 1000)}s ===`);
  console.log(`  no html / gone            ${N(t.noHtml).padStart(9)}  ${pct(t.noHtml)}`);
  console.log(`  extractRecord -> null     ${N(t.noRecord).padStart(9)}  ${pct(t.noRecord)}`);
  console.log(`  KEPT today (name+email)   ${N(t.kept).padStart(9)}  ${pct(t.kept)}`);
  console.log(`  DROPPED, email no name    ${N(t.emailNoName).padStart(9)}  ${pct(t.emailNoName)}   <- HEM inventory`);
  console.log(`  DROPPED, name no email    ${N(t.nameNoEmail).padStart(9)}  ${pct(t.nameNoEmail)}   <- modellable (FLOOR, see below)`);
  console.log(`  DROPPED, neither          ${N(t.neither).padStart(9)}  ${pct(t.neither)}`);
  console.log(`\n  the email-no-name bucket, by type:`);
  for (const k of Object.keys(emailTypes).sort((a, b) => emailTypes[b] - emailTypes[a])) {
    console.log(`    ${k.padEnd(14)} ${N(emailTypes[k]).padStart(8)}   ${((emailTypes[k] / Math.max(1, t.emailNoName)) * 100).toFixed(1)}%`);
  }
  const hem = (emailTypes.Professional || 0) + (emailTypes.Personal || 0);
  console.log(`    -> person-level (Professional+Personal): ${N(hem)}  ${((hem / Math.max(1, t.pages)) * 100).toFixed(1)}% of all pages`);
  // Caveat worth stating rather than letting the reader over-read the number: under allowNoEmail,
  // extractRecord only RETURNS an email-less record when isBio && first && last && gender. An email-less
  // page whose name carries no gender returns null and lands in the noRecord bucket above, so
  // name-no-email is a floor, not the true total. The email-no-name bucket has no such gate — a record
  // with an email always comes back — which is the number this audit exists to produce.
  console.log(`\n  the name-no-email bucket: ${N(nameNoEmailGendered.yes)} gendered / ${N(nameNoEmailGendered.no)} not.`);
  console.log(`  This bucket is a FLOOR: extractRecord only returns an email-less record when it already has`);
  console.log(`  a gendered name, so ungendered ones are counted under "extractRecord -> null" instead.`);
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
