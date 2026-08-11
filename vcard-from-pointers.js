/**
 * vcard-from-pointers.js — turn vCards into contacts DIRECTLY. No bio page, no scraping.
 *
 *   OPENSEARCH_ENDPOINT=… node vcard-from-pointers.js --ptr vcard-ptr.jsonl [--tag vcf] [--dry]
 *   OPENSEARCH_ENDPOINT=… node vcard-from-pointers.js --urls vcard-urls.txt          (live fetch)
 *
 * A .vcf is already a contact record: N/FN, TITLE, ORG, EMAIL, TEL (typed), ADR. Everything the bio-page
 * path has to *infer* — the name from a slug, the title from a curated list, the location from an area
 * code — the card simply states. So this path skips the entire extraction stack and its error modes.
 *
 * That is not a theoretical advantage. Contacts that happen to carry a vCard are far better than the
 * database average: 87.8% have a gender (vs 54.4%) and 59.5% have a phone (vs 21.2%).
 *
 * Sources, both already built:
 *   --ptr   WARC pointers from `cc-athena-miner --vcards --warc-out`  (S3-direct, no live traffic)
 *   --urls  a plain .vcf URL list, fetched live through the proxy      (e.g. vcard links we already store)
 *
 * The vCard's own URL becomes the contact's Web Source URL, and Source is "vCard".
 */
const fs = require('fs');
const readline = require('readline');
const path = require('path');
const vcard = require('./vcard');
const os = require('./opensearch');
const ex = require('./extractor');
const { makeCcS3 } = require('./cc-s3');
const { modelMissingEmails } = require('./email-model');
let ccEngine = null; try { ccEngine = require('./cc-engine'); } catch (e) { /* live mode optional */ }

const arg = (f, d) => { const i = process.argv.indexOf(f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const PTR = arg('--ptr', '');
const URLS = arg('--urls', '');
const TAG = arg('--tag', 'vcard');
const DRY = process.argv.includes('--dry');
const CONC = Number(process.env.CONC) || 32;
const CHUNK = Number(process.env.CHUNK) || 2000;

const genderMap = ex.loadGenderMap(path.join(__dirname, 'names-genders.csv'));

// The card -> record mapping lives in vcard.js so the Data Importer's vCard mode and this bulk path
// cannot drift apart.
const recordFromCard = (text, sourceUrl, nowIso) => vcard.recordFromCardText(text, sourceUrl, { genderMap, nowIso });

// A hand-exported list is rarely a clean one-URL-per-line file: it usually has a CSV header, may be
// quoted, and very often has no scheme (bdl.dk/wp-content/.../Name.vcf). Normalize rather than make the
// operator clean it up. Returns '' for anything that isn't plausibly a URL, which also drops the header.
function normalizeVcardUrl(raw) {
  let s = String(raw || '').trim().replace(/^["']|["'],?$/g, '').replace(/,$/, '').trim();
  if (!s) return '';
  if (s.includes(',')) s = s.split(',')[0].trim();          // first column of a wider CSV
  if (/^(vcards?|url|link|href)$/i.test(s)) return '';       // header row
  if (!/^https?:\/\//i.test(s)) {
    if (!/^[a-z0-9.-]+\.[a-z]{2,}(\/|$)/i.test(s)) return ''; // not a host -> not a URL
    s = 'https://' + s;
  }
  try { const u = new URL(s); if (u.protocol !== 'http:' && u.protocol !== 'https:') return ''; return u.href; }
  catch (e) { return ''; }
}

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  if (!PTR && !URLS) { console.error('need --ptr <pointers.jsonl> or --urls <list.txt>'); process.exit(1); }
  const client = os.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const nowIso = new Date().toISOString();
  const fetchWarc = PTR ? makeCcS3() : null;

  const jobs = [];
  const src = PTR || URLS;
  const seenUrl = new Set();
  const rl = readline.createInterface({ input: fs.createReadStream(src), crlfDelay: Infinity });
  for await (const line of rl) {
    const t = line.trim(); if (!t) continue;
    if (PTR) { try { const p = JSON.parse(t); if (p && p.url && p.filename) jobs.push(p); } catch (e) { /* skip */ } }
    else { const u = normalizeVcardUrl(t); if (u && !seenUrl.has(u)) { seenUrl.add(u); jobs.push({ url: u }); } }
  }
  console.error(`${jobs.length.toLocaleString()} vCard(s) to read from ${PTR ? 'Common Crawl (S3-direct)' : 'the live web'}${DRY ? '  [DRY RUN — no writes]' : ''}`);
  if (!jobs.length) process.exit(0);

  const t0 = Date.now();
  const tally = { read: 0, parsed: 0, noName: 0, unreachable: 0, records: 0, gendered: 0, withPhone: 0, withEmail: 0, modelled: 0, upserted: 0, errors: 0 };
  let pending = [];

  async function flush() {
    if (!pending.length) return;
    const recs = pending; pending = [];
    // Cards without an email but WITH a name+gender can still be modelled from the company's pattern.
    try { tally.modelled += await modelMissingEmails(recs, { dbQuery: async () => [] }); } catch (e) { /* best-effort */ }
    const docs = recs.map((r) => os.recordToDoc(r, nowIso)).filter((d) => d && d.first && d.last && d.email);
    tally.records += docs.length;
    if (DRY) return;
    for (let i = 0; i < docs.length; i += 2000) {
      try { await os.bulkUpsert(client, docs.slice(i, i + 2000)); tally.upserted += Math.min(2000, docs.length - i); }
      catch (e) { tally.errors += Math.min(2000, docs.length - i); }
    }
  }

  let idx = 0;
  const worker = async () => {
    for (;;) {
      const k = idx++; if (k >= jobs.length) return;
      const j = jobs[k];
      let text = '';
      try {
        text = PTR ? await fetchWarc(j) : (ccEngine ? await ccEngine.fetchDoc(j.url) : '');
      } catch (e) { tally.errors++; continue; }
      tally.read++;
      const rec = recordFromCard(text, j.url, nowIso);
      if (!rec) { if (text && /BEGIN:VCARD/i.test(text)) tally.noName++; continue; }
      tally.parsed++;
      if (rec['Gender']) tally.gendered++;
      if (rec['Phone']) tally.withPhone++;
      if (rec['Email Address']) tally.withEmail++;
      pending.push(rec);
      if (pending.length >= CHUNK) await flush();
      if (tally.read % 5000 === 0) console.error(`  read ${tally.read.toLocaleString()}/${jobs.length.toLocaleString()} | contacts ${tally.parsed.toLocaleString()} | ${Math.round(tally.read / ((Date.now() - t0) / 1000))}/s`);
    }
  };
  await Promise.all(Array.from({ length: Math.min(CONC, jobs.length) }, worker));
  await flush();

  const pct = (n) => (tally.parsed ? ((n / tally.parsed) * 100).toFixed(1) + '%' : '0%');
  console.error(`\nDONE${DRY ? ' [DRY — nothing written]' : ''}: read ${tally.read.toLocaleString()} card(s) -> ${tally.parsed.toLocaleString()} person record(s)`);
  console.error(`  with a gender : ${tally.gendered.toLocaleString()} (${pct(tally.gendered)})`);
  console.error(`  with a phone  : ${tally.withPhone.toLocaleString()} (${pct(tally.withPhone)})`);
  console.error(`  with an email : ${tally.withEmail.toLocaleString()} (${pct(tally.withEmail)})`);
  console.error(`  emails modelled ${tally.modelled.toLocaleString()} | dropped ${tally.noName.toLocaleString()} no-person-name`);
  console.error(`  -> ${tally.records.toLocaleString()} upsertable${DRY ? '' : `, ${tally.upserted.toLocaleString()} upserted, ${tally.errors} error(s)`} | ${Math.round((Date.now() - t0) / 1000)}s`);
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
