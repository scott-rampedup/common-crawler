/**
 * team-scan.js — run team-extract over a list of team/contact listing pages.
 *
 *   node team-scan.js --list "<export>.team-pages.txt" --out <dir> [--limit N] [--conc 12] [--seed S]
 *
 * READ-ONLY. Writes three files and touches nothing else:
 *   team-people.ndjson   one person record per line
 *   team-bio-urls.txt    the per-person bio pages found inside cards — the by-product that feeds the
 *                        EXISTING one-page-one-person pipeline, kept separate from the export's own
 *                        bio-url list so the two sources stay measurable against each other
 *   team-scan.log        per-page meta + the run summary
 *
 * Nothing here upserts or queues. Loading these records is a separate, deliberate step: the whole
 * point of the two-witness binding is that we look at what it produced before trusting it.
 */
const fs = require('fs');
const path = require('path');
const ex = require('./extractor');
const cc = require('./cc-engine');
const te = require('./team-extract');

const arg = (f, d) => { const i = process.argv.indexOf(f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const LIST = arg('--list', '');
const OUT = arg('--out', '');
const LIMIT = Number(arg('--limit', '0')) || 0;
const CONC = Number(arg('--conc', '12')) || 12;
const SEED = Number(arg('--seed', '0')) || 0;
const TIMEOUT = Number(arg('--timeout', '25000')) || 25000;

if (!LIST || !OUT) { console.error('need --list <team-pages.txt> --out <dir>'); process.exit(1); }
fs.mkdirSync(OUT, { recursive: true });

const genderMap = ex.loadGenderMap(path.join(__dirname, 'names-genders.csv'));
const FIRSTS = new Set(Object.keys(genderMap).map((k) => String(k).toLowerCase()));
console.error(`first-name lexicon: ${FIRSTS.size.toLocaleString()} names`);

const N = (n) => Number(n || 0).toLocaleString();

async function fetchPage(url) {
  const race = (p) => Promise.race([p, new Promise((r) => setTimeout(() => r(''), TIMEOUT))]);
  let html = '';
  try { html = await race(cc.liveFetchPage(url)); } catch (e) { /* fall through to the other fetcher */ }
  // The two fetchers are blocked by different sites, so a miss on one is worth retrying on the other.
  if (!html || String(html).length < 500) {
    try { const alt = await race(cc.fetchDoc(url)); if (alt && String(alt).length > String(html || '').length) html = alt; }
    catch (e) { /* keep the first result */ }
  }
  html = String(html || '');
  return html.length >= 500 ? html : '';
}

(async () => {
  let urls = fs.readFileSync(LIST, 'utf8').split(/\r?\n/).map((s) => s.trim()).filter(Boolean);
  if (SEED) {
    let s = SEED;
    const rnd = () => { s = (s * 1103515245 + 12345) & 0x7fffffff; return s / 0x7fffffff; };
    const pool = urls.slice(); urls = [];
    while (pool.length && (!LIMIT || urls.length < LIMIT)) urls.push(pool.splice(Math.floor(rnd() * pool.length), 1)[0]);
  } else if (LIMIT) urls = urls.slice(0, LIMIT);

  console.error(`${N(urls.length)} team page(s) -> ${OUT}`);
  const peopleOut = fs.createWriteStream(path.join(OUT, 'team-people.ndjson'));
  const logOut = fs.createWriteStream(path.join(OUT, 'team-scan.log'));
  const bio = new Set();

  const t0 = Date.now();
  const stat = { pages: urls.length, fetched: 0, withNames: 0, people: 0, byMethod: {}, byConf: {},
    withEmail: 0, twoWitness: 0, withTitle: 0, withPhone: 0, withBio: 0, conflicts: 0,
    orgEmails: 0, orgPhones: 0, over25: 0, uncorroborated: 0 };
  let done = 0;
  const q = urls.slice();

  await Promise.all(Array.from({ length: CONC }, async () => {
    while (q.length) {
      const url = q.pop(); if (!url) break;
      let html = '';
      try { html = await fetchPage(url); } catch (e) { /* counted as unfetched below */ }
      if (++done % 100 === 0) console.error(`  ${N(done)}/${N(urls.length)} | ${N(stat.people)} people`);
      if (!html) { logOut.write(JSON.stringify({ url, ok: false }) + '\n'); continue; }
      stat.fetched++;
      let res;
      try { res = te.extractPeople(html, url, { firsts: FIRSTS }); }
      catch (e) { logOut.write(JSON.stringify({ url, ok: false, err: String(e.message || e).slice(0, 120) }) + '\n'); continue; }
      const { people, meta } = res;
      logOut.write(JSON.stringify({ ok: true, ...meta, emitted: people.length }) + '\n');
      if (meta.names >= 2) stat.withNames++;
      if (meta.names > 25) stat.over25++;
      stat.conflicts += meta.conflicts || 0;
      stat.uncorroborated += meta.uncorroborated || 0;
      stat.orgEmails += (meta.orgEmails || []).length;
      stat.orgPhones += (meta.orgPhones || []).length;
      for (const p of people) {
        stat.people++;
        stat.byMethod[p.binding] = (stat.byMethod[p.binding] || 0) + 1;
        stat.byConf[p.confidence] = (stat.byConf[p.confidence] || 0) + 1;
        if (p.email) stat.withEmail++;
        if (p.witnesses && p.witnesses.includes('signature')) stat.twoWitness++;
        if (p.title) stat.withTitle++;
        if (p.phone) stat.withPhone++;
        if (p.bio_url) { stat.withBio++; bio.add(p.bio_url); }
        peopleOut.write(JSON.stringify(p) + '\n');
      }
    }
  }));

  peopleOut.end(); logOut.end();
  fs.writeFileSync(path.join(OUT, 'team-bio-urls.txt'), [...bio].join('\n') + (bio.size ? '\n' : ''));
  fs.writeFileSync(path.join(OUT, 'team-bio-urls.meta.json'), JSON.stringify({
    sourceList: path.basename(LIST), pagesScanned: stat.fetched, count: bio.size,
    kind: 'per-person bio pages found inside team-page cards', queued: false,
    at: new Date().toISOString(),
  }, null, 1));

  const pct = (a, b) => (b ? (100 * a / b).toFixed(1) + '%' : '-');
  const L = [];
  L.push('');
  L.push(`pages                 : ${N(stat.pages)} | fetched ${N(stat.fetched)} (${pct(stat.fetched, stat.pages)})`);
  L.push(`rosters (>=2 names)   : ${N(stat.withNames)} (${pct(stat.withNames, stat.fetched)} of fetched) | >25 people: ${N(stat.over25)} (${pct(stat.over25, stat.withNames)})`);
  L.push(`people emitted        : ${N(stat.people)}  (${(stat.people / Math.max(1, stat.withNames)).toFixed(1)} per roster)`);
  L.push(`  by binding          : ${Object.entries(stat.byMethod).map(([k, v]) => k + ' ' + N(v)).join(' | ') || '-'}`);
  L.push(`  by confidence       : ${Object.entries(stat.byConf).map(([k, v]) => k + ' ' + N(v)).join(' | ') || '-'}`);
  L.push(`  with title          : ${N(stat.withTitle)} (${pct(stat.withTitle, stat.people)})`);
  L.push(`  with phone          : ${N(stat.withPhone)} (${pct(stat.withPhone, stat.people)})`);
  L.push(`  with email          : ${N(stat.withEmail)} (${pct(stat.withEmail, stat.people)}) | both witnesses ${N(stat.twoWitness)} (${pct(stat.twoWitness, stat.people)})`);
  L.push(`  with a bio URL      : ${N(stat.withBio)} (${pct(stat.withBio, stat.people)})`);
  L.push(`names dropped (bare)  : ${N(stat.uncorroborated)}  — no title, identifier or bio link`);
  L.push(`conflicts dropped     : ${N(stat.conflicts)} | org-level emails ${N(stat.orgEmails)} | org-level phones ${N(stat.orgPhones)}`);
  L.push(`distinct bio URLs     : ${N(bio.size)} -> team-bio-urls.txt (NOT queued)`);
  L.push(`${Math.round((Date.now() - t0) / 1000)}s`);
  const summary = L.join('\n');
  console.error(summary);
  fs.appendFileSync(path.join(OUT, 'team-scan.log'), summary + '\n');
})().catch((e) => { console.error('ERR', (e && e.stack) || e); process.exit(1); });
