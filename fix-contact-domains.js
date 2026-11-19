/**
 * fix-contact-domains.js — clear employer domains on CONTACTS that are really just hosting platforms.
 *
 * The contacts path sets `domain` from the host of the bio page it scraped, and nothing ever applied the
 * non-company host list to it (that guard only ran on the companies index and the Google Maps / ATP /
 * Bing loaders). Measured 2026-11-19: 1,038,262 contacts carried a platform as their employer, of which
 * facebook.com alone was 1,016,897 — and only 586 of those had an @facebook.com mailbox.
 *
 * The record is KEPT and only the employer fields are cleared, which is the same treatment
 * fix-bad-domains.js gives the companies index. The email, name and gender are not in question here;
 * one wrong field is no reason to destroy 100,816 genders.
 *
 * Which rows are cleared is decided by employerDomainFor() in non-company-domains.js, so this backfill
 * and the write path can never drift apart: freemail is always wrong, a platform is wrong unless the
 * person's own mailbox is on it (that test keeps 3,330 real @linklaters.com and 48 @wix.com contacts).
 *
 * Deploy the opensearch.js guard BEFORE running this, or os-sync will keep writing fresh junk behind it.
 *
 *   OPENSEARCH_ENDPOINT=… node fix-contact-domains.js            # dry run, reports what it would clear
 *   OPENSEARCH_ENDPOINT=… node fix-contact-domains.js --apply
 */
const os = require('./opensearch');
const { NON_COMPANY_DOMAINS, FREEMAIL_PREFIXES, employerDomainFor, normHost } = require('./non-company-domains');

const APPLY = process.argv.includes('--apply');
const HOSTS = [...NON_COMPANY_DOMAINS];
const N = (x) => Number(x).toLocaleString();

// What this contact's employer fields SHOULD be. Returns null when nothing needs changing, so an
// unchanged row never costs a write.
function repair(s) {
  const wantDomain = employerDomainFor(s.domain, s.email);
  let wantCompany = s.company || '';
  if (!wantDomain && normHost(wantCompany) === normHost(s.domain)) {
    wantCompany = '';                       // company was only ever a placeholder for the domain
  } else if (wantCompany && NON_COMPANY_DOMAINS.has(normHost(wantCompany))
             && !employerDomainFor(wantCompany, s.email)) {
    wantCompany = '';                       // company independently holds a rejected host as a name
  }
  const domainChanged = wantDomain !== (s.domain || '');
  const companyChanged = wantCompany !== (s.company || '');
  if (!domainChanged && !companyChanged) return null;
  const doc = {};
  if (domainChanged) doc.domain = wantDomain;
  if (companyChanged) doc.company = wantCompany;
  return doc;
}

module.exports = { repair };

if (require.main === module && process.argv.includes('--selftest')) {
  let pass = 0, fail = 0;
  const eq = (label, got, want) => {
    const g = JSON.stringify(got), w = JSON.stringify(want);
    if (g === w) { pass++; return; }
    fail++; console.error('  FAIL ' + label + ': got ' + g + ' want ' + w);
  };
  eq('facebook placeholder, gmail mailbox',
    repair({ email: 'j@gmail.com', domain: 'facebook.com', company: 'facebook.com' }),
    { domain: '', company: '' });
  eq('real facebook staffer is left alone',
    repair({ email: 'j@facebook.com', domain: 'facebook.com', company: 'facebook.com' }), null);
  eq('gmail as employer is always wrong',
    repair({ email: 'j@gmail.com', domain: 'gmail.com', company: 'gmail.com' }), { domain: '', company: '' });
  eq('real company name survives a rejected host',
    repair({ email: 'j@gmail.com', domain: 'facebook.com', company: 'Acme Widgets Ltd' }), { domain: '' });
  eq('rejected host sitting in company only',
    repair({ email: 'j@gmail.com', domain: '', company: 'easyleadz.com' }), { company: '' });
  eq('ordinary employer untouched',
    repair({ email: 'm@valencianm.gov', domain: 'valencianm.gov', company: 'valencianm.gov' }), null);
  eq('linklaters untouched',
    repair({ email: 'p@linklaters.com', domain: 'linklaters.com', company: 'Linklaters LLP' }), null);
  eq('already clear needs no write', repair({ email: 'j@gmail.com', domain: '', company: '' }), null);
  eq('www-prefixed placeholder',
    repair({ email: 'j@gmail.com', domain: 'www.facebook.com', company: 'www.facebook.com' }),
    { domain: '', company: '' });
  console.log('fix-contact-domains selftest: ' + pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
}

if (require.main === module) (async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  const client = os.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const INDEX = os.INDEX;

  // Either field may carry the host. A terms query stays cheap at this width; the per-domain wildcard
  // form blew the search heap when it was tried as one 37-clause bool.
  // Either field may carry the host, and the freemail FAMILY rule matches hosts that are nowhere
  // spelled out (yahoo.es, hotmail.fr, outlook.com.br), so a terms list alone would never find them.
  // Prefix queries on a keyword field are cheap; it was the LEADING-wildcard form that blew the search
  // heap when this was first attempted.
  const should = [{ terms: { domain: HOSTS } }, { terms: { 'company.kw': HOSTS } }];
  for (const p of FREEMAIL_PREFIXES) {
    should.push({ prefix: { domain: p } }, { prefix: { 'company.kw': p } });
  }
  const query = { bool: { should, minimum_should_match: 1 } };

  const total = (await client.count({ index: INDEX, body: { query } })).body.count;
  console.error(`contacts holding a non-company host: ${N(total)} | mode: ${APPLY ? 'APPLY' : 'DRY RUN'}`);

  let scanned = 0, cleared = 0, kept = 0, batch = [];
  const byHost = new Map();
  const samples = [];
  const flush = async () => {
    if (APPLY && batch.length) {
      const r = await client.bulk({ body: batch, refresh: false });
      const b = r.body || r;
      if (b.errors) {
        const bad = (b.items || []).filter((i) => i.update && i.update.error).slice(0, 2);
        for (const i of bad) console.error('  bulk error: ' + JSON.stringify(i.update.error).slice(0, 200));
      }
    }
    batch = [];
  };

  let resp = await client.search({ index: INDEX, scroll: '5m', size: 2000,
    _source: ['domain', 'company', 'email'], body: { query } });
  let sid = (resp.body || resp)._scroll_id;
  for (;;) {
    const hits = (resp.body || resp).hits.hits;
    if (!hits.length) break;
    for (const h of hits) {
      scanned++;
      const s = h._source || {};
      const doc = repair(s);
      if (!doc) { kept++; continue; }
      cleared++;
      const host = normHost(s.domain) || normHost(s.company);
      byHost.set(host, (byHost.get(host) || 0) + 1);
      if (samples.length < 6) samples.push({ email: s.email, was: s.domain, company: s.company });
      batch.push({ update: { _index: INDEX, _id: h._id } }, { doc });
    }
    if (batch.length >= 4000) await flush();
    if (scanned % 100000 < 2000) {
      console.error(`  scanned ${N(scanned)} | ${APPLY ? 'cleared' : 'would clear'} ${N(cleared)} | kept ${N(kept)}`);
    }
    resp = await client.scroll({ scroll_id: sid, scroll: '5m' });
    sid = (resp.body || resp)._scroll_id;
  }
  await flush();
  try { await client.clearScroll({ body: { scroll_id: [sid] } }); } catch (e) { /* scroll already gone */ }

  console.error('\nsample rows:');
  for (const x of samples) console.error('  ' + JSON.stringify(x));
  console.error('\nper host (top 15):');
  for (const [h, n] of [...byHost.entries()].sort((a, b) => b[1] - a[1]).slice(0, 15)) {
    console.error('  ' + String(h || '(blank)').padEnd(24) + N(n));
  }
  console.error(`\nDONE: scanned ${N(scanned)} | ${APPLY ? 'cleared' : 'would clear'} ${N(cleared)}`
    + ` | kept (mailbox matches the host) ${N(kept)}${APPLY ? '' : '  (dry run — add --apply)'}`);
})().catch((e) => { console.error('ERR', e.message); process.exit(1); });
