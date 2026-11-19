/**
 * infer-domain-from-email.js — recover the employer domain from the mailbox when the record has none.
 *
 * Clearing the hosting platforms (fix-contact-domains.js) left ~1.6M contacts with no employer at all.
 * For most of them the employer was never lost — it is sitting in the address. `jdhayworth@clearchannel.com`
 * had facebook.com as its domain because the bio page was a Facebook page; Clear Channel is plainly the
 * employer and the mailbox says so.
 *
 * Measured on a random 10,000 of the domainless population (2026-11-19): 67.8% have a mailbox on a real
 * company host, and 73.5% of THOSE (weighted by contacts) have a matching record in the companies index.
 *
 * This script does exactly one thing: set `domain`. Everything after it already exists —
 *   enrich-firmographics.js   joins domain -> companies index for name / industry / size / HQ / founded
 *   company-name-waterfall.js resolves a real company NAME where the firmographic join found none
 * so this deliberately does NOT write `company` or `company_name`. Writing the domain into the name
 * field is the exact defect company-name-waterfall.js exists to undo.
 *
 * WHICH ADDRESSES COUNT. The test is employerDomainFor() — the same function the write path and
 * fix-contact-domains.js use — so a mailbox on a freemail or platform host infers nothing. Note this is
 * broader than classifyEmail()'s "Professional": a ROLE address still identifies the company, because
 * info@clearchannel.com is as much evidence of Clear Channel as a named mailbox is. It is evidence about
 * the employer, not about the person, and only the employer field is being written here.
 *
 * The host is reduced to its registrable domain via the PSL, so mail.acme.co.uk files under acme.co.uk
 * and a tenant on a shared host stays separate.
 *
 *   OPENSEARCH_ENDPOINT=… node infer-domain-from-email.js            # dry run
 *   OPENSEARCH_ENDPOINT=… node infer-domain-from-email.js --apply
 *   OPENSEARCH_ENDPOINT=… node infer-domain-from-email.js --selftest
 */
const os = require('./opensearch');
const psl = require('./psl-suffix');
const { employerDomainFor, hostOfEmail } = require('./non-company-domains');

const APPLY = process.argv.includes('--apply');
const arg = (f, d) => { const i = process.argv.indexOf(f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const LIMIT = Number(arg('--limit', '0')) || 0;
const N = (x) => Number(x).toLocaleString();

// The employer domain implied by the address, or '' when the address implies nothing.
function domainFromEmail(email) {
  const host = hostOfEmail(email);
  if (!host) return '';
  const root = psl.registrableOrHost(host) || host;
  return employerDomainFor(root, email);
}

module.exports = { domainFromEmail };

if (require.main === module && process.argv.includes('--selftest')) {
  let pass = 0, fail = 0;
  const eq = (l, g, w) => { if (g === w) { pass++; return; } fail++; console.error('  FAIL ' + l + ': got ' + JSON.stringify(g) + ' want ' + JSON.stringify(w)); };
  eq('the motivating case', domainFromEmail('jdhayworth@clearchannel.com'), 'clearchannel.com');
  eq('freemail infers nothing', domainFromEmail('leehuangshi@gmail.com'), '');
  eq('yahoo infers nothing', domainFromEmail('CptnJarhead@yahoo.com'), '');
  // A mailbox ON the platform is the one case where the platform IS the employer.
  eq('facebook staffer infers facebook', domainFromEmail('someone@facebook.com'), 'facebook.com');
  eq('role address still names the company', domainFromEmail('info@clearchannel.com'), 'clearchannel.com');
  eq('subdomain reduced to registrable', domainFromEmail('x@mail.acme.co.uk'), 'acme.co.uk');
  eq('multi-label public suffix kept whole', domainFromEmail('x@team.example.co.uk'), 'example.co.uk');
  eq('empty address', domainFromEmail(''), '');
  eq('malformed address', domainFromEmail('not-an-address'), '');
  eq('case folded', domainFromEmail('X@Acme.COM'), 'acme.com');
  console.log('infer-domain-from-email selftest: ' + pass + ' passed, ' + fail + ' failed');
  process.exit(fail ? 1 : 0);
}

if (require.main === module) (async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  const client = os.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const INDEX = os.INDEX;

  // Missing OR empty: `domain` is a keyword that the write path sets to '' rather than omitting, so
  // exists-alone would match every cleared row and find nothing to do.
  const query = { bool: {
    should: [{ term: { domain: '' } }, { bool: { must_not: [{ exists: { field: 'domain' } }] } }],
    minimum_should_match: 1,
    filter: [{ exists: { field: 'email' } }] } };

  const total = (await client.count({ index: INDEX, body: { query } })).body.count;
  console.error(`contacts with no employer domain: ${N(total)} | mode: ${APPLY ? 'APPLY' : 'DRY RUN'}`);

  let scanned = 0, inferred = 0, noneFromMailbox = 0, batch = [];
  const byDomain = new Map();
  const samples = [];
  const flush = async () => {
    if (APPLY && batch.length) {
      const r = await client.bulk({ body: batch, refresh: false });
      const b = r.body || r;
      if (b.errors) {
        for (const i of (b.items || []).filter((x) => x.update && x.update.error).slice(0, 2)) {
          console.error('  bulk error: ' + JSON.stringify(i.update.error).slice(0, 200));
        }
      }
    }
    batch = [];
  };

  let resp = await client.search({ index: INDEX, scroll: '5m', size: 2000, _source: ['email'], body: { query } });
  let sid = (resp.body || resp)._scroll_id;
  for (;;) {
    const hits = (resp.body || resp).hits.hits;
    if (!hits.length) break;
    for (const h of hits) {
      if (LIMIT && scanned >= LIMIT) break;
      scanned++;
      const dom = domainFromEmail((h._source || {}).email || '');
      if (!dom) { noneFromMailbox++; continue; }
      inferred++;
      byDomain.set(dom, (byDomain.get(dom) || 0) + 1);
      if (samples.length < 6) samples.push({ email: (h._source || {}).email, domain: dom });
      batch.push({ update: { _index: INDEX, _id: h._id } }, { doc: { domain: dom } });
    }
    if (batch.length >= 4000) await flush();
    if (scanned % 100000 < 2000) console.error(`  scanned ${N(scanned)} | ${APPLY ? 'set' : 'would set'} ${N(inferred)}`);
    if (LIMIT && scanned >= LIMIT) break;
    resp = await client.scroll({ scroll_id: sid, scroll: '5m' });
    sid = (resp.body || resp)._scroll_id;
  }
  await flush();
  try { await client.clearScroll({ body: { scroll_id: [sid] } }); } catch (e) { /* already expired */ }

  console.error('\nsample inferences:');
  for (const s of samples) console.error('  ' + JSON.stringify(s));
  console.error('\nmost common recovered employers:');
  for (const [d, n] of [...byDomain.entries()].sort((a, b) => b[1] - a[1]).slice(0, 12)) {
    console.error('  ' + d.padEnd(32) + N(n));
  }
  console.error(`\nDONE: scanned ${N(scanned)} | ${APPLY ? 'set' : 'would set'} a domain on ${N(inferred)}`
    + ` | mailbox implied nothing for ${N(noneFromMailbox)} | distinct employers ${N(byDomain.size)}`
    + `${APPLY ? '\nNEXT: enrich-firmographics.js, then company-name-waterfall.js' : '  (dry run — add --apply)'}`);
})().catch((e) => { console.error('ERR', e.message); process.exit(1); });
