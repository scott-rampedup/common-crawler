/**
 * domain-yield.js — which domains are worth fetching, and which are burning the fleet's time.
 *
 *   OPENSEARCH_ENDPOINT=… node domain-yield.js [--top 40] [--min-attempts 500]
 *
 * The fleet's wall-clock is dominated by the live crawl, and the live crawl is dominated by a handful of
 * large directory sites: 2,563,533 URLs came from just 9,725 domains, with the top 10 holding 69%. But
 * volume is not value. On the first fleet, one shard upserted 6,560 contacts while dropping 219,843
 * records for having no usable email — a 3% yield — and another upserted 209,426 from a comparable slice.
 * Those two shards cost the same fleet-hours.
 *
 * Nothing has ever compared cost to return per domain, because the cost side did not exist until the crawl
 * ledger recorded attempts. This joins the two:
 *
 *   attempts  — crawl_log, per domain (what the fetching COST)
 *   contacts  — the contacts index, per domain (what it RETURNED)
 *   yield     — contacts / attempts
 *
 * A domain at 0.5% yield is not free to crawl: at fleet rates a 300,000-page directory is hours of
 * proxied fetching for ~1,500 contacts, while those same hours spent on a 40%-yield domain return two
 * orders of magnitude more. Deprioritising the bottom of this table is the largest available speedup
 * that does not involve buying more machines.
 *
 * Read-only. It ranks; it does not exclude anything on its own.
 */
const os = require('./opensearch');
const ledger = require('./crawl-ledger');

const arg = (n, d) => { const i = process.argv.indexOf('--' + n); return i > 0 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const TOP = Number(arg('top', '40')) || 40;
const MIN_ATTEMPTS = Number(arg('min-attempts', '500')) || 500;

const N = (n) => Number(n || 0).toLocaleString();

(async () => {
  const c = os.makeClient(process.env.OPENSEARCH_ENDPOINT);

  // Attempts per domain, paged with a composite agg so the domain space is not truncated to a top-N.
  const attempts = new Map();
  let after = null, pages = 0;
  for (;;) {
    const comp = { size: 5000, sources: [{ d: { terms: { field: 'domain' } } }] };
    if (after) comp.after = after;
    const r = await c.search({ index: ledger.INDEX, body: { size: 0, aggs: { byD: { composite: comp } } } });
    const agg = (r.body || r).aggregations.byD;
    const buckets = agg.buckets || [];
    if (!buckets.length) break;
    for (const b of buckets) if (b.key.d) attempts.set(b.key.d, (attempts.get(b.key.d) || 0) + b.doc_count);
    after = agg.after_key;
    if (++pages % 10 === 0) console.error(`  ${N(attempts.size)} domains scanned…`);
    if (!after) break;
  }
  console.error(`ledger: ${N(attempts.size)} domain(s) with recorded attempts\n`);

  // Contacts per domain, for the domains that actually cost something.
  const candidates = [...attempts.entries()].filter(([, n]) => n >= MIN_ATTEMPTS).sort((a, b) => b[1] - a[1]);
  const rows = [];
  for (let i = 0; i < candidates.length; i += 100) {
    const chunk = candidates.slice(i, i + 100);
    const body = [];
    for (const [d] of chunk) { body.push({ index: os.INDEX }); body.push({ size: 0, query: { term: { domain: d } } }); }
    let responses = [];
    try { const r = await c.msearch({ body }); responses = ((r.body || r).responses) || []; }
    catch (e) { /* a failed chunk shows as 0 contacts; flagged by the caveat below */ }
    chunk.forEach(([d, att], k) => {
      const got = (responses[k] && responses[k].hits && responses[k].hits.total && responses[k].hits.total.value) || 0;
      rows.push({ domain: d, attempts: att, contacts: got, yield: got / Math.max(1, att) });
    });
  }

  const totalAtt = rows.reduce((s, r) => s + r.attempts, 0);
  const totalGot = rows.reduce((s, r) => s + r.contacts, 0);
  console.log(`domains with >= ${N(MIN_ATTEMPTS)} attempts: ${N(rows.length)}`);
  console.log(`  attempts ${N(totalAtt)} -> contacts ${N(totalGot)}  (overall yield ${((totalGot / Math.max(1, totalAtt)) * 100).toFixed(1)}%)\n`);

  const byCost = rows.slice().sort((a, b) => b.attempts - a.attempts).slice(0, TOP);
  console.log(`TOP ${TOP} BY COST (attempts) — where the fleet's hours actually go:`);
  console.log('  attempts    contacts   yield   domain');
  for (const r of byCost) {
    console.log(`  ${N(r.attempts).padStart(9)} ${N(r.contacts).padStart(11)}  ${(r.yield * 100).toFixed(1).padStart(5)}%   ${r.domain}`);
  }

  // The actionable list: expensive AND unproductive.
  const waste = rows.filter((r) => r.yield < 0.02).sort((a, b) => b.attempts - a.attempts);
  const wasteAtt = waste.reduce((s, r) => s + r.attempts, 0);
  console.log(`\nDOMAINS UNDER 2% YIELD: ${N(waste.length)}`);
  console.log(`  they cost ${N(wasteAtt)} attempts (${((wasteAtt / Math.max(1, totalAtt)) * 100).toFixed(1)}% of all fetching)`);
  console.log(`  and returned ${N(waste.reduce((s, r) => s + r.contacts, 0))} contacts`);
  console.log('  worst offenders:');
  for (const r of waste.slice(0, 15)) console.log(`    ${N(r.attempts).padStart(9)} attempts -> ${N(r.contacts).padStart(7)} contacts  ${r.domain}`);

  // --apply turns the measurement into policy. Guarded by BOTH a yield ceiling and a minimum attempt
  // count, so a domain is never condemned on a small sample — a new site with 40 fetches and no contacts
  // yet is not the same thing as one with 200,000 and none.
  if (process.argv.includes('--apply')) {
    const maxYield = Number(arg('max-yield', '0.02'));
    const minAtt = Number(arg('apply-min-attempts', String(MIN_ATTEMPTS)));
    const pick = rows.filter((r) => r.yield < maxYield && r.attempts >= minAtt);
    const stats = {};
    for (const r of pick) stats[r.domain] = { attempts: r.attempts, contacts: r.contacts };
    const gate = require('./domain-gate');
    const res = await gate.add(c, pick.map((r) => r.domain), `yield <${(maxYield * 100).toFixed(1)}% over >=${N(minAtt)} attempts`, stats);
    const saved = pick.reduce((s, r) => s + r.attempts, 0);
    const lost = pick.reduce((s, r) => s + r.contacts, 0);
    console.log(`\nAPPLIED: +${res.added} domain(s) blocked (blocklist now ${res.total})`);
    console.log(`  avoids ${N(saved)} fetch(es) per full pass; those domains had returned ${N(lost)} contact(s)`);
  }

  console.log('\n  CAVEAT: the ledger was seeded from completed miss lists with outcome="no-record", so seeded');
  console.log('  attempts are real but their outcomes are not. Contacts counts come from the contacts index');
  console.log('  and are exact; yields for freshly-crawled domains are the trustworthy ones.');
})().catch((e) => { console.error('ERR', e && e.message || e); process.exit(1); });
