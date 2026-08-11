/**
 * build-email-patterns.js — learn each company's email pattern ONCE, from the Professional addresses
 * already in the Master DB, and store it on the company so every future model starts from evidence.
 *
 *   OPENSEARCH_ENDPOINT=… node build-email-patterns.js --dry [--limit N]
 *   OPENSEARCH_ENDPOINT=… node build-email-patterns.js [--min-samples 2] [--min-agree 0.6]
 *
 * WHY: email-model currently re-derives a pattern per batch from whatever samples happen to be in hand,
 * and falls back to {first}.{last} when a batch has none. With 6.9M Professional addresses already
 * stored, one offline pass can compute a confident pattern per domain and write it to the company's
 * email_pattern / email_domain — which modelMissingEmails already prefers above everything else. Every
 * later run then starts from what the company actually does instead of a guess.
 *
 * SAMPLE QUALITY: only Professional addresses on contacts that HAVE a gender are used. Gender is the
 * pipeline's real-person signal, so it filters out the role inboxes and junk rows whose "names" would
 * otherwise vote for nonsense templates. Modelled addresses are excluded outright — learning a pattern
 * from addresses we ourselves synthesized would just launder the {first}.{last} default into a stored
 * fact and make it permanent.
 */
const os = require('./opensearch');
const co = require('./companies');
const { templateFor, TEMPLATES } = require('./email-pattern');

const arg = (f, d) => { const i = process.argv.indexOf(f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const DRY = process.argv.includes('--dry');
const LIMIT = Number(arg('--limit', '0')) || 0;
const MIN_SAMPLES = Math.max(1, Number(arg('--min-samples', '2')) || 2);
const MIN_AGREE = Math.min(1, Math.max(0.34, Number(arg('--min-agree', '0.6')) || 0.6));
const PAGE = Number(process.env.PAGE || 5000);

(async () => {
  if (!process.env.OPENSEARCH_ENDPOINT) { console.error('need OPENSEARCH_ENDPOINT'); process.exit(1); }
  const client = os.makeClient(process.env.OPENSEARCH_ENDPOINT);
  const coClient = co.makeClient(process.env.OPENSEARCH_ENDPOINT);

  // Real, human-verified-shaped evidence only: Professional (not Modelled, not Role-Based, not Personal)
  // AND a gender present.
  const QUERY = {
    bool: {
      filter: [{ term: { email_type: 'Professional' } }],
      must_not: [{ term: { gender: '' } }, { term: { 'first.kw': '' } }, { term: { 'last.kw': '' } }],
    },
  };
  const total = (await client.count({ index: os.INDEX, body: { query: QUERY } })).body.count;
  console.error(`Professional + gendered contacts: ${total.toLocaleString()}${DRY ? '  [DRY RUN — no writes]' : ''}`);
  console.error(`  thresholds: >=${MIN_SAMPLES} sample(s) per domain, winning template >=${(MIN_AGREE * 100).toFixed(0)}% of votes\n`);

  // domain -> { tpl -> votes, dom -> votes, n }
  const byDomain = new Map();
  const t0 = Date.now();
  let scanned = 0, usable = 0, after = null;

  for (;;) {
    const body = { size: PAGE, query: QUERY, _source: ['email', 'first', 'last', 'domain'], sort: [{ email: 'asc' }] };
    if (after) body.search_after = after;
    const hits = (await client.search({ index: os.INDEX, body })).body.hits.hits;
    if (!hits.length) break;
    for (const h of hits) {
      scanned++;
      const s = h._source;
      const email = String(s.email || '').toLowerCase();
      const at = email.indexOf('@');
      if (at < 1) continue;
      const local = email.slice(0, at), emailDomain = email.slice(at + 1);
      // Key on the CONTACT's company domain, not the email domain — a firm mailing from @firmmail.com
      // still belongs to firm.com, and email_domain records where the mail actually lands.
      const key = String(s.domain || emailDomain).toLowerCase();
      if (!key || !emailDomain) continue;
      const tpl = templateFor(local, s.first, s.last);
      if (!tpl) continue;                                   // local part doesn't fit this person's name
      usable++;
      let e = byDomain.get(key);
      if (!e) { e = { tpl: new Map(), dom: new Map(), n: 0 }; byDomain.set(key, e); }
      e.tpl.set(tpl, (e.tpl.get(tpl) || 0) + 1);
      e.dom.set(emailDomain, (e.dom.get(emailDomain) || 0) + 1);
      e.n++;
      if (LIMIT && scanned >= LIMIT) break;
    }
    after = hits[hits.length - 1].sort;
    if (scanned % 250000 < PAGE) console.error(`  scanned ${scanned.toLocaleString()} | usable ${usable.toLocaleString()} | domains ${byDomain.size.toLocaleString()}`);
    if (LIMIT && scanned >= LIMIT) break;
  }
  console.error(`\nscanned ${scanned.toLocaleString()} contact(s); ${usable.toLocaleString()} had a local part that fits the name; ${byDomain.size.toLocaleString()} distinct domain(s)\n`);

  // Decide a pattern per domain, requiring both a sample floor and real agreement.
  const decided = [];
  const tally = { domains: byDomain.size, tooFew: 0, contested: 0, decided: 0, written: 0, errors: 0 };
  const tplHist = new Map();
  for (const [domain, e] of byDomain) {
    if (e.n < MIN_SAMPLES) { tally.tooFew++; continue; }
    let bestT = '', bestV = -1;
    for (const t of TEMPLATES) { const v = e.tpl.get(t) || 0; if (v > bestV) { bestV = v; bestT = t; } }
    if (!bestT || bestV / e.n < MIN_AGREE) { tally.contested++; continue; }
    let bestD = '', bestDV = -1;
    for (const [d, v] of e.dom) if (v > bestDV) { bestDV = v; bestD = d; }
    decided.push({ domain, pattern: bestT, email_domain: bestD, samples: e.n, agree: bestV / e.n });
    tplHist.set(bestT, (tplHist.get(bestT) || 0) + 1);
    tally.decided++;
  }

  console.error(`domains with a confident pattern: ${tally.decided.toLocaleString()}`);
  console.error(`  skipped: ${tally.tooFew.toLocaleString()} too few samples, ${tally.contested.toLocaleString()} no clear winner\n`);
  console.error('winning templates:');
  for (const [t, n] of [...tplHist.entries()].sort((a, b) => b[1] - a[1]).slice(0, 12)) {
    console.error(`  ${t.padEnd(20)}${String(n.toLocaleString()).padStart(9)}  ${((n / tally.decided) * 100).toFixed(1)}%`);
  }
  console.error('\nsamples:');
  for (const d of decided.slice(0, 10)) console.error(`  ${d.domain.padEnd(34)} ${d.pattern.padEnd(18)} @${d.email_domain.padEnd(26)} n=${d.samples} agree=${(d.agree * 100).toFixed(0)}%`);

  if (!DRY) {
    console.error(`\nwriting ${decided.length.toLocaleString()} company email model(s)…`);
    for (const d of decided) {
      try { await co.setEmailModelByDomain(coClient, d.domain, { pattern: d.pattern, email_domain: d.email_domain }); tally.written++; }
      catch (e) { tally.errors++; }
      if (tally.written % 5000 === 0 && tally.written) console.error(`  written ${tally.written.toLocaleString()}`);
    }
  }

  console.error(`\nDONE${DRY ? ' [DRY — nothing written]' : ''}: ${tally.decided.toLocaleString()} pattern(s) decided`
    + `${DRY ? '' : `, ${tally.written.toLocaleString()} written, ${tally.errors} error(s)`} | ${Math.round((Date.now() - t0) / 1000)}s`);
})().catch((e) => { console.error('ERR', e.meta ? JSON.stringify(e.meta.body).slice(0, 300) : (e.stack || e.message || e)); process.exit(1); });
