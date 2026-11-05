/**
 * pipeline-health.js — one number that says whether the BIO-URL pipeline actually worked yesterday.
 * ---------------------------------------------------------------------------------------------------
 * WHY THIS EXISTS. Every stage of this pipeline reports its own success, and nothing has ever checked the
 * JOINT between two stages. That is how all of the following ran for weeks looking healthy:
 *
 *   - the sweep reported "ok: true, 599,335 new BIO URLs" on a night when 2,499 URLs were crawled;
 *   - live-fleet-shard discarded 53,006 of 53,081 URLs at the domain gate and exited 0, which Fly reports
 *     identically to finishing the work;
 *   - 129,744 sitemaps carried monitored:true while the sweep's own query excluded them;
 *   - bio-pump ran 25,680 passes on an image seven days old, because it sits outside fly.toml;
 *   - every recorded pass showed liveQueued: 0 and nobody noticed it was every pass.
 *
 * Each was a local success and a joint failure. So this measures the joint and nothing else: of the URLs
 * DISCOVERED in a window, how many ended up with a real crawl outcome? On 2026-11-05 that would have read
 * 0.4% the following morning instead of being found three weeks later.
 *
 * The funnel is reported stage by stage deliberately. A single conversion number tells you something broke;
 * the stage whose ratio collapses tells you WHERE, which is the part that normally costs a day of digging.
 *
 *   OPENSEARCH_ENDPOINT=… node pipeline-health.js            # yesterday, print + email
 *   OPENSEARCH_ENDPOINT=… node pipeline-health.js --days 3   # a 3-day window
 *   OPENSEARCH_ENDPOINT=… node pipeline-health.js --no-email
 *
 * Env: PIPELINE_HEALTH_MIN_PCT (default 50) — below this the report is sent as a FAILURE.
 */
const os = require('./opensearch');
const ledger = require('./crawl-ledger');
const mailer = require('./mailer');

const arg = (f, d) => { const i = process.argv.indexOf('--' + f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const has = (f) => process.argv.includes('--' + f);
const DAYS = Math.max(1, Number(arg('days', '1')) || 1);
const NO_EMAIL = has('no-email');
const MIN_PCT = Number(process.env.PIPELINE_HEALTH_MIN_PCT) || 50;
const CFG = process.env.CC_CONFIG_INDEX || 'cc_config';
const STATE_ID = 'pipeline_health';
const KEEP = 60;

const N = (x) => Number(x || 0).toLocaleString();
const pct = (a, b) => (b ? Math.round((a / b) * 1000) / 10 : 0);

// A "real outcome" is anything an attempt wrote. 'queued' is the absence of one -- recordDiscovered sets it
// and no crawl ever does, which is exactly what makes it the right thing to count against.
const REAL = ['extracted', 'no-record', 'empty', 'error'];

async function measure(client, { from, to }) {
  const cnt = async (q) => ((await client.count({ index: ledger.INDEX, body: { query: q } })).body).count;
  const inWindow = { range: { queued_at: { gte: from, lt: to } } };
  const and = (...qs) => ({ bool: { filter: qs } });

  // "Has a real outcome" is NOT the test, and getting this wrong would have made the check useless: on
  // 2026-11-05, 352,018 of 593,213 discovered URLs carried outcome 'no-record' — from crawls months
  // earlier, re-offered by the 30-day RETRY_DAYS window. Counting those reads 59.3% on a night when 2,499
  // URLs were actually fetched. The joint we care about is whether an attempt happened AFTER the discovery,
  // so compare the two timestamps. The queued_at filter bounds the script to one night's rows.
  const attemptedSinceDiscovery = { script: { script: { lang: 'painless', source:
    "if (doc['last_attempt'].size()==0 || doc['queued_at'].size()==0) return false;"
    + " return doc['last_attempt'].value.toInstant().toEpochMilli() >= doc['queued_at'].value.toInstant().toEpochMilli();" } } };

  const discovered = await cnt(inWindow);
  const crawled = await cnt(and(inWindow, { terms: { outcome: REAL } }, attemptedSinceDiscovery));
  const extracted = await cnt(and(inWindow, { term: { outcome: 'extracted' } }, attemptedSinceDiscovery));
  const stillQueued = await cnt(and(inWindow, { term: { outcome: 'queued' } }));
  // The gap between "has an outcome" and "was crawled since we found it" is the retry treadmill, and it is
  // worth seeing on its own: it is volume the sweep creates that no crawl will ever change.
  const staleVerdict = await cnt(and(inWindow, { terms: { outcome: REAL } },
    { bool: { must_not: [attemptedSinceDiscovery] } }));

  // Where the uncrawled ones live. One domain dominating this list is the whole diagnosis -- on 2026-11-05
  // it would have been six blocked domains, which is the bug, stated.
  const agg = await client.search({ index: ledger.INDEX, body: { size: 0,
    query: and(inWindow, { term: { outcome: 'queued' } }),
    aggs: { d: { terms: { field: 'domain', size: 10 } } } } });
  const topStuck = ((agg.body || agg).aggregations.d.buckets || []).map((b) => ({ domain: b.key, count: b.doc_count }));

  // Outcome mix of the ones that DID get crawled, so "crawled but yielding nothing" is visible as its own
  // failure rather than hiding inside a healthy-looking conversion rate.
  const mixAgg = await client.search({ index: ledger.INDEX, body: { size: 0,
    query: and(inWindow, { terms: { outcome: REAL } }),
    aggs: { o: { terms: { field: 'outcome', size: 6 } } } } });
  const mix = ((mixAgg.body || mixAgg).aggregations.o.buckets || []).map((b) => ({ outcome: b.key, count: b.doc_count }));

  // Contacts the monitor actually landed in the window. The end of the chain: everything above is plumbing.
  let contacts = 0;
  try {
    contacts = ((await client.count({ index: os.INDEX, body: { query: and(
      { range: { updated_at: { gte: from, lt: to } } }, { term: { source: 'Sitemap Monitor' } }) } })).body).count;
  } catch (e) { contacts = -1; }

  return { from, to, discovered, crawled, extracted, stillQueued, staleVerdict, topStuck, mix, contacts,
    conversion: pct(crawled, discovered), yield: pct(extracted, discovered) };
}

function build(m) {
  const ok = m.discovered === 0 || m.conversion >= MIN_PCT;
  const day = String(m.from).slice(0, 10);
  const subject = `${ok ? 'Pipeline OK' : 'PIPELINE STALLED'} — ${m.conversion}% of ${N(m.discovered)} discovered BIO URLs were crawled (${day})`;

  const rows = [
    ['Discovered (ledger queued_at)', N(m.discovered)],
    ['Crawled SINCE being discovered', `<strong>${N(m.crawled)}</strong> (${m.conversion}%)`],
    ['Still only "queued"', N(m.stillQueued)],
    ['Carrying an older verdict (retry churn)', N(m.staleVerdict)],
    ['Yielded a page we extracted', `${N(m.extracted)} (${m.yield}% of discovered)`],
    ['Contacts landed (Sitemap Monitor)', m.contacts < 0 ? 'n/a' : N(m.contacts)],
  ];
  const banner = ok
    ? `<p style="margin:0 0 16px;padding:10px 14px;background:#ecfdf5;border-left:3px solid #059669;color:#065f46">${m.conversion}% of what was discovered got crawled.</p>`
    : `<p style="margin:0 0 16px;padding:10px 14px;background:#fef2f2;border-left:3px solid #dc2626;color:#991b1b">Only ${m.conversion}% of discovered BIO URLs were crawled (threshold ${MIN_PCT}%). ${N(m.stillQueued)} are still sitting at "queued" — something between discovery and the crawl is not running.</p>`;

  const stuck = m.topStuck.length ? `<h3 style="font-size:14px;margin:24px 0 8px;color:#111827">Where the uncrawled URLs are</h3>
    <table style="border-collapse:collapse;font-size:13px">${m.topStuck.map((t) => `<tr><td style="padding:4px 16px 4px 0;color:#374151">${t.domain || '(none)'}</td><td style="padding:4px 0;text-align:right;font-variant-numeric:tabular-nums">${N(t.count)}</td></tr>`).join('')}</table>` : '';
  const mixHtml = m.mix.length ? `<p style="font-size:13px;color:#6b7280;margin-top:16px">Outcomes: ${m.mix.map((x) => `${x.outcome} ${N(x.count)}`).join(' · ')}</p>` : '';

  const html = `<div style="font-family:Inter,system-ui,Segoe UI,Arial,sans-serif;max-width:620px;margin:0 auto;color:#111827">
    <h2 style="font-size:1.2rem;margin:0 0 4px">BIO-URL pipeline — discovery to crawl</h2>
    <p style="color:#6b7280;font-size:13px;margin:0 0 16px">${String(m.from).slice(0, 16)} → ${String(m.to).slice(0, 16)} UTC</p>
    ${banner}
    <table style="border-collapse:collapse;font-size:14px">
      ${rows.map(([k, v]) => `<tr><td style="padding:6px 20px 6px 0;color:#6b7280;white-space:nowrap">${k}</td><td style="padding:6px 0;font-variant-numeric:tabular-nums">${v}</td></tr>`).join('')}
    </table>
    ${stuck}${mixHtml}
    <p style="color:#9ca3af;font-size:12px;margin-top:24px;border-top:1px solid #eee;padding-top:12px">Common Crawler · every stage reports its own success; this reports the joint between them.</p>
  </div>`;

  const text = [`BIO-URL pipeline — discovery to crawl (${day})`, '',
    ok ? `OK: ${m.conversion}% of discovered URLs were crawled.`
       : `STALLED: only ${m.conversion}% crawled (threshold ${MIN_PCT}%). ${N(m.stillQueued)} still queued.`, '',
    ...rows.map(([k, v]) => `${k}: ${String(v).replace(/<[^>]+>/g, '')}`),
    ...(m.topStuck.length ? ['', 'Uncrawled by domain:', ...m.topStuck.map((t) => `  ${t.domain || '(none)'}: ${N(t.count)}`)] : []),
    '', '— Common Crawler'].join('\n');

  return { subject, text, html, ok };
}

// Keep the history, so "it has been degrading for a week" is answerable. Best-effort: a reporting failure
// must never be the reason nobody hears about a stalled pipeline.
async function record(client, m, ok) {
  try {
    let runs = [];
    try { const g = await client.get({ index: CFG, id: STATE_ID }); runs = ((g.body || g)._source || {}).runs || []; }
    catch (e) { const c = e && (e.statusCode || (e.meta && e.meta.statusCode)); if (!(c === 404 || /404|not_found|index_not_found/i.test(String(e && e.message)))) throw e; }
    runs.unshift({ at: new Date().toISOString(), from: m.from, to: m.to, discovered: m.discovered,
      crawled: m.crawled, stillQueued: m.stillQueued, extracted: m.extracted, contacts: m.contacts,
      conversion: m.conversion, ok });
    await client.index({ index: CFG, id: STATE_ID, body: { runs: runs.slice(0, KEEP) }, refresh: true });
  } catch (e) { console.error('[pipeline-health] could not record run:', e.message); }
}

async function run({ client = null, days = DAYS, email = !NO_EMAIL, from = null, to = null } = {}) {
  const c = client || os.makeClient(process.env.OPENSEARCH_ENDPOINT);
  // The window ENDS 24h ago, not now: a URL discovered an hour ago has not had a chance yet, and counting
  // it as a failure would make the check cry wolf every morning the sweep runs late.
  // --from/--to override it, so a past night can be re-measured after the fact -- which is the only way to
  // check that the check itself reports what actually happened.
  const toD = to || arg('to', '') || new Date(Date.now() - 24 * 3600 * 1000).toISOString();
  const fromD = from || arg('from', '') || new Date(Date.parse(toD) - days * 24 * 3600 * 1000).toISOString();
  const m = await measure(c, { from: new Date(fromD).toISOString(), to: new Date(toD).toISOString() });
  const msg = build(m);

  console.log(`discovered ${N(m.discovered)} · crawled since discovery ${N(m.crawled)} (${m.conversion}%) · still queued ${N(m.stillQueued)} · stale verdict ${N(m.staleVerdict)} · extracted ${N(m.extracted)} · contacts ${m.contacts < 0 ? 'n/a' : N(m.contacts)}`);
  if (m.topStuck.length) for (const t of m.topStuck.slice(0, 5)) console.log(`   stuck: ${String(t.domain || '(none)').padEnd(36)} ${N(t.count)}`);
  await record(c, m, msg.ok);
  if (email) {
    try { await mailer.sendMail({ to: process.env.MONITOR_REPORT_TO || mailer.adminEmail(), subject: msg.subject, text: msg.text, html: msg.html }); }
    catch (e) { console.error('[pipeline-health] email failed:', e.message); }
  }
  return { ...m, ok: msg.ok };
}

module.exports = { run, measure, build };

if (require.main === module) {
  run().then((r) => process.exit(r.ok ? 0 : 1))
    .catch((e) => { console.error('pipeline-health ERR', (e && e.stack) || e); process.exit(1); });
}
