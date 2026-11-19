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
 * Env: PIPELINE_HEALTH_MIN_PCT        (default 50) — crawl conversion below this is a FAILURE.
 *      PIPELINE_HEALTH_MIN_PERSON_PCT (default 25) — share of landed contacts that are actually
 *                                     PEOPLE; below this is also a FAILURE. See below for why.
 */
const os = require('./opensearch');
const ledger = require('./crawl-ledger');
const mailer = require('./mailer');

const arg = (f, d) => { const i = process.argv.indexOf('--' + f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const has = (f) => process.argv.includes('--' + f);
const DAYS = Math.max(1, Number(arg('days', '1')) || 1);
const NO_EMAIL = has('no-email');
const MIN_PCT = Number(process.env.PIPELINE_HEALTH_MIN_PCT) || 50;
// Crawling is plumbing; people are the product, and until now nothing measured the difference.
// On 2026-11-19 this file reported conversion 92.3% and ok:true against a database where 7.2M of
// 17.4M rows carry no gender -- because they are not people. Sampling them found the commonest
// "first names" were admin, board, the, our and meet: /team/ and /our-board/ pages minted as
// contacts. A health check that cannot fail on that is measuring the wrong joint.
//
// Gender is the honest proxy for "is a person": it is only ever filled when the first name matched
// a real given name, so it cannot be satisfied by a section page. The threshold is deliberately
// low -- this is a collapse detector, not a quality target -- and is meant to be raised once the
// steady-state figure is known.
const MIN_PERSON_PCT = Number(process.env.PIPELINE_HEALTH_MIN_PERSON_PCT) || 25;
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

  // Contacts landed in the window, and how many of them are PEOPLE. The end of the chain:
  // everything above is plumbing.
  //
  // This used to filter on source='Sitemap Monitor'. Measured 2026-11-19, that source landed FIVE
  // rows in ten days while the index took 2,244,203 -- so the one line reporting the end of the
  // chain was reporting a channel that had stopped running, and read 0 every night without anyone
  // treating 0 as a problem. Count every source, and break the yield out BY source, because that
  // is what names the channel doing the diluting (on that date: Public Prospects, 1.55M rows at
  // 37.8% people, against 89-96% for everything the crawler produces).
  const hasGender = { bool: { must: [{ exists: { field: 'gender' } }], must_not: [{ term: { gender: '' } }] } };
  let contacts = 0, people = 0, bySource = [];
  try {
    const inWin = { range: { updated_at: { gte: from, lt: to } } };
    const landed = async (extra) => ((await client.count({ index: os.INDEX,
      body: { query: and(inWin, ...extra) } })).body).count;
    contacts = await landed([]);
    // A gender is only ever written when the first name matched a real given name, so this counts
    // rows that are a person rather than a page.
    people = await landed([hasGender]);
    const a = await client.search({ index: os.INDEX, body: { size: 0, query: inWin,
      aggs: { s: { terms: { field: 'source', size: 12 }, aggs: { g: { filter: hasGender } } } } } });
    bySource = ((((a.body || a).aggregations || {}).s || {}).buckets || []).map((b) => ({
      source: b.key || '(none)', landed: b.doc_count, people: (b.g || {}).doc_count || 0,
      pct: pct((b.g || {}).doc_count || 0, b.doc_count) }));
  } catch (e) { contacts = -1; people = -1; }

  return { from, to, discovered, crawled, extracted, stillQueued, staleVerdict, topStuck, mix,
    contacts, people, bySource,
    conversion: pct(crawled, discovered), yield: pct(extracted, discovered),
    // of what we landed, how much is sellable; and end-to-end, people per URL discovered
    personPct: pct(people, contacts), personYield: pct(people, discovered) };
}

function build(m) {
  // A window with NO discoveries is not a pass. The sweep runs nightly and records every URL it finds, so
  // zero means either the sweep did not run or it is not recording -- which is exactly the bug that went
  // unnoticed from 2026-08 to 2026-11-05, when the ledger write sat in a callback the nightly sweep never
  // used. The first version of this check emailed "Pipeline OK — 0% of 0", which is the precise failure
  // this file exists to prevent: a monitor reporting health when it has no evidence of any.
  const noData = m.discovered === 0;
  // Two ways to fail, because there are two ways for this pipeline to be broken: it can stop
  // crawling, or it can keep crawling and stop producing people. Only the first was ever detected.
  const crawlOk = m.conversion >= MIN_PCT;
  // Judged only when something actually landed -- with nothing to measure, this is not a failure.
  const peopleOk = m.contacts <= 0 || m.personPct >= MIN_PERSON_PCT;
  const ok = !noData && crawlOk && peopleOk;
  const day = String(m.from).slice(0, 10);
  const subject = noData
    ? `NO DISCOVERIES RECORDED — the sweep logged no BIO URLs for ${day}`
    : !crawlOk
      ? `PIPELINE STALLED — ${m.conversion}% of ${N(m.discovered)} discovered BIO URLs were crawled (${day})`
      : !peopleOk
        ? `POOR YIELD — only ${m.personPct}% of ${N(m.contacts)} landed contacts are people (${day})`
        : `Pipeline OK — ${m.conversion}% crawled, ${N(m.people)} people landed (${day})`;

  const rows = [
    ['Discovered (ledger queued_at)', N(m.discovered)],
    ['Crawled SINCE being discovered', `<strong>${N(m.crawled)}</strong> (${m.conversion}%)`],
    ['Still only "queued"', N(m.stillQueued)],
    ['Carrying an older verdict (retry churn)', N(m.staleVerdict)],
    ['Yielded a page we extracted', `${N(m.extracted)} (${m.yield}% of discovered)`],
    ['Contacts landed (all sources)', m.contacts < 0 ? 'n/a' : N(m.contacts)],
    ['...of which are PEOPLE (have a gender)', m.people < 0 ? 'n/a'
      : `<strong>${N(m.people)}</strong> (${m.personPct}% of landed)`],
  ];
  const banner = noData
    ? `<p style="margin:0 0 16px;padding:10px 14px;background:#fffbeb;border-left:3px solid #d97706;color:#92400e">No BIO URLs were recorded as discovered in this window at all. Either the sweep did not run, or it ran and did not write to the crawl ledger. This is not a pass — there is nothing to measure.</p>`
    : ok
      ? `<p style="margin:0 0 16px;padding:10px 14px;background:#ecfdf5;border-left:3px solid #059669;color:#065f46">${m.conversion}% of what was discovered got crawled, and ${m.personPct}% of what landed is a person.</p>`
      : !crawlOk
        ? `<p style="margin:0 0 16px;padding:10px 14px;background:#fef2f2;border-left:3px solid #dc2626;color:#991b1b">Only ${m.conversion}% of discovered BIO URLs were crawled (threshold ${MIN_PCT}%). ${N(m.stillQueued)} are still sitting at "queued" — something between discovery and the crawl is not running.</p>`
        : `<p style="margin:0 0 16px;padding:10px 14px;background:#fef2f2;border-left:3px solid #dc2626;color:#991b1b">The crawl is working — ${m.conversion}% converted — but only ${m.personPct}% of the ${N(m.contacts)} contacts it landed are people (threshold ${MIN_PERSON_PCT}%). The rest carry no gender, which in practice means a section page was stored as a contact. Crawling harder will not fix this.</p>`;

  const stuck = m.topStuck.length ? `<h3 style="font-size:14px;margin:24px 0 8px;color:#111827">Where the uncrawled URLs are</h3>
    <table style="border-collapse:collapse;font-size:13px">${m.topStuck.map((t) => `<tr><td style="padding:4px 16px 4px 0;color:#374151">${t.domain || '(none)'}</td><td style="padding:4px 0;text-align:right;font-variant-numeric:tabular-nums">${N(t.count)}</td></tr>`).join('')}</table>` : '';
  const mixHtml = m.mix.length ? `<p style="font-size:13px;color:#6b7280;margin-top:16px">Outcomes: ${m.mix.map((x) => `${x.outcome} ${N(x.count)}`).join(' · ')}</p>` : '';

  // Which channel is producing people and which is producing rows. Sorted worst-yield first among
  // the ones big enough to matter, because that is the one to go and look at.
  const srcRows = (m.bySource || []).filter((x) => x.landed >= 100)
    .sort((a, b) => a.pct - b.pct);
  const srcHtml = srcRows.length ? `<h3 style="font-size:14px;margin:24px 0 8px;color:#111827">People per source (worst yield first)</h3>
    <table style="border-collapse:collapse;font-size:13px">${srcRows.map((x) => `<tr><td style="padding:4px 16px 4px 0;color:#374151">${x.source}</td><td style="padding:4px 16px 4px 0;text-align:right;font-variant-numeric:tabular-nums">${N(x.landed)}</td><td style="padding:4px 0;text-align:right;font-variant-numeric:tabular-nums;color:${x.pct < MIN_PERSON_PCT ? '#b91c1c' : '#065f46'}">${x.pct}%</td></tr>`).join('')}</table>` : '';

  const html = `<div style="font-family:Inter,system-ui,Segoe UI,Arial,sans-serif;max-width:620px;margin:0 auto;color:#111827">
    <h2 style="font-size:1.2rem;margin:0 0 4px">BIO-URL pipeline — discovery to crawl</h2>
    <p style="color:#6b7280;font-size:13px;margin:0 0 16px">${String(m.from).slice(0, 16)} → ${String(m.to).slice(0, 16)} UTC</p>
    ${banner}
    <table style="border-collapse:collapse;font-size:14px">
      ${rows.map(([k, v]) => `<tr><td style="padding:6px 20px 6px 0;color:#6b7280;white-space:nowrap">${k}</td><td style="padding:6px 0;font-variant-numeric:tabular-nums">${v}</td></tr>`).join('')}
    </table>
    ${stuck}${srcHtml}${mixHtml}
    <p style="color:#9ca3af;font-size:12px;margin-top:24px;border-top:1px solid #eee;padding-top:12px">Common Crawler · every stage reports its own success; this reports the joint between them.</p>
  </div>`;

  const text = [`BIO-URL pipeline — discovery to crawl (${day})`, '',
    noData ? 'NO DISCOVERIES RECORDED: nothing was logged as discovered in this window, so there is nothing to measure. Either the sweep did not run or it is not writing to the crawl ledger.'
      : ok ? `OK: ${m.conversion}% of discovered URLs were crawled.`
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
