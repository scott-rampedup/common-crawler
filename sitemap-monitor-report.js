/**
 * sitemap-monitor-report.js — the nightly Sitemap Monitor email, built from the pass summary itself.
 *
 * There is an older send-monitor-report.js. It reads the LEGACY SQLite monitor tables (watched_sitemaps,
 * bio_urls, observations), which cover a few thousand hand-watched sitemaps — not the Sitemap Library's
 * 237,018 monitored People sitemaps in OpenSearch. Its numbers therefore describe a different, much smaller
 * system than the one that actually runs nightly. This module reports on the sweep that ran.
 *
 * Also records each run in cc_config so the count is auditable after the fact and the UI can show history —
 * an email is not a record.
 */
const mailer = require('./mailer');

const CONFIG_INDEX = process.env.CC_CONFIG_INDEX || 'cc_config';
const RUNS_ID = 'sitemap_monitor_runs';
const KEEP_RUNS = 60;                     // ~2 months of nightlies

const n = (v) => Number(v || 0).toLocaleString();
const pct = (a, b) => (b ? Math.round((a / b) * 1000) / 10 : 0);

// Persist the run, newest first, capped. Best-effort: a reporting failure must never fail the sweep.
async function recordRun(client, summary) {
  if (!client) return;
  try {
    let runs = [];
    try { const g = await client.get({ index: CONFIG_INDEX, id: RUNS_ID }); runs = ((g.body || g)._source || {}).runs || []; }
    catch (e) { if (!/404|not_found|index_not_found/i.test(String(e && e.message))) throw e; }
    runs.unshift({ startedAt: summary.startedAt, finishedAt: summary.finishedAt, seconds: summary.seconds,
      total: summary.total, scanned: summary.scanned, withGap: summary.withGap, newUrls: summary.newUrls,
      liveQueued: summary.liveQueued, noUrls: summary.noUrls, errors: summary.errors,
      seenUrls: summary.seenUrls, stateOk: summary.stateOk, stateErrors: summary.stateErrors,
      queueErrors: summary.queueErrors, ok: !!summary.ok });
    await client.index({ index: CONFIG_INDEX, id: RUNS_ID, body: { runs: runs.slice(0, KEEP_RUNS) }, refresh: true });
  } catch (e) { console.error('[monitor-report] could not record run:', e.message); }
}

function build(summary, extra = {}) {
  const day = String(summary.finishedAt || summary.startedAt || '').slice(0, 10);
  // "Complete" is coverage AND landed writes. The first nightly run compared nothing and wrote nothing,
  // and a coverage-only test would have called that a 0% sweep rather than a failed one.
  const covered = summary.total > 0 && summary.scanned >= summary.total;
  const complete = covered && !summary.stateErrors && !summary.queueErrors;
  const mins = Math.round((summary.seconds || 0) / 60);
  const subject = `Sitemap Monitor — ${n(summary.newUrls)} new BIO URLs from ${n(summary.withGap)} sitemaps (${day})`;

  const rows = [
    ['Sitemaps compared', `${n(summary.scanned)} of ${n(summary.total)} monitored (${pct(summary.scanned, summary.total)}%)`],
    ...(summary.seenUrls != null ? [['BIO URLs monitored', `${n(summary.seenUrls)} (not de-duplicated across sitemaps)`]] : []),
    ['Sitemaps with new bios', n(summary.withGap)],
    ['New BIO URLs found', `<strong>${n(summary.newUrls)}</strong>`],
    ['Queued for extraction', `${n(summary.newUrls)} (${n(summary.liveQueued)} also started live)`],
    ['Sitemaps returning no URLs', n(summary.noUrls)],
    ['Errors', n(summary.errors)],
    ['Index writes', `${n(summary.stateOk)} ok${summary.stateErrors ? ` · <strong style="color:#b91c1c">${n(summary.stateErrors)} FAILED</strong>` : ''}`],
    ['Sweep duration', `${mins} min`],
    ...(summary.peakHeapGb != null ? [['Peak heap', `${summary.peakHeapGb} GB of ${summary.heapLimitGb} GB (conc ${summary.conc})`]] : []),
  ];
  if (summary.queuedObjects != null) rows.push(['Queue objects written', `${n(summary.queuedObjects)}${summary.queueErrors ? ` · <strong style="color:#b91c1c">${n(summary.queueErrors)} FAILED</strong>` : ''}`]);
  if (extra.backlog != null) rows.push(['BIO ETL queue after sweep', n(extra.backlog)]);

  // State the coverage claim explicitly. A monitor that quietly checked 29% of the library still sends a
  // cheerful email; saying which it was is the difference between a report and a reassurance.
  const bad = (t) => `<p style="margin:0 0 16px;padding:10px 14px;background:#fef2f2;border-left:3px solid #dc2626;color:#991b1b">${t}</p>`;
  const banner = complete
    ? `<p style="margin:0 0 16px;padding:10px 14px;background:#ecfdf5;border-left:3px solid #059669;color:#065f46">Full sweep — every monitored sitemap was re-checked tonight.</p>`
    : !covered
      ? bad(`Incomplete sweep — ${n(summary.scanned)} of ${n(summary.total)} sitemaps were checked. The rest were not compared.`)
      : bad(`Sweep covered every sitemap but ${n(summary.stateErrors)} index write(s)${summary.queueErrors ? ` and ${n(summary.queueErrors)} queue write(s)` : ''} FAILED — results are not fully recorded. ${summary.stateSample ? String(summary.stateSample).slice(0, 120) : ''}`);

  const topRows = (summary.top || []).map((t) => `<tr><td style="padding:4px 16px 4px 0;color:#374151">${t.domain}</td><td style="padding:4px 0;text-align:right;font-variant-numeric:tabular-nums;color:#111827">${n(t.count)}</td></tr>`).join('');
  const top = topRows ? `<h3 style="font-size:14px;margin:24px 0 8px;color:#111827">Most new bios</h3>
    <table style="border-collapse:collapse;font-size:13px">${topRows}</table>` : '';

  const html = `<div style="font-family:Inter,system-ui,Segoe UI,Arial,sans-serif;max-width:620px;margin:0 auto;color:#111827">
    <h2 style="font-size:1.2rem;margin:0 0 4px">Sitemap Monitor — nightly sweep</h2>
    <p style="color:#6b7280;font-size:13px;margin:0 0 16px">${day} · finished ${String(summary.finishedAt || '').slice(11, 16)} UTC</p>
    ${banner}
    <table style="border-collapse:collapse;font-size:14px">
      ${rows.map(([k, v]) => `<tr><td style="padding:6px 20px 6px 0;color:#6b7280;white-space:nowrap">${k}</td><td style="padding:6px 0;color:#111827;font-variant-numeric:tabular-nums">${v}</td></tr>`).join('')}
    </table>
    ${top}
    <p style="color:#9ca3af;font-size:12px;margin-top:24px;border-top:1px solid #eee;padding-top:12px">Common Crawler · new bios land as contacts with Source &ldquo;Sitemap Monitor&rdquo; (New Hire = Y).</p>
  </div>`;

  const text = [`Sitemap Monitor — nightly sweep (${day})`, '',
    complete ? 'Full sweep: every monitored sitemap was re-checked.'
      : !covered ? `INCOMPLETE: ${n(summary.scanned)} of ${n(summary.total)} sitemaps checked.`
      : `WRITES FAILED: covered every sitemap but ${n(summary.stateErrors)} index write(s) failed.`, '',
    ...rows.map(([k, v]) => `${k}: ${String(v).replace(/<[^>]+>/g, '')}`),
    ...(summary.top || []).length ? ['', 'Most new bios:', ...(summary.top || []).map((t) => `  ${t.domain}: ${n(t.count)}`)] : [],
    '', '— Common Crawler'].join('\n');

  return { subject, text, html };
}

async function sendSweepReport(summary, { client = null, to = null, extra = {} } = {}) {
  await recordRun(client, summary);
  const rcpt = to || process.env.MONITOR_REPORT_TO || mailer.adminEmail();
  const msg = build(summary, extra);
  return mailer.sendMail({ to: rcpt, ...msg });
}

module.exports = { sendSweepReport, build, recordRun };
