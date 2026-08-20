/**
 * sitemap-lib-monitor.js — the Sitemap Library's built-in monitor (gap-fill model). Re-checks the WHOLE
 * Library's People sitemaps for deltas: re-fetch each and extract only the page URLs we DON'T already have
 * a contact for — the contacts DB is the baseline, so this closes the "Have vs Pages" gap and lands new
 * hires (Source='Sitemap Monitor'). Monitoring is ON for every sitemap by default (monitored=false opts a
 * sitemap out); last_checked ordering rotates coverage so all get re-checked over the schedule.
 * Phase 1 = People -> contacts. (Location -> companies is a later phase.)
 */

// Generic sitemap words that carry no profession signal — stripped so a keyword like "agents-sitemap.xml"
// yields the useful token "agent(s)" rather than the everything-matching "sitemap".
const KW_STOP = new Set(['sitemap', 'sitemaps', 'sitemapindex', 'index', 'xml', 'gz', 'wp', 'post', 'posts', 'page', 'pages', 'main', 'all', 'www', 'http', 'https', 'html']);

// Distinctive profession tokens from a stored keyword (its matched filename), for the keyword second pass.
// e.g. "loan-officer-sitemap.xml" -> {loan, officer};  "agents.xml" -> {agents, agent} (depluralized).
function keywordTokens(keyword) {
  const raw = String(keyword || '').toLowerCase().replace(/\.[a-z0-9]+$/, '');   // drop a file extension
  const out = new Set();
  for (const t of raw.split(/[^a-z0-9]+/)) {
    if (t.length < 3 || KW_STOP.has(t)) continue;
    out.add(t);
    if (t.endsWith('s') && t.length > 3) out.add(t.slice(0, -1));                  // singular ~ plural
  }
  return out;
}

module.exports.makeLibMonitor = function makeLibMonitor(deps) {
  const { sitemaps, sitemapsClient, contactsClient, contactsIndex, ccEngine, extract,
    directoryRules = {}, genderMap = {}, bioSitemapNames = null, locationSitemapNames = null,
    log = () => {} } = deps;
  let running = false;

  // Of these page URLs, which do we already have a contact for (exact web_source_url, keyword field)?
  async function haveSet(urls) {
    const have = new Set();
    if (!contactsClient || !urls.length) return have;
    for (let i = 0; i < urls.length; i += 1024) {
      const chunk = urls.slice(i, i + 1024);
      try {
        const r = await contactsClient.search({ index: contactsIndex, body: { size: 0, query: { terms: { web_source_url: chunk } }, aggs: { u: { terms: { field: 'web_source_url', size: chunk.length } } } } });
        for (const b of (((r.body || r).aggregations.u.buckets) || [])) have.add(b.key);
      } catch (e) { /* best-effort */ }
    }
    return have;
  }

  const peopleUrls = (watches) => [...new Set((watches || []).filter((w) => w.kind === 'People').flatMap((w) => (w.urls || []).map((u) => u.url)))];

  // ONE NIGHTLY PASS over EVERY monitored People sitemap in the Library.
  //
  // The contract this has to keep: every sitemap is re-fetched, its bio URLs compared against the contacts
  // we already hold, the new ones flagged and handed to extraction, and its last_checked stamped — for all
  // of them, every night, regardless of how far behind the extraction queue is.
  //
  // The previous version broke that contract two ways, and both looked like "the monitor ran fine":
  //   1. it took a single 50,000-sitemap slice (OpenSearch's per-search ceiling) out of 237,018, so a full
  //      sweep took ~2.4 days and "nightly" meant the oldest 29%;
  //   2. it early-returned the moment the URL cap was hit — `if (summary.extracted >= cap) return` — which
  //      skipped every remaining sitemap AND left their last_checked unwritten, so the sitemaps most likely
  //      to have new bios were the ones the next pass would also skip.
  //
  // So: the comparison is now uncapped and streamed. `liveCap` bounds only how many URLs get an INLINE live
  // crawl job; past it the URLs still go to the durable S3 queue for the ETL. A backlog can delay when a new
  // bio is turned into a contact. It can no longer stop us from finding it.
  async function runPass({ liveCap = 300000, conc = 64, flush = 3000, kind = 'People', type = '',
                           page = 5000, cap = null, maxSitemaps = 0, fetchTimeout = 8000,
                           fetchMaxBytes = 8 * 1024 * 1024, stateFlush = 500, onProgress = null } = {}) {
    if (running) { log('monitor pass already running — skipping'); return { skipped: true }; }
    if (cap != null) liveCap = cap;                       // back-compat with the old {cap} callers
    running = true;
    const startedAt = new Date().toISOString();
    const t0 = Date.now();
    const summary = { startedAt, finishedAt: null, seconds: 0, total: 0, scanned: 0, withGap: 0,
      newUrls: 0, liveQueued: 0, jobs: 0, noUrls: 0, errors: 0, liveCapReached: false, top: [],
      stateOk: 0, stateErrors: 0, stateRejected: 0, stateSample: '', ok: false };
    const byDomain = new Map();                            // domain -> new-URL count, for the report
    // A sweep of this size is dominated by sitemaps that no longer resolve, and the default fetchDoc spends
    // up to 15s on the primary path and another 15s on the residential gateway for each one. Measured on
    // live data: 262 of 408 sitemaps returned nothing, at 16.4s average -> a 45-hour full sweep. Shorten the
    // timeout and only escalate to residential for statuses that actually mean "blocked" rather than "gone".
    const swFetch = (u) => ccEngine.fetchDoc(u, { timeout: fetchTimeout, fallbackStatus: [403, 429, 503], maxBytes: fetchMaxBytes });
    const nowIso = startedAt;
    let buffer = [];
    // State writes go through ONE bulk per stateFlush sitemaps instead of one update per sitemap. At
    // conc 320 the old path put that many single-doc updates in flight at once, which is what OpenSearch
    // sheds first under load -- and every rejection was swallowed.
    let stateBuf = [];
    const flushState = async () => {
      if (!stateBuf.length) return;
      const batch = stateBuf; stateBuf = [];
      const r = await sitemaps.bulkSetMonitorState(sitemapsClient, batch);
      summary.stateOk += r.ok; summary.stateErrors += r.errors; summary.stateRejected += r.rejected;
      if (r.sample && !summary.stateSample) { summary.stateSample = r.sample; log(`  state write error: ${r.sample}`); }
    };
    const putState = async (sitemap_url, patch) => {
      stateBuf.push({ sitemap_url, patch });
      if (stateBuf.length >= stateFlush) await flushState();
    };

    const doFlush = () => {
      if (!buffer.length) return;
      const urls = buffer; buffer = [];
      // Under the live cap we ask for a live job too; past it, queue only. Either way the URLs are durable.
      const live = summary.liveQueued < liveCap;
      if (live) summary.liveQueued += urls.length; else summary.liveCapReached = true;
      try { extract(urls, `Sitemap Monitor: ${urls.length} new bio URL(s)`, { live }); summary.jobs++; }
      catch (e) { summary.errors++; }
    };

    const processOne = async (d) => {
      summary.scanned++;
      try {
        const { watches } = await ccEngine.discoverSitemaps({ urls: [d.sitemap_url], directoryRules, genderMap, bioSitemapNames, locationSitemapNames, _fetchDoc: swFetch });
        let pageUrls = peopleUrls(watches);
        // keyword second pass ONLY when the strict pass got nothing (avoids doubling fetches at scale)
        if (!pageUrls.length) {
          const hints = keywordTokens(d.keyword);
          if (hints.size) { try { const { watches: w2 } = await ccEngine.discoverSitemaps({ urls: [d.sitemap_url], directoryRules, genderMap, bioSitemapNames, locationSitemapNames, keywordHints: hints, _fetchDoc: swFetch }); pageUrls = peopleUrls(w2); } catch (e) { /* */ } }
        }
        let missing = [];
        if (pageUrls.length) { const have = await haveSet(pageUrls); missing = pageUrls.filter((u) => !have.has(u)); }
        else summary.noUrls++;
        if (missing.length) {
          summary.withGap++; summary.newUrls += missing.length;
          byDomain.set(d.domain || d.sitemap_url, (byDomain.get(d.domain || d.sitemap_url) || 0) + missing.length);
          buffer.push(...missing);
          if (buffer.length >= flush) doFlush();
        }
        // last_new_at makes "new tonight" queryable — last_new alone can't distinguish a sitemap that found
        // 12 new bios this pass from one that found 12 a week ago and none since.
        await putState(d.sitemap_url, {
          last_checked: nowIso, last_new: missing.length,
          total_new: (Number(d.total_new) || 0) + missing.length,
          ...(missing.length ? { last_new_at: nowIso } : {}),
          monitor_note: pageUrls.length ? '' : 'no urls fetched',
        });
      } catch (e) {
        summary.errors++;
        try { await putState(d.sitemap_url, { last_checked: nowIso, monitor_note: String(e.message || 'error').slice(0, 200) }); } catch (e2) { /* */ }
      }
    };

    try {
      summary.total = await sitemaps.monitoredCount(sitemapsClient, { kind, type });
      log(`nightly sweep: ${summary.total.toLocaleString()} monitored ${[type, kind].filter(Boolean).join(' ')} sitemap(s) — ALL of them (conc ${conc}, live cap ${liveCap.toLocaleString()})`);
      let lastLog = 0;
      // maxSitemaps bounds the SWEEP (used by the manual button so it returns in seconds). The nightly run
      // leaves it 0 = every sitemap; it is not a throughput knob and nothing schedules it.
      const limit = maxSitemaps > 0 ? Math.min(maxSitemaps, summary.total) : 0;
      if (limit) summary.total = limit;
      outer:
      for await (const rows of sitemaps.monitoredCursor(sitemapsClient, { kind, type, page })) {
        for (let i = 0; i < rows.length; i += conc) {
          await Promise.all(rows.slice(i, i + conc).map(processOne));
          if (limit && summary.scanned >= limit) break outer;
        }
        if (summary.scanned - lastLog >= 20000) {
          lastLog = summary.scanned;
          const rate = Math.round(summary.scanned / Math.max(1, (Date.now() - t0) / 1000));
          const eta = rate ? Math.round((summary.total - summary.scanned) / rate / 60) : 0;
          log(`  ${summary.scanned.toLocaleString()}/${summary.total.toLocaleString()} sitemaps | ${summary.newUrls.toLocaleString()} new bio URL(s) | ${rate}/s | ETA ${eta}m`);
          if (onProgress) { try { onProgress({ ...summary }); } catch (e) { /* */ } }
        }
      }
      doFlush();
      await flushState();
      try { await sitemapsClient.indices.refresh({ index: sitemaps.INDEX }); } catch (e) { /* */ }
    } finally {
      running = false;
      summary.finishedAt = new Date().toISOString();
      summary.seconds = Math.round((Date.now() - t0) / 1000);
      try { await flushState(); } catch (e) { /* */ }
      // A sweep is only "ok" if it covered everything AND its writes actually landed. Coverage alone was
      // the old bar, and a pass that stamped nothing still read as a clean run.
      summary.ok = summary.total > 0 && summary.scanned >= summary.total && summary.stateErrors === 0;
      summary.top = [...byDomain.entries()].sort((a, b) => b[1] - a[1]).slice(0, 15).map(([domain, count]) => ({ domain, count }));
      log(`nightly sweep done: scanned ${summary.scanned.toLocaleString()}/${summary.total.toLocaleString()}, ` +
          `${summary.withGap.toLocaleString()} sitemap(s) with new bios, ${summary.newUrls.toLocaleString()} new URL(s) ` +
          `in ${summary.jobs} job(s), ${summary.noUrls.toLocaleString()} returned no urls, ${summary.errors.toLocaleString()} error(s), ` +
          `state writes ${summary.stateOk.toLocaleString()} ok / ${summary.stateErrors.toLocaleString()} failed` +
          `${summary.stateRejected ? ` (${summary.stateRejected.toLocaleString()} rejected by OpenSearch)` : ''}, ${summary.seconds}s`);
    }
    return summary;
  }

  return { runPass, isRunning: () => running };
};
