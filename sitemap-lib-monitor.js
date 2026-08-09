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

  // One pass over the Library's People sitemaps (least-recently-checked first). Re-fetches each, finds the
  // page URLs we DON'T already have a contact for (delta), and hands them to extraction — so every sitemap
  // is re-checked for new hires. Concurrency for throughput; missing URLs are batched into a few big jobs
  // (not one per sitemap). `cap` bounds URLs enqueued per pass; last_checked ordering rotates the rest.
  async function runPass({ cap = 300000, conc = 16, batch = 50000, flush = 3000 } = {}) {
    if (running) { log('monitor pass already running — skipping'); return { skipped: true }; }
    running = true;
    const summary = { scanned: 0, withGap: 0, extracted: 0, jobs: 0, errors: 0 };
    const nowIso = new Date().toISOString();
    let buffer = [];
    const doFlush = () => { if (!buffer.length) return; const urls = buffer; buffer = []; try { extract(urls, `Sitemap Monitor: ${urls.length} new bio URL(s)`); summary.jobs++; } catch (e) { summary.errors++; } };

    const processOne = async (d) => {
      if (summary.extracted >= cap) return;
      summary.scanned++;
      try {
        const { watches } = await ccEngine.discoverSitemaps({ urls: [d.sitemap_url], directoryRules, genderMap, bioSitemapNames, locationSitemapNames });
        let pageUrls = peopleUrls(watches);
        // keyword second pass ONLY when the strict pass got nothing (avoids doubling fetches at scale)
        if (!pageUrls.length) {
          const hints = keywordTokens(d.keyword);
          if (hints.size) { try { const { watches: w2 } = await ccEngine.discoverSitemaps({ urls: [d.sitemap_url], directoryRules, genderMap, bioSitemapNames, locationSitemapNames, keywordHints: hints }); pageUrls = peopleUrls(w2); } catch (e) { /* */ } }
        }
        let missing = [];
        if (pageUrls.length) { const have = await haveSet(pageUrls); missing = pageUrls.filter((u) => !have.has(u)); }
        if (missing.length && summary.extracted < cap) {
          const room = cap - summary.extracted;
          if (missing.length > room) missing = missing.slice(0, room);
          summary.withGap++; summary.extracted += missing.length;
          buffer.push(...missing);
          if (buffer.length >= flush) doFlush();
        }
        await sitemaps.setMonitorState(sitemapsClient, d.sitemap_url, {
          last_checked: nowIso, last_new: missing.length,
          total_new: (Number(d.total_new) || 0) + missing.length,
          monitor_note: pageUrls.length ? '' : 'no urls fetched',
        });
      } catch (e) {
        summary.errors++;
        try { await sitemaps.setMonitorState(sitemapsClient, d.sitemap_url, { last_checked: nowIso, monitor_note: String(e.message || 'error').slice(0, 200) }); } catch (e2) { /* */ }
      }
    };

    try {
      const rows = await sitemaps.monitoredBatch(sitemapsClient, batch, 'People');
      log(`monitor pass: ${rows.length} sitemap(s) this pass (conc ${conc}, cap ${cap})`);
      // process in concurrent chunks
      for (let i = 0; i < rows.length && summary.extracted < cap; i += conc) {
        await Promise.all(rows.slice(i, i + conc).map(processOne));
      }
      doFlush();
      try { await sitemapsClient.indices.refresh({ index: sitemaps.INDEX }); } catch (e) { /* */ }
      log(`monitor pass done: scanned ${summary.scanned}, gaps ${summary.withGap}, extracted ${summary.extracted} in ${summary.jobs} job(s), errors ${summary.errors}`);
    } finally { running = false; }
    return summary;
  }

  return { runPass, isRunning: () => running };
};
