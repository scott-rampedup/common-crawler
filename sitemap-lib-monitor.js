/**
 * sitemap-lib-monitor.js — the Sitemap Library's built-in monitor (gap-fill model). For each opt-in
 * (monitored=true) People sitemap, re-fetch it and extract only the page URLs we DON'T already have a
 * contact for — the contacts DB is the baseline, so this closes the "Have vs Pages" gap and lands new
 * hires (Source='Sitemap Monitor'). Replaces the old SQLite watched_sitemaps monitor.
 * Phase 1 = People -> contacts. (Location -> companies is a later phase.)
 */
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

  // One bounded pass over monitored People sitemaps, least-recently-checked first. `cap` = max URLs
  // handed to extraction this pass (the rest resume next pass via the last_checked ordering).
  async function runPass({ cap = 5000 } = {}) {
    if (running) { log('monitor pass already running — skipping'); return { skipped: true }; }
    running = true;
    const summary = { scanned: 0, withGap: 0, extracted: 0, errors: 0 };
    try {
      const rows = await sitemaps.monitoredBatch(sitemapsClient, 2000, 'People');
      const nowIso = new Date().toISOString();
      for (const d of rows) {
        if (summary.extracted >= cap) break;
        summary.scanned++;
        try {
          const { watches } = await ccEngine.discoverSitemaps({ urls: [d.sitemap_url], directoryRules, genderMap, bioSitemapNames, locationSitemapNames });
          // Phase 1 = People. A leaf People sitemap yields one watch; a People Parent index expands to
          // several child watches — collect page URLs across all of them (deduped).
          const pageUrls = [...new Set((watches || []).filter((w) => w.kind === 'People').flatMap((w) => (w.urls || []).map((u) => u.url)))];
          let missing = [];
          if (pageUrls.length) {
            const have = await haveSet(pageUrls);
            missing = pageUrls.filter((u) => !have.has(u));
            if (missing.length > cap - summary.extracted) missing = missing.slice(0, cap - summary.extracted);
          }
          if (missing.length) {
            summary.withGap++;
            try { extract(missing, 'Sitemap Monitor: ' + (d.domain || '')); summary.extracted += missing.length; } catch (e) { summary.errors++; }
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
      }
      try { await sitemapsClient.indices.refresh({ index: sitemaps.INDEX }); } catch (e) { /* */ }
      log(`monitor pass: scanned ${summary.scanned}, gaps ${summary.withGap}, extracted ${summary.extracted}, errors ${summary.errors}`);
    } finally { running = false; }
    return summary;
  }

  return { runPass, isRunning: () => running };
};
