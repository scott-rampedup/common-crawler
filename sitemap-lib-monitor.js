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

// The crawl blocklist, applied at DISCOVERY. It used to be read only by bio-etl and live-fleet-shard —
// the last two stages — so a blocked domain was still fetched, parsed, compared, queued to S3, written to
// the ledger, resolved against Common Crawl and shipped to a fleet, and only then dropped. Measured on the
// 2026-11-05 sweep: 80.1% of the 599,335 URLs queued that night were on domains already blocked, and the
// fleet discarded 53,006 of 53,081 (99.86%) on arrival — nine machines booted to crawl 75 URLs.
//
// Worse, it was self-sustaining. A dropped URL never gets a crawl_log outcome, so the ledger keeps
// offering it, so the sweep re-reports the same sitemaps in full every night: realsearch.com.au 48,215
// new of 48,215 seen, topcreditcardprocessors 30,598 of 30,602. Blocking at the source is what breaks
// that loop — the first pass after this writes real outcomes for nothing, but it also stops creating the
// work that was never going to be done.
const blockHostOf = (u) => { try { return new URL(u).hostname.replace(/^www\./i, '').toLowerCase(); } catch (e) { return ''; } };

module.exports.makeLibMonitor = function makeLibMonitor(deps) {
  const { sitemaps, sitemapsClient, contactsClient, contactsIndex, ccEngine, extract,
    directoryRules = {}, genderMap = {}, bioSitemapNames = null, locationSitemapNames = null,
    log = () => {} } = deps;
  let running = false;

  // Which of these page URLs do we already know about?
  //
  // This used to ask the contacts index alone: "is there a contact whose web_source_url is this?" That
  // misses the largest category by far -- a page that was fetched successfully and simply had no person on
  // it produces no contact, so it looked unseen and was re-queued every single night, forever. Measured on
  // 73,068 queued URLs: 67,997 (93%) were already known once the crawl ledger is consulted, and the sweep
  // was re-discovering essentially all of them. That is why 23 Aug produced 6,326,337 "new" URLs.
  //
  // skip-known.knownSet applies both tests -- contacts AND the crawl ledger of pages already attempted --
  // which is the same gate the drain uses. Using a different definition of "known" in the producer than in
  // the consumer is what let the queue compound.
  const { knownSet } = require('./skip-known');
  const ledger = require('./crawl-ledger');
  async function haveSet(urls) {
    if (!contactsClient || !urls.length) return new Set();
    // preProcessCountsAsKnown: a placeholder means we have already recorded this URL, so the sweep must
    // not queue it again. This is what ends the nightly re-discovery loop.
    try { return await knownSet(urls, { client: contactsClient, preProcessCountsAsKnown: true }); }
    catch (e) { return new Set(); }   // unknown -> re-queue: costs a fetch, never loses a page
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
  // kind defaults to '' — EVERY active, monitored sitemap, whatever it was classified as. It used to be
  // 'People', which sounds like scope and behaved like a gate: 11,732 Location rows were excluded and only
  // 2 of them had ever been swept, so "Location sitemaps don't have bios" was never a finding, just an
  // assumption that fetched nothing to test itself. The contract is that a sitemap producing BIO URLs gets
  // scanned, and the only way to know is to look. Extraction already decides — peopleUrls() returns what is
  // actually there — and a sitemap that keeps returning nothing retires itself via ZERO_RETIRE_PASSES, so
  // the cost of being wrong is three passes. Pass kind explicitly to narrow it.
  async function runPass({ liveCap = 300000, conc = 64, flush = 3000, kind = '', type = '',
                           page = 5000, cap = null, maxSitemaps = 0, fetchTimeout = 8000,
                           fetchMaxBytes = 8 * 1024 * 1024, stateFlush = 500, onProgress = null } = {}) {
    if (running) { log('monitor pass already running — skipping'); return { skipped: true }; }
    if (cap != null) liveCap = cap;                       // back-compat with the old {cap} callers
    running = true;
    const startedAt = new Date().toISOString();
    const t0 = Date.now();
    const summary = { startedAt, finishedAt: null, seconds: 0, total: 0, scanned: 0, withGap: 0,
      newUrls: 0, seenUrls: 0, liveQueued: 0, jobs: 0, noUrls: 0, errors: 0, liveCapReached: false, top: [],
      stateOk: 0, stateErrors: 0, stateRejected: 0, stateSample: '', retired: 0, outstanding: null,
      discovered: 0, discoverErrors: 0, blockedSitemaps: 0, blockedUrls: 0, classified: 0, ok: false };
    // One read per pass, not per sitemap. An unreadable list must not silently turn the gate off, so say
    // so loudly: crawling everything is the expensive failure mode this whole change exists to stop.
    let blockedDomains = new Set();
    try {
      blockedDomains = await require('./domain-gate').loadSet(contactsClient);
      log(`domain gate: ${blockedDomains.size} domain(s) blocked — skipped before the fetch`);
    } catch (e) {
      log(`domain gate UNAVAILABLE (${e.message}) — sweeping every domain, including blocked ones`);
    }
    // A sitemap that fetches nothing ONCE is usually a block or a timeout, not a dead sitemap — 9,026 of
    // them fetched 0 on 2026-08-26 having produced an average of 366 URLs before, and monitor_note cannot
    // tell a hard block from an empty file. Retiring on a single zero would have thrown those away. Retire
    // on a RUN of them instead, so a bad night costs one wasted fetch and a genuinely dead sitemap leaves
    // the sweep within a week.
    const ZERO_RETIRE_PASSES = Math.max(1, Number(process.env.SITEMAP_ZERO_RETIRE_PASSES) || 3);
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

    // Recording the discovery belongs HERE, beside the queue write, and not in either caller's extract
    // callback. It was first wired into ui-server's, which the nightly sweep never runs: sitemap-sweep.js
    // builds its own monitor with its own callback and queues straight to S3. The 2026-11-04 pass queued
    // 1,130,898 URLs and wrote 0 ledger entries because of it. One place, so a third caller cannot miss it.
    const ledgerWrites = [];
    // crawl_log predates queued_at and ensureIndex only ever CREATES, so an existing index would leave the
    // new field to dynamic mapping, which types a date by guessing at its first value. Push it once.
    let ledgerReady = null;
    const noteDiscovered = (urls) => {
      if (!contactsClient || !urls.length) return;
      if (!ledgerReady) {
        ledgerReady = ledger.ensureIndex(contactsClient)
          .then(() => ledger.ensureFields(contactsClient))
          .catch(() => {});
      }
      ledgerWrites.push(
        ledgerReady
          .then(() => ledger.recordDiscovered(contactsClient, urls, { source: 'Sitemap Monitor' }))
          .then((r) => { summary.discovered += (r && r.indexed) || 0; summary.discoverErrors += (r && r.errors) || 0; })
          .catch(() => { summary.discoverErrors += urls.length; })
      );
    };

    const doFlush = () => {
      if (!buffer.length) return;
      const urls = buffer; buffer = [];
      // Under the live cap we ask for a live job too; past it, queue only. Either way the URLs are durable.
      const live = summary.liveQueued < liveCap;
      if (live) summary.liveQueued += urls.length; else summary.liveCapReached = true;
      try { extract(urls, `Sitemap Monitor: ${urls.length} new bio URL(s)`, { live }); summary.jobs++; }
      catch (e) { summary.errors++; }
      // After extract, never before: the queue write is the durable record, and a URL must not be marked
      // discovered if handing it to the queue threw.
      noteDiscovered(urls);
    };

    const processOne = async (d) => {
      summary.scanned++;
      // Before the fetch, which is the whole point: the sweep's cost is dominated by fetching and parsing
      // sitemaps, so a domain we have decided not to crawl should not cost us a request. State is still
      // written so last_checked stays honest and the sitemap is not mistaken for one the sweep missed.
      // zero_passes is deliberately NOT touched — "we chose not to look" must not retire a live sitemap
      // as dead, because unblocking the domain has to bring it straight back.
      const smDomain = String(d.domain || '').replace(/^www\./i, '').toLowerCase() || blockHostOf(d.sitemap_url);
      if (smDomain && blockedDomains.has(smDomain)) {
        summary.blockedSitemaps++;
        try { await putState(d.sitemap_url, { last_checked: nowIso, last_new: 0, monitor_note: 'skipped: domain is on the crawl blocklist' }); } catch (e) { /* reporting only */ }
        return;
      }
      try {
        const { watches } = await ccEngine.discoverSitemaps({ urls: [d.sitemap_url], directoryRules, genderMap, bioSitemapNames, locationSitemapNames, _fetchDoc: swFetch });
        let pageUrls = peopleUrls(watches);
        // keyword second pass ONLY when the strict pass got nothing (avoids doubling fetches at scale)
        if (!pageUrls.length) {
          const hints = keywordTokens(d.keyword);
          if (hints.size) { try { const { watches: w2 } = await ccEngine.discoverSitemaps({ urls: [d.sitemap_url], directoryRules, genderMap, bioSitemapNames, locationSitemapNames, keywordHints: hints, _fetchDoc: swFetch }); pageUrls = peopleUrls(w2); } catch (e) { /* */ } }
        }
        let missing = [];
        // Record what was SEEN, not only what was new. last_new alone cannot answer "how many bio URLs are
        // we monitoring" -- the delta is the small end of the number and the interesting one is the base.
        summary.seenUrls += pageUrls.length;
        if (pageUrls.length) { const have = await haveSet(pageUrls); missing = pageUrls.filter((u) => !have.has(u)); }
        else summary.noUrls++;
        // The sitemap's own domain passing the gate does not mean every URL in it does — an index or a
        // directory site lists pages on other hosts. Filter per URL as well, so nothing blocked is queued.
        if (missing.length && blockedDomains.size) {
          const before = missing.length;
          missing = missing.filter((u) => !blockedDomains.has(blockHostOf(u)));
          summary.blockedUrls += before - missing.length;
        }
        if (missing.length) {
          summary.withGap++; summary.newUrls += missing.length;
          byDomain.set(d.domain || d.sitemap_url, (byDomain.get(d.domain || d.sitemap_url) || 0) + missing.length);
          buffer.push(...missing);
          if (buffer.length >= flush) doFlush();
        }
        // last_new_at makes "new tonight" queryable — last_new alone can't distinguish a sitemap that found
        // 12 new bios this pass from one that found 12 a week ago and none since.
        // Consecutive zero-fetch passes, reset the moment the sitemap produces anything again.
        const zeroPasses = pageUrls.length ? 0 : (Number(d.zero_passes) || 0) + 1;
        const retire = zeroPasses >= ZERO_RETIRE_PASSES;
        if (retire) summary.retired++;
        // Settle an unclassified row rather than leaving it unclassified forever. These arrive with kind:''
        // because the classifier returned nothing at LOAD time — a failed fetch, a timeout, an empty file —
        // and a blank kind used to exclude them from the sweep silently. The sweep has just fetched the
        // sitemap and run the same classifier, so if it yielded people URLs the question is answered and the
        // answer is written back. One that keeps yielding nothing retires through zero_passes below.
        const classify = (!String(d.kind || '').trim() && pageUrls.length) ? { kind: 'People' } : {};
        if (classify.kind) summary.classified++;
        await putState(d.sitemap_url, {
          last_checked: nowIso, last_new: missing.length, last_seen_urls: pageUrls.length,
          total_new: (Number(d.total_new) || 0) + missing.length,
          ...(missing.length ? { last_new_at: nowIso } : {}),
          ...classify,
          zero_passes: zeroPasses,
          monitor_note: pageUrls.length ? '' : 'no urls fetched',
          ...(retire ? { monitored: false, monitor_note: `retired: 0 bio URLs on ${zeroPasses} consecutive passes` } : {}),
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
      for await (const rows of sitemaps.monitoredCursor(sitemapsClient, { kind, type, page, notCheckedSince: nowIso })) {
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
      // Discovery writes are fire-and-forget during the pass so they never slow the sweep, but the pass
      // is not finished until they land -- `outstanding` below counts them, and the next pass reads them.
      try { await Promise.all(ledgerWrites); } catch (e) { /* counted per-batch already */ }
      // A sweep is only "ok" if it covered everything AND its writes actually landed. Coverage alone was
      // the old bar, and a pass that stamped nothing still read as a clean run.
      summary.ok = summary.total > 0 && summary.scanned >= summary.total && summary.stateErrors === 0;
      summary.top = [...byDomain.entries()].sort((a, b) => b[1] - a[1]).slice(0, 15).map(([domain, count]) => ({ domain, count }));
      // newUrls is "found tonight". outstanding is "discovered at some point and still not crawled" —
      // the two are different questions and reporting only the first is what made a stalled consumer look
      // like a productive producer for ten weeks.
      try {
        const r = await contactsClient.count({ index: ledger.INDEX, body: { query: { term: { outcome: 'queued' } } } });
        summary.outstanding = ((r.body || r).count);
      } catch (e) { /* reporting only — never fail a pass over it */ }
      log(`nightly sweep done: scanned ${summary.scanned.toLocaleString()}/${summary.total.toLocaleString()}, ` +
          `${summary.seenUrls.toLocaleString()} bio URL(s) monitored, ` +
          `${summary.withGap.toLocaleString()} sitemap(s) with new bios, ${summary.newUrls.toLocaleString()} new URL(s) ` +
          `in ${summary.jobs} job(s), ${summary.noUrls.toLocaleString()} returned no urls, ${summary.errors.toLocaleString()} error(s), ` +
          `state writes ${summary.stateOk.toLocaleString()} ok / ${summary.stateErrors.toLocaleString()} failed` +
          `${summary.stateRejected ? ` (${summary.stateRejected.toLocaleString()} rejected by OpenSearch)` : ''}, ` +
          `${summary.retired.toLocaleString()} retired after ${ZERO_RETIRE_PASSES} zero passes, ` +
          `${summary.discovered.toLocaleString()} discovery ledger write(s)` +
          `${summary.discoverErrors ? ` / ${summary.discoverErrors.toLocaleString()} failed` : ''}, ` +
          `${summary.outstanding == null ? 'outstanding unknown' : `${summary.outstanding.toLocaleString()} outstanding (queued, not yet crawled)`}, ` +
          `${summary.seconds}s`);
    }
    return summary;
  }

  return { runPass, isRunning: () => running };
};
