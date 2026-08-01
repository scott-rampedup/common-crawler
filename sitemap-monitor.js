/**
 * sitemap-monitor.js — nightly new-employee detection via bio-dedicated child sitemaps
 * ------------------------------------------------------------------------------------
 * The cheap monitoring loop the SCALING-ROADMAP calls the "killer feature": re-fetch each
 * watched child sitemap on a schedule, diff its URL set against a stored baseline, and treat
 *   - a NEW bio URL      => candidate NEW HIRE   (extract it -> contact)
 *   - a DISAPPEARED URL  => candidate DEPARTURE  (flagged in the change feed)
 * Only the DELTA is extracted, so monitoring a directory costs ~nothing per night.
 *
 * Two cost levers keep a pass cheap:
 *   1. We watch only the child sitemaps that are DEDICATED to people pages (agents-sitemap.xml),
 *      found by cc-engine.discoverBioSitemaps — never the whole tree.
 *   2. <lastmod> SKIP: a child whose <lastmod> in its parent index is unchanged is not refetched;
 *      a child with no lastmod is refetched but its content hash short-circuits the diff.
 *
 * Pure orchestration with injected deps (db, engine, fetchDoc, extract, log, hash, now) so the
 * whole thing is offline-testable: `node sitemap-monitor.js --selftest`.
 */

function djb2(s) {                                   // tiny content hash (change detector; not crypto)
  s = String(s || ''); let h = 5381;
  for (let i = 0; i < s.length; i++) h = ((h << 5) + h + s.charCodeAt(i)) | 0;
  return (h >>> 0).toString(36);
}

function makeMonitor(deps = {}) {
  const {
    db,                                              // makeDb(...) instance
    engine,                                          // require('./cc-engine')
    fetchDoc,                                        // async (url) => body string ('' on failure)
    extract = null,                                  // async (urls, label) => void  (prod: start a webpage job)
    directoryRules = {},
    genderMap = {},
    bioSitemapNames = null,                          // Set of known people/bio sitemap filenames (fast-path)
    log = () => {},
    hash = djb2,
    now = () => new Date().toISOString(),
    extractBatch = Number(process.env.MONITOR_EXTRACT_BATCH) || 2000,   // URLs handed to `extract` per call/cursor-advance
    extractMaxPerPass = Number(process.env.MONITOR_EXTRACT_MAX) || 100000, // bound one pass's extraction (backlog drains over passes)
  } = deps;

  if (!db || !engine || typeof fetchDoc !== 'function') {
    throw new Error('makeMonitor: db, engine and fetchDoc are required');
  }

  // Resolve a bare domain to its sitemap URL(s): robots.txt `Sitemap:` lines, else common fallbacks.
  async function resolveSitemaps(domain) {
    const host = String(domain || '').trim()
      .replace(/^https?:\/\//i, '').replace(/\/.*$/, '').replace(/^www\./i, '').toLowerCase();
    if (!host) return [];
    const found = new Set();
    for (const base of [`https://${host}`, `https://www.${host}`]) {
      const robots = await fetchDoc(`${base}/robots.txt`);
      if (robots) {
        const re = /^\s*sitemap:\s*(\S+)/gim; let m;
        while ((m = re.exec(robots))) { const u = m[1].trim(); if (u) found.add(u); }
        if (found.size) break;
      }
    }
    if (!found.size) for (const p of ['/sitemap_index.xml', '/sitemap.xml']) found.add(`https://${host}${p}`);
    return [...found];
  }

  // Discover bio-dedicated child sitemaps for the given domains/sitemap URLs, register each as a
  // watch, and SEED its baseline (no observations — the initial roster isn't "new hires"). Returns
  // a per-watch summary.
  async function discoverWatches({ domains = [], sitemaps = [], minBioRatio, minBioCount } = {}) {
    const seeds = new Set();
    for (const s of sitemaps) { const t = String(s || '').trim(); if (t) seeds.add(t); }
    for (const d of domains) for (const sm of await resolveSitemaps(d)) seeds.add(sm);
    if (!seeds.size) return { added: 0, watches: [] };

    const opts = { urls: [...seeds], directoryRules, genderMap, bioSitemapNames, _fetchDoc: fetchDoc };
    if (minBioRatio != null) opts.minBioRatio = minBioRatio;
    if (minBioCount != null) opts.minBioCount = minBioCount;
    const { watches } = await engine.discoverBioSitemaps(opts);

    const out = [];
    for (const w of watches) {
      db.upsertWatch(w);
      const seeded = db.syncSitemapUrls(w.sitemapUrl, w.domain, w.bioUrls, { seed: true });
      // Seed change-detection from the parent index's <lastmod> only; leave last_hash null (we don't
      // retain the raw child XML here). The first pass sets the content hash from the real fetch.
      db.setWatchState(w.sitemapUrl, { lastLastmod: w.lastmod || null, lastFetched: now() });
      out.push({ sitemapUrl: w.sitemapUrl, domain: w.domain, bioCount: w.bioCount,
        bioRatio: Number(w.bioRatio.toFixed(3)), seeded: seeded.present });
      log(`watch + ${w.sitemapUrl} (${w.bioCount} bios, ratio ${w.bioRatio.toFixed(2)})`);
    }
    return { added: out.length, watches: out };
  }

  // Compute one watched child's CURRENT bio URLs from already-fetched XML (reuses the engine's
  // exact URL filtering/normalization by feeding the XML inline).
  async function bioEntriesFromXml(xml, sitemapUrl = '') {
    const r = await engine.discoverBioSitemaps({
      content: xml, sourceUrl: sitemapUrl, bioSitemapNames,   // sourceUrl lets a known-bio filename match inline content
      minBioCount: 0, minBioRatio: 0, directoryRules, genderMap, _fetchDoc: fetchDoc,
    });
    return r.watches.length ? r.watches[0].bioUrls : [];
  }

  // Extract detected deltas in crash-safe batches, driven by a PERSISTED cursor over the observation
  // feed (extract_cursor = last observation id handed off). The cursor only advances after a batch is
  // successfully handed to `extract`, so an interrupted pass resumes exactly here next time (self-
  // healing) and the seeded baseline — which emits no observations — is never extracted. Idempotent:
  // `extract` upserts by email and its jobs auto-resume, so a re-run of an in-flight batch is safe.
  async function drainExtractions() {
    if (!extract) return 0;
    let cursor = Number(db.getMonitorMeta('extract_cursor', 0)) || 0;
    let done = 0;
    while (done < extractMaxPerPass) {
      const batch = db.pendingExtractions({ afterId: cursor, limit: extractBatch });
      if (!batch.length) break;
      const urls = [...new Set(batch.map((r) => r.url))];
      try {
        await extract(urls, `Monitor: ${urls.length} new bio URL(s)`);
        db.markExtracted(urls);
      } catch (e) { log(`extract batch failed (cursor held, retries next pass): ${e.message}`); break; }
      cursor = batch[batch.length - 1].id;                 // advance ONLY past a handed-off batch
      db.setMonitorMeta('extract_cursor', String(cursor));
      done += urls.length;
    }
    if (done) log(`extracted ${done} delta bio URL(s) (cursor -> ${cursor})`);
    return done;
  }

  // One monitoring pass over every active watch. Returns a summary; records observations for the delta
  // and hands new/reappeared bios to extraction via the persisted cursor. `force` ignores the
  // <lastmod>/hash skip (re-diff everything).
  async function runMonitorPass({ force = false } = {}) {
    const watches = db.activeWatches();
    const summary = { scanned: 0, skipped: 0, fetchFailed: 0, changed: 0,
      newBios: 0, reappeared: 0, departed: 0, extracted: 0, errors: 0 };

    // Recover first: drain any deltas detected but not yet extracted (e.g. a prior pass was killed
    // mid-run before its extraction ran). Cheap when there's no backlog.
    summary.extracted += await drainExtractions();

    // Group by parent index so each parent is fetched once; its child <lastmod>s drive the skip.
    const groups = new Map();
    for (const w of watches) { const k = w.parent_url || ''; (groups.get(k) || groups.set(k, []).get(k)).push(w); }

    for (const [parentKey, ws] of groups) {
      let childLastmod = null;
      if (parentKey) {
        try {
          const px = await fetchDoc(parentKey);
          if (px) { const { entries } = engine.extractSitemapLocs(px); childLastmod = new Map(entries.map((e) => [e.loc, e.lastmod])); }
        } catch (e) { log(`parent fetch failed ${parentKey}: ${e.message}`); }
      }
      for (const w of ws) {
        summary.scanned++;
        try {
          const curLastmod = childLastmod ? (childLastmod.get(w.sitemap_url) || null) : null;
          // <lastmod> skip: parent says this child is unchanged -> don't even fetch it.
          if (!force && curLastmod != null && w.last_lastmod != null && curLastmod === w.last_lastmod) {
            summary.skipped++; continue;
          }
          const xml = await fetchDoc(w.sitemap_url);
          if (!xml) { summary.fetchFailed++; log(`fetch failed (skip diff) ${w.sitemap_url}`); continue; }
          const contentHash = hash(xml);
          // Content-hash skip: no lastmod but bytes identical -> nothing changed.
          if (!force && w.last_hash && contentHash === w.last_hash) {
            db.setWatchState(w.sitemap_url, { lastLastmod: curLastmod || w.last_lastmod, lastHash: contentHash, lastFetched: now() });
            summary.skipped++; continue;
          }
          const currentEntries = await bioEntriesFromXml(xml, w.sitemap_url);
          const diff = db.syncSitemapUrls(w.sitemap_url, w.domain, currentEntries);
          if (diff.newUrls.length || diff.reappearedUrls.length || diff.departedUrls.length) summary.changed++;
          summary.newBios += diff.newUrls.length;
          summary.reappeared += diff.reappearedUrls.length;
          summary.departed += diff.departedUrls.length;
          // NB: no in-memory extract list — syncSitemapUrls already persisted new_bio/reappeared
          // observations, and drainExtractions() (below + start of next pass) turns them into contacts.
          db.setWatchState(w.sitemap_url, { lastLastmod: curLastmod || w.last_lastmod, lastHash: contentHash, lastFetched: now() });
          if (diff.newUrls.length || diff.departedUrls.length) {
            log(`${w.sitemap_url}: +${diff.newUrls.length} new, -${diff.departedUrls.length} departed`);
          }
        } catch (e) { summary.errors++; log(`watch error ${w.sitemap_url}: ${e.message}`); }
      }
    }

    // Extract this pass's freshly-detected deltas (new + reappeared) via the same cursor drain, so a
    // completed pass lands its new hires immediately; anything left over is picked up next pass.
    summary.extracted += await drainExtractions();
    log(`monitor pass: scanned ${summary.scanned}, skipped ${summary.skipped}, new ${summary.newBios}, departed ${summary.departed}, extracted ${summary.extracted}`);
    return summary;
  }

  // Run a pass now, then every intervalHours. Guards against overlapping passes. Returns the timer.
  function schedule({ intervalHours = 24, runOnStart = false } = {}) {
    let running = false;
    const tick = async () => {
      if (running) { log('monitor pass already running — skipping tick'); return; }
      running = true;
      try { await runMonitorPass(); } catch (e) { log(`monitor pass crashed: ${e.message}`); }
      finally { running = false; }
    };
    if (runOnStart) tick();
    const timer = setInterval(tick, Math.max(1, intervalHours) * 3600 * 1000);
    if (timer.unref) timer.unref();
    return timer;
  }

  return { resolveSitemaps, discoverWatches, runMonitorPass, schedule, bioEntriesFromXml };
}

module.exports = { makeMonitor, djb2 };

// ---------------------------------------------------------------------------------------------
// Offline selftest: real temp SQLite DB + the real engine + a fake fetchDoc. Exercises seed ->
// no-op pass -> new-hire detection (+extraction) -> departure -> <lastmod> skip.
// ---------------------------------------------------------------------------------------------
if (require.main === module && process.argv.includes('--selftest')) {
  (async () => {
    const fs = require('fs'); const os = require('os'); const path = require('path');
    const engine = require('./cc-engine');
    const { makeDb } = require('./db');
    let pass = 0, fail = 0;
    const ok = (name, cond) => { if (cond) { pass++; console.log('  ✓', name); } else { fail++; console.log('  ✗', name); } };

    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mon-'));
    const _log = console.log; console.log = () => {};                 // silence makeDb boot logs
    const db = makeDb(dir);
    console.log = _log;

    const H = 'agency.com';
    const PARENT = `https://${H}/sitemap_index.xml`;
    const AGENTS = `https://${H}/agents.xml`;
    const BLOG = `https://${H}/blog.xml`;

    // Mutable fake site. agents.xml starts with 3 agents (>= default minBioCount); blog.xml is
    // never bio-dedicated.
    let agentsUrls = ['jane-doe', 'john-roe', 'amy-poe'];
    let parentAgentsLastmod = '2026-06-20';
    const agentsXml = () => `<urlset>${agentsUrls.map((s) =>
      `<url><loc>https://${H}/agents/${s}/</loc><lastmod>2026-06-20</lastmod></url>`).join('')}</urlset>`;
    const parentXml = () => `<sitemapindex>` +
      `<sitemap><loc>${AGENTS}</loc><lastmod>${parentAgentsLastmod}</lastmod></sitemap>` +
      `<sitemap><loc>${BLOG}</loc><lastmod>2026-01-01</lastmod></sitemap></sitemapindex>`;
    const docs = () => ({
      [`https://${H}/robots.txt`]: `User-agent: *\nSitemap: ${PARENT}\n`,
      [PARENT]: parentXml(),
      [AGENTS]: agentsXml(),
      [BLOG]: `<urlset><url><loc>https://${H}/blog/a/</loc></url><url><loc>https://${H}/news/b/</loc></url></urlset>`,
    });
    const fetchDoc = async (u) => docs()[u] || '';

    const extracted = [];
    const extract = async (urls) => { extracted.push(...urls); };

    const mon = makeMonitor({ db, engine, fetchDoc, extract, log: () => {} });

    // 1) discovery: only agents.xml is bio-dedicated; baseline seeded WITHOUT observations.
    const disc = await mon.discoverWatches({ domains: [H] });
    ok('discoverWatches registers only the bio-dedicated child', disc.added === 1 && disc.watches[0].sitemapUrl === AGENTS);
    ok('seed populates the baseline (3 present bio urls)', db.listWatches()[0].present_count === 3);
    ok('seeding emits NO observations', (db.recentObservations().length === 0));

    // 2) unchanged pass -> nothing new, child skipped via <lastmod> match.
    const p1 = await mon.runMonitorPass();
    ok('unchanged pass finds 0 new bios', p1.newBios === 0 && p1.departed === 0);
    ok('unchanged pass SKIPS the child (lastmod match)', p1.skipped === 1 && p1.scanned === 1);

    // 3) a NEW agent appears; parent lastmod bumps so the child is re-checked.
    agentsUrls = ['jane-doe', 'john-roe', 'amy-poe', 'zoe-kim'];
    parentAgentsLastmod = '2026-06-25';
    const p2 = await mon.runMonitorPass();
    ok('new-hire pass detects exactly 1 new bio', p2.newBios === 1 && p2.changed === 1 && p2.skipped === 0);
    ok('new bio URL is queued for extraction', extracted.length === 1 && extracted[0] === `https://${H}/agents/zoe-kim/`);
    ok('a new_bio observation is recorded', db.recentObservations({ event: 'new_bio' }).length === 1);
    ok('baseline now holds 4 present bios', db.listWatches()[0].present_count === 4);
    ok('the new bio is marked extracted', p2.extracted === 1);

    // 4) an agent DEPARTS; parent lastmod bumps again.
    agentsUrls = ['jane-doe', 'amy-poe', 'zoe-kim'];   // john-roe gone
    parentAgentsLastmod = '2026-06-26';
    const p3 = await mon.runMonitorPass();
    ok('departure pass detects exactly 1 departed', p3.departed === 1 && p3.newBios === 0);
    ok('a departed observation is recorded', db.recentObservations({ event: 'departed' }).length === 1);
    ok('baseline now holds 3 present + 1 departed', db.listWatches()[0].present_count === 3 && db.listWatches()[0].departed_count === 1);

    // 5) lastmod skip holds even when content WOULD differ (proves we trust the parent lastmod).
    agentsUrls = ['jane-doe', 'amy-poe', 'zoe-kim', 'max-lee'];   // would be a new hire...
    // parentAgentsLastmod unchanged (2026-06-26) -> pass must SKIP and NOT see max.
    const p4 = await mon.runMonitorPass();
    ok('unchanged lastmod skips the child even though content changed', p4.skipped === 1 && p4.newBios === 0);
    const p5 = await mon.runMonitorPass({ force: true });   // force re-diff picks max up
    ok('force pass bypasses the skip and finds the held-back hire', p5.newBios === 1);

    try { fs.rmSync(dir, { recursive: true, force: true }); } catch (e) { /* temp */ }
    console.log(`\n${pass} passed, ${fail} failed`);
    process.exit(fail ? 1 : 0);
  })().catch((e) => { console.error(e); process.exit(1); });
}
