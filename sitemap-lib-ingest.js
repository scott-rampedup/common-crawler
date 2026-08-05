/**
 * sitemap-lib-ingest.js — bridge from Data Ingest -> Sitemap Library. Classifies submitted sitemaps with
 * the SAME engine the discovery driver uses (cc-engine.discoverSitemaps + the People/Location filename
 * lexicons + gender lexicon) and upserts them into the Library (source='imported'). Records ANY submitted
 * sitemap: the ones that classify land as People/Location; a submitted leaf that doesn't classify lands as
 * an Unknown row (admin can reclassify or delete). Shared by the live ingest endpoints and the backfill.
 *
 * A submitted sitemap-INDEX is expanded by discoverSitemaps into child watches, so its children are what
 * get recorded (not the index URL itself) — matching how the Library is normally populated.
 */

// Classify + upsert one batch of submitted sitemap URLs (optionally raw pasted content). Returns
// { submitted, classified, unknown, upserted, errors }.
async function ingestSitemapsToLibrary(deps) {
  const { sitemaps, sitemapsClient, ccEngine, urls = [], content = '',
    genderMap = {}, bioSitemapNames = null, locationSitemapNames = null, directoryRules = {},
    source = 'imported', nowIso = null, log = () => {} } = deps;
  if (!sitemapsClient) return { submitted: 0, classified: 0, unknown: 0, upserted: 0, errors: 0 };

  const submitted = [...new Set((urls || []).map((u) => String(u || '').trim()).filter(Boolean))];
  const hasContent = !!String(content || '').trim();
  if (!submitted.length && !hasContent) return { submitted: 0, classified: 0, unknown: 0, upserted: 0, errors: 0 };
  const now = nowIso || new Date().toISOString();

  let watches = [];
  try {
    // Pasted content has no real URL to key on, so only feed inline content when no URLs were submitted.
    const opts = { urls: submitted, directoryRules, genderMap, bioSitemapNames, locationSitemapNames };
    if (!submitted.length && hasContent) opts.content = content;
    const r = await ccEngine.discoverSitemaps(opts);
    watches = r.watches || [];
  } catch (e) { log('discover failed: ' + e.message); }

  const docs = [];
  const classifiedUrls = new Set();
  const parentUrls = new Set();
  for (const w of watches) {
    docs.push(sitemaps.docFromWatch(w, { source }));
    classifiedUrls.add(w.sitemapUrl);
    if (w.parentUrl) parentUrls.add(w.parentUrl);
  }
  // "Any sitemap": also record each submitted leaf that produced no watch (skip indexes we expanded into
  // children, identified by a child watch pointing back to it as its parent).
  let unknown = 0;
  for (const u of submitted) {
    if (classifiedUrls.has(u) || parentUrls.has(u)) continue;
    docs.push(sitemaps.docFromUrl(u, { source }));
    unknown++;
  }

  if (!docs.length) return { submitted: submitted.length, classified: watches.length, unknown, upserted: 0, errors: 0 };
  let upserted = 0, errors = 0;
  for (let i = 0; i < docs.length; i += 500) {                       // bound each bulk call
    const r = await sitemaps.bulkUpsert(sitemapsClient, docs.slice(i, i + 500), now);
    upserted += r.upserted; errors += r.errors;
  }
  return { submitted: submitted.length, classified: watches.length, unknown, upserted, errors };
}

module.exports = { ingestSitemapsToLibrary };
