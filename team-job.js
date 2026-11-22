/**
 * team-job.js — run a list of TEAM PAGES as a Data Importer job.
 *
 * A Webpages job assumes one page = one person, which is exactly what a team page is not. This runs
 * the same URL list through team-extract instead: segment the roster, bind each person's details
 * inside their own card, and emit ONE RECORD PER PERSON in the engine's canonical column shape.
 *
 * Emitting the standard shape is the whole trick — once a person is a normal record the rest of the
 * pipeline already works on them unchanged: vCard enrichment, modelMissingEmails (which learns the
 * domain's pattern from stored Professional emails and verifies the result), geocoding, and the
 * email-keyed merge into the Master DB. Nothing about modelling needs rebuilding for team pages; a
 * record only has to arrive with First, Last, Gender and a Domain to qualify.
 *
 *   runTeamPages(urls, { genderMap, onRecord, onProgress, shouldStop, concurrency })
 */
const ex = require('./extractor');
const cc = require('./cc-engine');
const te = require('./team-extract');

const TIMEOUT = Number(process.env.TEAM_FETCH_TIMEOUT) || 25000;

async function fetchPage(url) {
  const race = (p) => Promise.race([p, new Promise((r) => setTimeout(() => r(''), TIMEOUT))]);
  let html = '';
  try { html = await race(cc.liveFetchPage(url)); } catch (e) { /* try the other fetcher */ }
  // The two fetchers are blocked by different sites, so a miss on one is worth retrying on the other.
  if (!html || String(html).length < 500) {
    try { const alt = await race(cc.fetchDoc(url)); if (alt && String(alt).length > String(html || '').length) html = alt; }
    catch (e) { /* keep the first result */ }
  }
  html = String(html || '');
  return html.length >= 500 ? html : '';
}

/** A team-extract person -> the engine's canonical record row. */
function toRecord(p, pageUrl, genderMap, nowIso) {
  const evidence = pageUrl;                       // where we actually SAW this person
  const domain = p.domain || ex.getBaseDomain(evidence) || '';
  const cc2 = ex.countryCodeFromDomain ? ex.countryCodeFromDomain(domain) : '';
  const email = String(p.email || '').toLowerCase();
  const row = {
    'Time Stamp': nowIso,
    'Source': 'Team Page',
    'Web Source URL': evidence,
    'Directory': '',
    // Prefer the person's own page for the path fields when the card linked to one -- that is the
    // bio URL, and it is what the directory/path classifiers are built to read.
    'Path ID': ex.pathIdFromUrl(p.bio_url || evidence) || '',
    'Domain': domain,
    'Last Path': ex.lastPathFromUrl ? (ex.lastPathFromUrl(p.bio_url || evidence) || '') : '',
    'Bio Check': p.confidence || '',
    'First': p.first || '',
    'Last': p.last || '',
    // Gender is not cosmetic here: modelMissingEmails only models for records that have one, so a
    // person without a gender lookup silently loses the modelled address the user asked for.
    'Gender': ex.genderFromMap(genderMap, String(p.first || '').toLowerCase()),
    'Title': '',
    'Position': p.title || '',
    'Description': '',
    'Email Address': email,
    'Email Type': email ? ex.classifyEmail(email) : '',
    'LinkedIn URL': p.linkedin_url || '',
    'Facebook': '', 'Twitter': '', 'WhatsApp': '', 'Google Maps': '',
    'vCard': '',
    'Phone': p.phone ? (ex.toE164(p.phone, cc2) || p.phone) : '',
    'Phone Type': '', 'Phone Location': '', 'Phone 2': '', 'Phone 2 Type': '',
    'Type': '',
  };
  return row;
}

async function runTeamPages(urls, opts = {}) {
  const { genderMap = {}, onRecord = () => {}, onProgress = () => {}, shouldStop = () => false } = opts;
  const CONC = Number(opts.concurrency) || Number(process.env.TEAM_CONCURRENCY) || 8;
  const q = (urls || []).slice();
  const nowIso = new Date().toISOString();
  const stats = { pages: q.length, fetched: 0, people: 0, bioUrls: new Set(), dropped: 0 };

  await Promise.all(Array.from({ length: Math.min(CONC, Math.max(1, q.length)) }, async () => {
    while (q.length) {
      if (shouldStop()) break;
      const url = q.pop();
      if (!url) break;
      let html = '';
      try { html = await fetchPage(url); } catch (e) { /* counted as empty below */ }
      if (!html) { onProgress({ domain: url, status: 'no-candidates', source: 'Team Page' }); continue; }
      stats.fetched++;
      let out;
      try { out = te.extractPeople(html, url, { firsts: opts.firsts }); }
      catch (e) { onProgress({ domain: url, status: 'no-candidates', source: 'Team Page' }); continue; }
      const people = (out && out.people) || [];
      const meta = (out && out.meta) || {};
      stats.dropped += meta.uncorroborated || 0;
      for (const p of people) {
        if (p.bio_url) stats.bioUrls.add(p.bio_url);
        stats.people++;
        onRecord(toRecord(p, url, genderMap, nowIso));
      }
      onProgress({ domain: url, status: people.length ? 'domain-done' : 'no-candidates',
        source: 'Team Page', people: people.length, method: meta.method || '' });
    }
  }));

  return { pages: stats.pages, fetched: stats.fetched, people: stats.people,
    bioUrls: [...stats.bioUrls], dropped: stats.dropped };
}

module.exports = { runTeamPages, toRecord };
