/**
 * cc-home-enrich.js — enrich a company from its Common Crawl HOME PAGE.
 * -------------------------------------------------------------------------------------------------
 * resolveHome(domain) -> CC WARC pointer for the home page (latest crawl, falling back to recent ones).
 * parseHome(html)     -> { description, phone, email, facebook, instagram, map, linkedin[], bio[],
 *                          alternateWebsites[], emails[], phones[] } pulled from the archived home page.
 * buildContacts(...)  -> [ "First, Last, Gender, Email, Address, LinkedIn, BIO;" ] — one per person that
 *                        can be assembled from >=2 of {LinkedIn/in, Email, BIO URL} that agree on a name
 *                        AND whose first name resolves to a gender. count = number of unique contacts.
 * enrichCompany(...)  -> the full field set for the Company Crawler, applying the move/delete rules.
 *
 * Reuses cc-engine (queryIndexUrl/fetchWarc/isBioOrContactUrl) + extractor (name/gender/email).
 */
const cc = require('./cc-engine');
const ex = require('./extractor');

// Websites that belong in "Alternate Websites", not the primary Website (seed list; the Admin UI can edit it).
const ALT_DEFAULT = ['linktr.ee', 'youtube.com', 'crunchbase.com', 'twitter.com', 'x.com', 'yelp.com', 'etsy.com', 'about.me', 'sites.google.com', 'amazon.com/shop', 'wix.com', 'wixsite.com'];

const cap = (s) => String(s || '').charAt(0).toUpperCase() + String(s || '').slice(1);
const EMAIL_RE = /[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi;
const PHONE_RE = /\+?\d[\d().\-\s]{7,}\d/g;

function hrefsFromHtml(html) {
  const out = []; const re = /<a\b[^>]*\bhref\s*=\s*["']([^"']+)["']/gi; let m;
  while ((m = re.exec(html)) && out.length < 4000) out.push(m[1]);
  return out;
}
function metaDescription(html) {
  const m = html.match(/<meta[^>]+(?:name|property)\s*=\s*["'](?:description|og:description)["'][^>]*\bcontent\s*=\s*["']([^"']*)["']/i)
    || html.match(/<meta[^>]+\bcontent\s*=\s*["']([^"']*)["'][^>]*(?:name|property)\s*=\s*["'](?:description|og:description)["']/i);
  return m ? m[1].replace(/\s+/g, ' ').trim().slice(0, 600) : '';
}
// a social/media URL "has a path" if there's a real segment after the host (not just "/" or empty).
function hasPath(u) { try { const x = new URL(/^https?:/i.test(u) ? u : 'https://' + u); return x.pathname.replace(/^\/+|\/+$/g, '').length > 0; } catch { return false; } }
const clean = (u) => String(u || '').split('#')[0].trim();
const stripQ = (u) => String(u || '').split('?')[0];

// Does a website URL belong to the alternate-website list?
function isAlternate(u, list) {
  const low = String(u || '').toLowerCase().replace(/^https?:\/\//, '').replace(/^www\./, '');
  return (list || ALT_DEFAULT).some((p) => { p = p.toLowerCase(); return p.includes('/') ? low.startsWith(p) || low.includes('/' + p.split('/').slice(1).join('/')) && low.split('/')[0].includes(p.split('/')[0]) : low.split('/')[0] === p || low.split('/')[0].endsWith('.' + p); });
}

// Resolve the company home page to a CC pointer, trying a few URL forms across recent crawls.
async function resolveHome(domain, { crawls, demoMode = false } = {}) {
  const d = String(domain || '').toLowerCase().replace(/^www\./, '').split('/')[0];
  if (!d) return null;
  const cr = crawls && crawls.length ? crawls : [cc.CRAWL || 'CC-MAIN-2026-25'];
  const forms = [`https://${d}/`, `https://www.${d}/`, `http://${d}/`, `http://www.${d}/`];
  for (const crawl of cr) {
    for (const url of forms) {
      try { const rec = await cc.queryIndexUrl(url, { crawl, demoMode }); if (rec && rec.filename) return rec; } catch (e) { /* try next */ }
    }
  }
  return null;
}

// Parse the home-page HTML into the raw field buckets. altList = the (admin-editable) alternate-website patterns.
function parseHome(html, domain, altList) {
  const out = { description: '', facebook: '', instagram: '', map: '', linkedin: [], bio: [], alternateWebsites: [], emails: [], phones: [] };
  if (!html) return out;
  out.description = metaDescription(html);
  const selfHost = String(domain || '').toLowerCase().replace(/^www\./, '');
  const seenLi = new Set(), seenBio = new Set(), seenAlt = new Set();
  for (const raw of hrefsFromHtml(html)) {
    let href = clean(raw); if (!href) continue;
    if (href.startsWith('//')) href = 'https:' + href;                       // protocol-relative -> https:
    else if (/^(facebook|instagram|linkedin|twitter|x|youtube)\.com\//i.test(href)) href = 'https://' + href;  // scheme-less social
    const low = href.toLowerCase();
    // skip social SHARE / intent / plugin buttons (not the company's own page)
    if (/(sharer|share\.php|\/sharearticle|\/share\?|\/intent\/|\/dialog\/|\/plugins\/|\/sharing\/|\/tweet\b|addtoany|sharelink|utm_source=share)/i.test(low)) continue;
    if (/^mailto:/i.test(href)) { const e = stripQ(href.replace(/^mailto:/i, '')).trim(); if (e) out.emails.push(e); continue; }
    if (/^tel:/i.test(href)) { out.phones.push(href.replace(/^tel:/i, '').trim()); continue; }
    const wam = low.match(/wa\.me\/(\+?\d[\d]*)/); if (wam) { out.phones.push(wam[1]); continue; }
    if (!out.facebook && (/(^|\/\/|\.)facebook\.com\//i.test(low) || /(^|\/\/)fb\.com\//i.test(low)) && hasPath(href)) out.facebook = href;
    else if (!out.instagram && /(^|\/\/|\.)instagram\.com\//i.test(low) && hasPath(href)) out.instagram = href;
    else if (!out.map && (/google\.[^/]+\/maps/i.test(low) || /goo\.gl\/maps/i.test(low) || /maps\.app\.goo\.gl/i.test(low) || /bing\.com\/maps/i.test(low))) out.map = href;
    else if (/linkedin\.com\/in\//i.test(low)) { const u = stripQ(href); if (!seenLi.has(u)) { seenLi.add(u); out.linkedin.push(u); } }
    else if (isAlternate(href, altList) && !seenAlt.has(low)) { seenAlt.add(low); out.alternateWebsites.push(href); }
    else if (/^https?:\/\//i.test(href) && ex.getBaseDomain(href) === selfHost && cc.isBioOrContactUrl(href)) { const u = stripQ(href); if (!seenBio.has(u)) { seenBio.add(u); out.bio.push(u); } }
  }
  // description-embedded phone/email
  for (const e of (out.description.match(EMAIL_RE) || [])) out.emails.push(e.toLowerCase());
  for (const p of (out.description.match(PHONE_RE) || [])) out.phones.push(p.trim());
  out.emails = [...new Set(out.emails.map((e) => e.toLowerCase()))];
  out.phones = [...new Set(out.phones)];
  return out;
}

// Derive a {first,last} from a linkedin/in URL, email, or bio URL (returns nulls when not confidently named).
function nameFromLinkedin(u) { const m = String(u).match(/linkedin\.com\/in\/([^/?#]+)/i); return m ? ex.nameFromSlug(m[1].replace(/-\d+$/, '')) : { first: '', last: '' }; }
function nameFromBio(u) { return ex.nameFromSlug(ex.nameSlugFromUrl(u) || ''); }
function nameFromEmail(e) {
  const local = String(e).split('@')[0].toLowerCase().replace(/\d+$/, '');
  const parts = local.split(/[._\-]+/).filter((p) => p.length >= 2 && /^[a-z]+$/.test(p));
  return parts.length >= 2 ? { first: cap(parts[0]), last: cap(parts[1]) } : { first: '', last: '' };
}
const nkey = (n) => (n.first || '').toLowerCase() + '|' + (n.last || '').toLowerCase();

// Group LinkedIn/in + emails + bio URLs by the person's name; emit a contact when >=2 field-types agree
// on a named person AND that first name resolves to a gender. Format:
//   "First, Last, Gender, Email, Address, LinkedIn, BIO;"
function buildContacts({ linkedin = [], emails = [], bio = [] }, { genderMap = {}, address = '' } = {}) {
  const groups = new Map();   // nkey -> { first,last, types:Set, email,linkedin,bio }
  const add = (n, type, val) => {
    if (!n.first || !n.last) return;
    const k = nkey(n); let g = groups.get(k);
    if (!g) { g = { first: n.first, last: n.last, types: new Set(), email: '', linkedin: '', bio: '' }; groups.set(k, g); }
    g.types.add(type); if (!g[type]) g[type] = val;
  };
  for (const u of linkedin) add(nameFromLinkedin(u), 'linkedin', u);
  for (const e of emails) add(nameFromEmail(e), 'email', e);
  for (const u of bio) add(nameFromBio(u), 'bio', u);
  const contacts = [], structured = [];
  for (const g of groups.values()) {
    if (g.types.size < 2) continue;                                  // need >= 2 agreeing fields
    const gender = genderMap[(g.first || '').toLowerCase()] || '';   // must be gender-assignable
    if (!gender) continue;
    contacts.push([g.first, g.last, gender, g.email || '', address || '', g.linkedin || '', g.bio || ''].join(', ') + ';');
    structured.push({ first: g.first, last: g.last, gender, email: g.email || '', address: address || '', linkedin: g.linkedin || '', bio: g.bio || '' });
  }
  return { contacts, structured, count: contacts.length };
}

// buildEmployees — high-liberty association for a Google-Maps rollup. The handful of emails / LinkedIn-/in/
// / bio URLs pulled from ONE business SHARE a source, so we link them aggressively into "Employees":
//   1) name people from LinkedIn/in + bio URLs (first + last)
//   2) attach each email to a person via progressively looser rules:
//        a. dotted email first+last (jane.doe@)                          -> exact name match
//        b. first-initial+lastname (jdoe@ / j.doe@ == j + doe)           -> match on last, first-initial
//        c. lastname CONTAINED + first-initial (jdoe12@, mjdoe@)         -> contains match
//        d. first-name-only email (jane@)                               -> match a person's first name alone
//   3) also allow a dotted email to stand up its own employee. Email-keyed store -> an employee needs an
//      email; a person named only by an email must have a gender-known first name (drops sales@/info@ junk).
function buildEmployees({ linkedin = [], emails = [], bio = [] }, { genderMap = {}, address = '' } = {}) {
  const norm = (s) => String(s || '').toLowerCase().replace(/[^a-z]/g, '');
  const people = [];   // { first, last, linkedin, bio, email }
  const findOrAdd = (n) => {
    if (!n.first || !n.last) return null;
    let p = people.find((x) => norm(x.first) === norm(n.first) && norm(x.last) === norm(n.last));
    if (!p) { p = { first: n.first, last: n.last, linkedin: '', bio: '', email: '' }; people.push(p); }
    return p;
  };
  for (const u of linkedin) { const p = findOrAdd(nameFromLinkedin(u)); if (p && !p.linkedin) p.linkedin = u; }
  for (const u of bio) { const p = findOrAdd(nameFromBio(u)); if (p && !p.bio) p.bio = u; }

  for (const e of emails) {
    const localRaw = String(e).split('@')[0].toLowerCase().replace(/\d+$/, '');
    const local = norm(localRaw);
    const full = nameFromEmail(e);                                       // {first,last} when dotted
    let p = null;
    if (full.first && full.last) p = people.find((x) => norm(x.first) === norm(full.first) && norm(x.last) === norm(full.last));   // (a)
    if (!p) p = people.find((x) => { const ln = norm(x.last), fi = norm(x.first).charAt(0); return ln.length >= 2 && local === fi + ln; });   // (b)
    if (!p) p = people.find((x) => { const ln = norm(x.last), fi = norm(x.first).charAt(0); return ln.length >= 3 && local.includes(ln) && local.charAt(0) === fi; });   // (c)
    if (!p) { const firstTok = norm(localRaw.split(/[._\-]+/)[0]); if (firstTok.length >= 3) p = people.find((x) => norm(x.first) === firstTok); }   // (d)
    if (p) { if (!p.email) p.email = e; }
    else if (full.first && full.last) people.push({ first: full.first, last: full.last, linkedin: '', bio: '', email: e });        // email names its own person
  }

  const structured = [];
  for (const p of people) {
    if (!p.email) continue;                                              // email-keyed store needs an email
    const gender = genderMap[(p.first || '').toLowerCase()] || '';
    if (!p.linkedin && !p.bio && !gender) continue;                     // email-only person must have a known first name
    structured.push({ first: p.first, last: p.last, gender, email: p.email, address, linkedin: p.linkedin, bio: p.bio });
  }
  return { structured, count: structured.length };
}

// If the company's primary `website` is really a social/map/alternate URL, move it out (and blank website).
function reclassifyWebsite(website, altList) {
  const w = clean(website); if (!w) return {};
  const low = w.toLowerCase();
  if (/(facebook\.com|fb\.com)\//i.test(low) && hasPath(w)) return { facebook: w, website: '' };
  if (/instagram\.com\//i.test(low) && hasPath(w)) return { instagram: w, website: '' };
  if (/(google\.[^/]+\/maps|bing\.com\/maps|goo\.gl\/maps|maps\.app\.goo\.gl)/i.test(low)) return { map: w, website: '' };
  if (isAlternate(w, altList)) return { altMove: w, website: '' };
  return {};
}

// Assemble the field updates from an already-fetched home-page HTML (the reusable core — drivable from an
// Athena-resolved pointer, not just per-company CDX). Returns { updates, people }.
function enrichFromHtml(company, html, { genderMap = {}, altList, now, crawl } = {}) {
  const domain = company.domain || '';
  const p = parseHome(html, domain, altList);
  const address = company.full_address || '';
  const { contacts, structured, count } = buildContacts({ linkedin: p.linkedin, emails: p.emails, bio: p.bio }, { genderMap, address });
  const rc = reclassifyWebsite(company.website, altList);
  const alt = [...new Set([...(p.alternateWebsites || []), ...(rc.altMove ? [rc.altMove] : [])])];
  const phone = (p.phones[0] || '').trim();
  const email = (p.emails[0] || '').toLowerCase();
  const up = {
    description: p.description,
    facebook: p.facebook || rc.facebook || '',
    instagram: p.instagram || rc.instagram || '',
    map: p.map || rc.map || '',
    linkedin_contact: p.linkedin.join('; '),
    bio_url: p.bio.join('; '),
    alternate_websites: alt.join('; '),
    contacts: contacts.join(' '),
    contacts_count: count,
  };
  if (now) up.cc_refreshed_at = now;          // when this company was last CC-refreshed (drives deltas + "last refreshed")
  if (crawl) up.cc_crawl = crawl;             // the CC crawl the freshest capture came from
  if (phone && !String(company.phone || '').trim()) up.phone = phone;
  if (email) { up.email = email; up.email_type = ex.classifyEmail(email); }
  if ('website' in rc) up.website = '';
  const people = p.linkedin.map((u) => { const n = nameFromLinkedin(u); return { linkedin: u, first: n.first, last: n.last }; }).filter((x) => x.first && x.last);
  return { updates: up, people, contacts: structured };
}

// Full per-company enrichment: resolve its home page in CC (CDX), fetch, then enrichFromHtml.
async function enrichCompany(company, { genderMap = {}, crawls, fetchWarc, altList, now } = {}) {
  const ptr = await resolveHome(company.domain || '', { crawls });
  if (!ptr) return { found: false, reason: 'not in CC' };
  let html = ''; try { html = await fetchWarc(ptr); } catch (e) { return { found: false, reason: 's3: ' + e.message }; }
  if (!html) return { found: false, reason: 'empty' };
  const r = enrichFromHtml(company, html, { genderMap, altList, now: now || new Date().toISOString(), crawl: (crawls && crawls[0]) || '' });
  return { found: true, updates: r.updates, people: r.people, ptr: { url: ptr.url } };
}

module.exports = { ALT_DEFAULT, resolveHome, parseHome, buildContacts, buildEmployees, reclassifyWebsite, enrichFromHtml, enrichCompany, hasPath, isAlternate, nameFromLinkedin, nameFromEmail, nameFromBio };
