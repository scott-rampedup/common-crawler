/**
 * atp-load.js — Phase 1 loader for AllThePlaces (alltheplaces.xyz) location data. Reads a CSV of
 * (Name, Source URL) where each Source URL is a brand's GeoJSON FeatureCollection; fetches each,
 * maps every POI Feature to the SAME normalized "Location" record shape as gm-load/bing-load
 * (componentized address -> gm-build skips the address parser), so gm-build/gm-upsert consume it
 * identically. Emits atp-locations.ndjson. company_type='Location', source_map='AllThePlaces'.
 *
 *   node atp-load.js [--csv "All The Places - Location Data.csv"] [--out <dir>] [--limit-brands N] [--limit N]
 *
 * AllThePlaces feature.properties: ref, name, brand, nsi_id, @spider, shop, phone, website, email,
 *   addr:housenumber, addr:street, addr:city, addr:state, addr:postcode, addr:country (ISO-2).
 */
const fs = require('fs');
const https = require('https');
const path = require('path');
const ex = require('./extractor');
const co = require('./companies');
const { rootDomain } = require('./email-model');

const arg = (f, d) => { const i = process.argv.indexOf(f); return i > -1 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
const CSV = arg('--csv', path.join(__dirname, 'All The Places - Location Data.csv'));
const OUT = arg('--out', __dirname);
const LIMIT_BRANDS = Number(arg('--limit-brands', '0')) || 0;
const LIMIT = Number(arg('--limit', '0')) || 0;

const COUNTRY = { US: 'United States', GB: 'United Kingdom', UK: 'United Kingdom', CA: 'Canada', AU: 'Australia', NZ: 'New Zealand',
  IE: 'Ireland', DE: 'Germany', FR: 'France', ES: 'Spain', IT: 'Italy', PT: 'Portugal', NL: 'Netherlands', BE: 'Belgium',
  CH: 'Switzerland', AT: 'Austria', SE: 'Sweden', NO: 'Norway', DK: 'Denmark', FI: 'Finland', PL: 'Poland', CZ: 'Czechia',
  MX: 'Mexico', BR: 'Brazil', AR: 'Argentina', CL: 'Chile', CO: 'Colombia', JP: 'Japan', CN: 'China', IN: 'India',
  SG: 'Singapore', HK: 'Hong Kong', KR: 'South Korea', AE: 'United Arab Emirates', SA: 'Saudi Arabia', ZA: 'South Africa',
  AL: 'Albania', GR: 'Greece', TR: 'Turkey', RU: 'Russia', UA: 'Ukraine', RO: 'Romania', HU: 'Hungary' };

const genderMap = ex.loadGenderMap(path.join(__dirname, 'names-genders.csv'));
try { ex.loadEmailBlocklist(path.join(__dirname, 'email-blocklist.txt')); } catch (e) { /* no list */ }
let dirRules = {}; try { dirRules = ex.loadDirectoryRules(path.join(__dirname, 'data', 'directory-rules.json')); } catch (e) { /* built-ins */ }
const clean = (s) => String(s == null ? '' : s).trim();
const abs = (u) => { u = clean(u); return /^https?:/i.test(u) ? u : (u ? 'https://' + u : ''); };
const PEOPLE_SUB = new Set(['agent', 'agents', 'advisor', 'advisors', 'realtor', 'realtors', 'provider', 'providers', 'doctor', 'doctors']);
const LOC_SUB = new Set(['location', 'locations', 'store', 'stores', 'bank', 'banks', 'dealer', 'dealers']);
function websiteType(u) {
  u = abs(u); if (!u) return '';
  try { const labels = new URL(u).hostname.toLowerCase().replace(/^www\./, '').split('.'); if (labels.length > 2) { if (PEOPLE_SUB.has(labels[0])) return 'People'; if (LOC_SUB.has(labels[0])) return 'Location'; } } catch (e) {}
  try { const pid = ex.pathIdFromUrl(u); if (pid && pid.id) return pid.id; } catch (e) {}
  try { const d = ex.classifyDirectory(u, '', dirRules, genderMap); if (d && d !== 'Contact Us') return d; } catch (e) {}
  return '';
}
function fullAddress(p) {
  const street = [clean(p['addr:housenumber']), clean(p['addr:street'])].filter(Boolean).join(' ');
  const cityline = [clean(p['addr:city']), [clean(p['addr:state']), clean(p['addr:postcode'])].filter(Boolean).join(' ')].filter(Boolean).join(', ');
  const country = COUNTRY[clean(p['addr:country']).toUpperCase()] || clean(p['addr:country']);
  return [street, cityline, country].filter(Boolean).join(', ');
}
function fetchJson(url, redirs = 3) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, { timeout: 120000, headers: { 'User-Agent': 'rampedup-atp-load' } }, (res) => {
      if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location && redirs > 0) { res.resume(); return resolve(fetchJson(new URL(res.headers.location, url).href, redirs - 1)); }
      if (res.statusCode !== 200) { res.resume(); return reject(new Error('HTTP ' + res.statusCode)); }
      let buf = ''; res.setEncoding('utf8'); res.on('data', (d) => { buf += d; }); res.on('end', () => { try { resolve(JSON.parse(buf)); } catch (e) { reject(new Error('bad JSON (' + Math.round(buf.length / 1024) + 'KB)')); } });
    });
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
    req.on('error', reject);
  });
}

(async () => {
  const lines = fs.readFileSync(CSV, 'utf8').split(/\r?\n/).slice(1).filter((l) => l.trim());
  const brands = lines.map((l) => { const m = l.match(/^\s*"?(.*?)"?\s*,\s*(https?:\/\/\S+)\s*$/); return m ? { name: m[1].trim(), url: m[2].trim() } : null; }).filter(Boolean);
  console.error(`AllThePlaces brands: ${brands.length} | out: ${OUT}`);
  const out = fs.createWriteStream(path.join(OUT, 'atp-locations.ndjson'));
  const now = new Date().toISOString();
  let bn = 0, seen = 0, kept = 0, skippedNoWeb = 0, fetchErr = 0; const t0 = Date.now();
  for (const { name, url } of brands) {
    if (LIMIT_BRANDS && bn >= LIMIT_BRANDS) break;
    bn++;
    let gj; try { gj = await fetchJson(url); } catch (e) { fetchErr++; console.error(`  [${bn}] ${name}: FETCH FAIL ${e.message}`); continue; }
    const feats = (gj && Array.isArray(gj.features)) ? gj.features : [];
    let bkept = 0;
    for (const f of feats) {
      seen++;
      if (LIMIT && kept >= LIMIT) break;
      const p = (f && f.properties) || {};
      const dom = co.normDomain(clean(p.website)) || rootDomain(clean(p.website));
      if (co.isBadCompanyDomain(dom)) { skippedNoWeb++; continue; }   // domain must be the real web source (never free-mail/social/shared)
      const email = ex.cleanEmail(clean(p.email)).toLowerCase();
      const coords = (f.geometry && Array.isArray(f.geometry.coordinates)) ? f.geometry.coordinates.join(',') : '';
      const rec = {
        company_type: 'Location', root_domain: dom,
        cid: 'atp:' + (clean(p.nsi_id) || clean(p['@spider']) || name) + ':' + (clean(p.ref) || coords),
        name: clean(p.name) || name,
        full_address: fullAddress(p),
        locality: clean(p['addr:city']), region: clean(p['addr:state']),
        country: COUNTRY[clean(p['addr:country']).toUpperCase()] || clean(p['addr:country']),
        website: 'https://' + dom + '/', website_type: websiteType(p.website),
        category: clean(p.shop) || clean(p.brand) || name,
        email, email_type: email ? ex.classifyEmail(email) : '',
        phone: clean(p.phone), phone_type: '',
        facebook: '', instagram: '', whatsapp: '', linkedin_contact: '', bio_url: '', team_page: '',
        time_stamp: now, source_map: 'AllThePlaces',
      };
      out.write(JSON.stringify(rec) + '\n'); kept++; bkept++;
    }
    console.error(`  [${bn}/${brands.length}] ${name}: ${feats.length} features -> ${bkept} kept (total ${kept.toLocaleString()})`);
    if (LIMIT && kept >= LIMIT) break;
  }
  out.end();
  await new Promise((r) => out.on('finish', r));
  console.error(`DONE: ${bn} brands, ${seen.toLocaleString()} features -> ${kept.toLocaleString()} Location records | ${skippedNoWeb.toLocaleString()} no-website | ${fetchErr} fetch-err | ${Math.round((Date.now() - t0) / 1000)}s`);
})().catch((e) => { console.error('ERR', e && e.stack || e); process.exit(1); });
