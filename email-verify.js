/**
 * email-verify.js — deliverability check for MODELLED emails, via RampedUp's Exhaust Email Validation API.
 *
 * POST {email} -> { lastValidationStatus, domainDeliverabilityStatus, domainIsCatchAll, domainHasNoMxRecord, … }.
 * GOOD  = lastValidationStatus in {VALID, DETERMINISTIC, CATCH_ALL}  OR the domain is catch-all (accepts all).
 * BAD   = INVALID / UNKNOWN / null on a verifiable domain (the modelled mailbox isn't confirmed).
 * NO_MX = the domain can't receive mail at all → every address there is bad.
 *
 * The endpoint is an internal ELB (no matching public cert), so TLS verification is disabled for it only.
 * Results are cached per email. On any API/network error we return { ok:false } so callers can fall back
 * gracefully (use the best-guess unverified email) rather than dropping the record.
 */
const https = require('https');

const BASE = process.env.EMAIL_VERIFY_URL || 'https://exhaustapi-alb-803136601.us-east-1.elb.amazonaws.com';
const VPATH = process.env.EMAIL_VERIFY_PATH || '/api/v1/exhaustvalidation';
const TIMEOUT = Number(process.env.EMAIL_VERIFY_TIMEOUT_MS) || 30000;
const agent = new https.Agent({ rejectUnauthorized: false, keepAlive: true, maxSockets: 8 });

const GOOD_STATUS = new Set(['valid', 'deterministic', 'catch_all', 'catchall']);
const BAD_STATUS = new Set(['invalid', 'unknown']);
const _cache = new Map();   // lowercased email -> classification

function classify(resp) {
  if (!resp || typeof resp !== 'object') return { ok: false, good: false, status: 'error', catchAll: false, verifiable: false, noMx: false };
  const lvs = String(resp.lastValidationStatus || '').toLowerCase().replace(/[\s-]+/g, '_');   // deterministic|valid|invalid|unknown|role_based|''
  const dds = String(resp.domainDeliverabilityStatus || '').toUpperCase();
  const catchAll = resp.domainIsCatchAll === true || dds === 'CATCH_ALL' || lvs === 'catch_all' || lvs === 'catchall';
  const noMx = resp.domainHasNoMxRecord === true || dds === 'NO_MX' || dds === 'INVALID';
  let good;
  if (GOOD_STATUS.has(lvs)) good = true;            // mailbox confirmed VALID/DETERMINISTIC (or literal catch_all)
  else if (BAD_STATUS.has(lvs)) good = false;       // confirmed INVALID/UNKNOWN
  else good = catchAll;                             // no stored mailbox status → good only if the domain accepts all
  if (noMx) good = false;                           // domain can't receive mail → always bad
  return { ok: true, good, status: lvs || dds.toLowerCase() || 'unknown', catchAll, verifiable: dds === 'VERIFIABLE', noMx };
}

function post(email) {
  return new Promise((resolve) => {
    const body = JSON.stringify({ email });
    let u; try { u = new URL(BASE + VPATH); } catch (e) { return resolve(null); }
    const req = https.request({
      hostname: u.hostname, port: u.port || 443, path: u.pathname + u.search, method: 'POST', agent, timeout: TIMEOUT,
      headers: { 'Content-Type': 'application/json', Accept: 'application/json', 'Content-Length': Buffer.byteLength(body) },
    }, (res) => { let s = ''; res.on('data', (d) => (s += d)); res.on('end', () => { try { resolve(JSON.parse(s)); } catch (e) { resolve(null); } }); });
    req.on('error', () => resolve(null));
    req.on('timeout', () => { req.destroy(); resolve(null); });
    req.write(body); req.end();
  });
}

async function verifyEmail(email) {
  const key = String(email || '').toLowerCase().trim();
  if (!key || key.indexOf('@') < 1) return { ok: false, good: false, status: 'invalid', catchAll: false, verifiable: false, noMx: false };
  if (_cache.has(key)) return _cache.get(key);
  const r = classify(await post(key));
  if (r.ok) _cache.set(key, r);        // don't cache transient errors
  return r;
}

module.exports = { verifyEmail, classify };
