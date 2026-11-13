// Public Prospects as an evidence source for email modelling.
//
// Two jobs, both read-only, both optional:
//
//   patternSamples(domains)  observed (person, address) pairs on a domain, so a pattern can be
//                            LEARNED instead of guessed.
//   observed(emails)         which of our modelled guesses have actually been seen in public, so a
//                            guess can be promoted to an observed address.
//
// Why this exists: on 2026-11-12, 62.4% of the addresses this pipeline modelled (84,495 of 135,326)
// came from the blind {first}.{last} default, because neither the processing DB nor OpenSearch held
// a single observed address on that domain. Public Prospects holds 185,677,340 addresses across 18
// sources, and of the 12,740 domains that modelled blind it has personal mailboxes for 7,895 of them
// -- covering 69.4% of those rows. The pattern was knowable; nothing was asking anything that knew.
//
// Measured the hard way, so future callers do not repeat it:
//   * email_rollup.email_domain is indexed, and sightings.email_norm is indexed. Use those.
//   * sightings.email_registrable is NOT indexed -- querying it scans 313M rows and times out.
//   * a 12,740-element ANY() also times out; keep batches small or stage into a temp table.
//
// Everything here fails soft. PP being unreachable, slow or unconfigured must never stop a crawl
// from merging -- it only means we fall back to guessing, which is what happened before it existed.
const { Pool } = require('pg');

const DSN = process.env.PP_DATABASE_URL || '';
const TIMEOUT_MS = Math.max(1000, Number(process.env.PP_TIMEOUT_MS) || 15000);
const MAX_SAMPLES = Math.max(10, Number(process.env.PP_MAX_SAMPLES) || 400);
const CACHE_MAX = Math.max(100, Number(process.env.PP_CACHE_MAX) || 20000);
// Thresholds for the page_registrable path only (see patternSamples path B).
const PAGE_MIN_SAMPLES = Math.max(1, Number(process.env.PP_PAGE_MIN_SAMPLES) || 5);
const PAGE_MIN_SHARE = Number(process.env.PP_PAGE_MIN_SHARE) || 0.6;

let pool = null;
let poolBroken = false;
let warned = false;

function enabled() {
  return !!DSN;
}

function getPool() {
  if (poolBroken || !DSN) return null;
  if (pool) return pool;
  try {
    pool = new Pool({
      connectionString: DSN,
      max: Math.max(1, Number(process.env.PP_POOL_MAX) || 3),
      connectionTimeoutMillis: TIMEOUT_MS,
      idleTimeoutMillis: 30000,
      // Belt and braces: a slow PP query must not hold a crawl open. Set on the server side so it
      // applies even to a query that never returns a row.
      statement_timeout: TIMEOUT_MS,
      query_timeout: TIMEOUT_MS,
    });
    pool.on('error', (e) => { console.error('[pp] pool error:', e.message); });
  } catch (e) {
    poolBroken = true;
    console.error('[pp] could not create pool:', e.message);
    return null;
  }
  return pool;
}

function note(e, what) {
  // One warning per process, then silence: a PP outage during a 500-job backlog would otherwise
  // write half a million identical lines and bury everything that matters.
  if (!warned) {
    warned = true;
    console.error(`[pp] ${what} failed (further PP errors suppressed this process):`, e.message);
  }
}

// Mirror of pp_normalize_email for the corporate case: lower-case, trim, drop a +suffix. Gmail
// dot-stripping is deliberately NOT replicated -- it applies only to gmail/googlemail, and a
// modelled work address is never on either.
function normEmail(raw) {
  const e = String(raw || '').trim().toLowerCase();
  const at = e.indexOf('@');
  if (at < 1) return '';
  const local = e.slice(0, at).split('+')[0];
  const domain = e.slice(at + 1).replace(/\.+$/, '');
  if (!local || !domain.includes('.')) return '';
  return `${local}@${domain}`;
}

// "Anub A. Abraham, DO" -> Anub / Abraham. Returns null when there is no usable pair.
//
// Deliberately permissive: PP's person_name carries extractor noise (company names in the name
// field, surname-first ordering). Filtering that here would mean guessing which is which, and
// inferEmailPattern already refuses to vote unless the local part actually fits the name -- so a
// malformed row costs nothing and excludes itself.
const SUFFIX_RE = /\s*,\s*(m\.?d\.?|d\.?o\.?|ph\.?d\.?|r\.?n\.?|esq\.?|j\.?d\.?|mba|cpa|pa-?c|np|dds|dmd|facs|faap)\b.*$/i;
const GEN_RE = /\s+(jr|sr|ii|iii|iv)\.?$/i;
function splitName(raw) {
  let t = String(raw || '').trim().replace(SUFFIX_RE, '').replace(GEN_RE, '');
  t = t.replace(/\s+/g, ' ').trim();
  if (!t) return null;
  const parts = t.split(' ').filter((p) => p && !/^[^a-z]+$/i.test(p));
  if (parts.length < 2) return null;
  return { first: parts[0], last: parts[parts.length - 1] };
}

const sampleCache = new Map();   // domain -> rows
function cacheSet(k, v) {
  if (sampleCache.size >= CACHE_MAX) sampleCache.clear();   // crude, but bounded and never stale-forever
  sampleCache.set(k, v);
}

// Observed (person, address) pairs on these domains, as canonical records that addSample accepts.
// `domains` is tried in order -- typically [exact, registrable], so biox.stanford.edu can fall back
// to stanford.edu.
async function patternSamples(domains) {
  const list = (Array.isArray(domains) ? domains : [domains])
    .map((d) => String(d || '').trim().toLowerCase()).filter(Boolean);
  if (!list.length) return [];
  const key = list.join('|');
  if (sampleCache.has(key)) return sampleCache.get(key);

  const p = getPool();
  if (!p) return [];
  let out = [];

  const toRecords = (pairs) => {
    const rows = [];
    for (const r of pairs) {
      const nm = splitName(r.person_name);
      if (!nm) continue;
      rows.push({
        First: nm.first,
        Last: nm.last,
        'Email Address': r.email_norm,
        // Marked Professional so addSample's provenance check accepts it: these are OBSERVED
        // addresses, which is exactly what that check is there to require.
        'Email Type': 'Professional',
      });
    }
    return rows;
  };

  try {
    // Path A -- by EMAIL domain. Finds staff whose address is at the domain we are modelling.
    for (const domain of list) {
      const r1 = await p.query(
        `SELECT email_norm FROM email_rollup
          WHERE email_domain = $1 AND mailbox_type = 'personal'
          LIMIT $2`, [domain, MAX_SAMPLES]);
      const addrs = r1.rows.map((r) => r.email_norm).filter(Boolean);
      if (!addrs.length) continue;
      const r2 = await p.query(
        `SELECT DISTINCT ON (email_norm) email_norm, person_name
           FROM sightings
          WHERE email_norm = ANY($1::text[])
            AND person_name IS NOT NULL AND person_name <> ''`, [addrs]);
      const rows = toRecords(r2.rows);
      if (rows.length) { out = rows; break; }      // first domain that yields evidence wins
    }

    // Path B -- by the WEBSITE the person was seen on, when path A found nothing.
    //
    // This is the case blind guessing damages most. cumedicine.us publishes staff bios, but those
    // people's mail is at ucdenver.edu -- so email_rollup has nothing under cumedicine.us, path A
    // comes back empty, and the default writes 1,146 addresses at a domain that does not carry
    // their mail. page_registrable records the site the address was FOUND on rather than the one
    // it belongs to, which is precisely the link we need, and idx_sightings_page_reg makes it cheap.
    if (!out.length) {
      for (const domain of list) {
        const r3 = await p.query(
          `SELECT DISTINCT ON (email_norm) email_norm, person_name
             FROM sightings
            WHERE page_registrable = $1
              AND mailbox_type = 'personal'
              AND person_name IS NOT NULL AND person_name <> ''
            LIMIT $2`, [domain, MAX_SAMPLES]);
        const rows = toRecords(r3.rows);
        // Path B is far noisier than path A and has to earn its answer.
        //
        // Path A asks "who has mail AT this company", which is self-selecting. Path B asks "whose
        // address appeared ON this website", and on a directory or marketplace that is hundreds of
        // unrelated people: effectiveagents.com returned 34 agents spread across their own
        // brokerages, with only 10% sharing any one mail domain. Requiring a real sample AND a
        // dominant mail domain keeps the genuine case -- a practice whose staff all use the parent
        // health system's mail -- and drops the aggregators, where no single pattern is true.
        const domCount = new Map();
        for (const r of rows) {
          const d = String(r['Email Address']).split('@')[1] || '';
          if (d) domCount.set(d, (domCount.get(d) || 0) + 1);
        }
        let top = 0;
        for (const n of domCount.values()) if (n > top) top = n;
        const share = rows.length ? top / rows.length : 0;
        if (rows.length >= PAGE_MIN_SAMPLES && share >= PAGE_MIN_SHARE) { out = rows; break; }
      }
    }
  } catch (e) {
    note(e, 'patternSamples');
    return [];
  }
  cacheSet(key, out);
  return out;
}

// Which of these addresses have actually been seen in public, with the evidence behind each.
// Returns Map(normalisedEmail -> { sightings, sources, firstSeen, lastSeen, mailboxType }).
async function observed(emails) {
  const p = getPool();
  if (!p) return new Map();
  const want = [...new Set((emails || []).map(normEmail).filter(Boolean))];
  const found = new Map();
  if (!want.length) return found;
  const CHUNK = 1000;                      // a large ANY() makes the planner give up and scan
  try {
    for (let i = 0; i < want.length; i += CHUNK) {
      const batch = want.slice(i, i + CHUNK);
      const r = await p.query(
        `SELECT email_norm, n_sightings, n_sources, mailbox_type,
                to_char(first_seen,'YYYY-MM-DD') AS first_seen,
                to_char(last_seen,'YYYY-MM-DD')  AS last_seen
           FROM email_rollup
          WHERE email_norm = ANY($1::text[])`, [batch]);
      for (const row of r.rows) {
        found.set(row.email_norm, {
          sightings: Number(row.n_sightings) || 0,
          sources: Number(row.n_sources) || 0,
          mailboxType: row.mailbox_type || '',
          firstSeen: row.first_seen || '',
          lastSeen: row.last_seen || '',
        });
      }
    }
  } catch (e) {
    note(e, 'observed');
    return new Map();
  }
  return found;
}

async function close() {
  if (pool) { try { await pool.end(); } catch (e) { /* shutting down */ } pool = null; }
}

module.exports = { enabled, patternSamples, observed, normEmail, splitName, close };
