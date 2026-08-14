/**
 * domain-gate.js — the list of domains we do NOT spend fetches on. Module + CLI.
 *
 *   node domain-gate.js --list
 *   node domain-gate.js --add seniorfinanceadvisor.com,profiles.superlawyers.com --reason "0 contacts / 418k attempts"
 *   node domain-gate.js --remove kellysearch.com
 *
 * Measured 2026-08-14: 125 domains below 2% yield accounted for 1,988,734 of 3,257,054 attempts — 61.1%
 * of ALL live fetching — and returned on the order of 2,000 contacts between them. seniorfinanceadvisor.com
 * alone cost 221,134 fetches for one contact; profiles.superlawyers.com cost 196,950 for none. Removing
 * that tail is a ~2.6x reduction in fleet work for ~1% of the contacts.
 *
 * The list lives in cc_config beside the other admin-editable lists, so it can be changed without a deploy
 * and every stage reads the same source of truth. It is applied as early as possible — in bio-etl's URL
 * collection, before the Athena resolve — so a blocked domain costs nothing anywhere downstream: no
 * resolve, no Lambda, no miss-list entry, no fleet time.
 *
 * Blocking stops FETCHING. It does not delete contacts already collected from a domain: ptindirectory.com
 * is a poor investment at 1,402 contacts per 238,229 fetches, but those 1,402 people are real.
 *
 * Entries record why and when, because a bare list of domains becomes unauditable within a month and
 * nobody can tell a deliberate exclusion from a mistake.
 */
const CONFIG_INDEX = process.env.CC_CONFIG_INDEX || 'cc_config';
const DOC_ID = 'domain_blocklist';

const normDomain = (d) => String(d || '').toLowerCase().trim()
  .replace(/^https?:\/\//, '').replace(/^www\./, '').split('/')[0].split('?')[0];

async function load(client) {
  try {
    const g = await client.get({ index: CONFIG_INDEX, id: DOC_ID });
    const b = (g.body || g)._source || {};
    return Array.isArray(b.domains) ? b.domains : [];
  } catch (e) { return []; }
}

/** A Set of blocked domains, for O(1) checks in a hot loop. */
async function loadSet(client) {
  const set = new Set();
  for (const e of await load(client)) { const d = normDomain(e && e.domain); if (d) set.add(d); }
  return set;
}

async function save(client, entries) {
  await client.index({ index: CONFIG_INDEX, id: DOC_ID, body: { domains: entries, updated_at: new Date().toISOString() }, refresh: true });
}

async function add(client, domains, reason, stats) {
  const cur = await load(client);
  const have = new Map(cur.map((e) => [normDomain(e.domain), e]));
  const at = new Date().toISOString();
  let added = 0;
  for (const raw of domains) {
    const d = normDomain(raw);
    if (!d || have.has(d)) continue;
    const e = { domain: d, reason: reason || 'low yield', blocked_at: at };
    if (stats && stats[d]) { e.attempts = stats[d].attempts; e.contacts = stats[d].contacts; }
    have.set(d, e); added++;
  }
  await save(client, [...have.values()]);
  return { added, total: have.size };
}

async function remove(client, domains) {
  const cur = await load(client);
  const drop = new Set(domains.map(normDomain));
  const kept = cur.filter((e) => !drop.has(normDomain(e.domain)));
  await save(client, kept);
  return { removed: cur.length - kept.length, total: kept.length };
}

module.exports = { CONFIG_INDEX, DOC_ID, load, loadSet, save, add, remove, normDomain };

if (require.main === module) {
  const os = require('./opensearch');
  const arg = (n, d) => { const i = process.argv.indexOf('--' + n); return i > 0 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : d; };
  const N = (n) => Number(n || 0).toLocaleString();
  (async () => {
    const client = os.makeClient(process.env.OPENSEARCH_ENDPOINT);
    const addList = arg('add', ''), rmList = arg('remove', '');
    if (addList) {
      const r = await add(client, addList.split(',').map((s) => s.trim()).filter(Boolean), arg('reason', ''));
      console.log(`added ${r.added}; blocklist now ${r.total} domain(s)`);
      return;
    }
    if (rmList) {
      const r = await remove(client, rmList.split(',').map((s) => s.trim()).filter(Boolean));
      console.log(`removed ${r.removed}; blocklist now ${r.total} domain(s)`);
      return;
    }
    const cur = await load(client);
    console.log(`blocklist: ${cur.length} domain(s)`);
    let att = 0, got = 0;
    for (const e of cur.slice().sort((a, b) => (b.attempts || 0) - (a.attempts || 0))) {
      att += Number(e.attempts) || 0; got += Number(e.contacts) || 0;
      console.log(`  ${String(e.domain).padEnd(38)} ${N(e.attempts).padStart(10)} attempts -> ${N(e.contacts).padStart(7)} contacts   ${e.reason || ''}`);
    }
    if (att) console.log(`\n  fetches avoided per full pass: ${N(att)}  (they had returned ${N(got)} contacts)`);
  })().catch((e) => { console.error('ERR', e && e.message || e); process.exit(1); });
}
