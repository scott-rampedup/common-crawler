// optout.js — the "right to be removed" registry, kept in a SEPARATE OpenSearch index (`optout`) from the
// contacts data. Flow: a person submits their email on the public /opt-out portal -> we store a PENDING
// request + a random token and email them a one-click confirm link. On confirm we mark it CONFIRMED, delete
// their records from the contacts index, and LEAVE them in this registry so opensearch.bulkUpsert suppresses
// them forever — if the same person is processed/crawled again, they're removed again automatically.
const crypto = require('crypto');
const os = require('./opensearch');
const INDEX = os.OPTOUT_INDEX;   // 'optout' (or $OPTOUT_INDEX)

const MAPPING = { settings: { number_of_shards: 1, number_of_replicas: 0 }, mappings: { properties: {
  email: { type: 'keyword' }, name: { type: 'keyword' }, reason: { type: 'text' }, token: { type: 'keyword' },
  status: { type: 'keyword' }, requested_at: { type: 'date' }, confirmed_at: { type: 'date' }, source_ip: { type: 'keyword' },
} } };

const normEmail = (e) => String(e || '').trim().toLowerCase();
const isEmail = (e) => /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(e);

async function ensure(client) {
  try { const e = await client.indices.exists({ index: INDEX }); if ((e.body ?? e) === true || e.statusCode === 200) return false; } catch (x) { /* fall through to create */ }
  try { await client.indices.create({ index: INDEX, body: MAPPING }); return true; }
  catch (e) { if (/resource_already_exists/.test(String((e && e.message) || e))) return false; throw e; }
}

// Create/refresh a PENDING opt-out request; returns { email, already, token }. Removes nothing yet — removal
// only happens on confirm(). Idempotent: an already-confirmed email stays confirmed (no new token).
async function requestOptOut(client, { email, name = '', reason = '', ip = '' }) {
  const id = normEmail(email);
  if (!isEmail(id)) throw new Error('invalid email');
  let existing = null;
  try { const g = await client.get({ index: INDEX, id }); existing = (g.body || g)._source; } catch (e) { /* new */ }
  if (existing && existing.status === 'confirmed') return { email: id, already: true, token: null };
  const token = crypto.randomBytes(24).toString('hex');
  await client.index({ index: INDEX, id, refresh: true, body: {
    email: id, name: String(name || '').slice(0, 120), reason: String(reason || '').slice(0, 500),
    token, status: 'pending', requested_at: new Date().toISOString(), source_ip: String(ip || '').slice(0, 64) } });
  return { email: id, already: false, token };
}

// Confirm a pending request by its token. Returns the email (now confirmed) or null if the token is unknown.
async function confirm(client, token) {
  token = String(token || '').trim();
  if (!token || token.length < 16) return null;
  let hit;
  try { const r = await client.search({ index: INDEX, body: { size: 1, query: { term: { token } } } }); hit = (r.body || r).hits.hits[0]; }
  catch (e) { return null; }
  if (!hit) return null;
  const email = hit._source.email;
  await client.update({ index: INDEX, id: hit._id, refresh: true, body: { doc: { status: 'confirmed', confirmed_at: new Date().toISOString(), token: '' } } });
  return email;
}

module.exports = { INDEX, MAPPING, ensure, requestOptOut, confirm, normEmail, isEmail };
