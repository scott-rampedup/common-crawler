/**
 * ai-enrich.js — "AI Search" field cleanup for contact records.
 *
 * Sends ONE contact's existing fields to Claude and gets back corrected values
 * for the job title (Position), First, Last, Email, Phone, and LinkedIn. There is
 * NO web access — Claude cleans and infers from the data provided only.
 * Requires the ANTHROPIC_API_KEY env var (set as a Fly secret in production).
 */
const Anthropic = require('@anthropic-ai/sdk');
const { normalizeTitle } = require('./title-normalize');
const { nameStringFromPath } = require('./name-from-path');

const MODEL = 'claude-opus-4-8';

let _client = null;
function client() {
  if (_client) return _client;
  if (!process.env.ANTHROPIC_API_KEY) return null;   // caller surfaces a clear error
  _client = new Anthropic();                          // reads ANTHROPIC_API_KEY from env
  return _client;
}
function isConfigured() { return !!process.env.ANTHROPIC_API_KEY; }

// Structured-output schema. Each value is the corrected field, or "" to keep the existing one.
const SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    title:    { type: 'string', description: 'Corrected job title/role, or "" to keep existing' },
    first:    { type: 'string', description: 'Corrected first name, or "" to keep existing' },
    last:     { type: 'string', description: 'Corrected last name, or "" to keep existing' },
    email:    { type: 'string', description: 'Best email per the rules below, or "" to keep existing' },
    linkedin: { type: 'string', description: 'LinkedIn profile URL if confidently known, else ""' },
  },
  required: ['title', 'first', 'last', 'email', 'linkedin'],
};

const SYSTEM = [
  'You are a data-quality assistant that corrects CRM contact records.',
  "You are given ONE contact's existing fields and must return corrected values for the five output fields.",
  'Work ONLY from the data provided — you have no internet access. Never invent data you cannot derive.',
  '',
  'Priority order of importance: Title, First, Last, Email, LinkedIn.',
  '- Only change a field when you are confident it is MORE accurate; otherwise return "" (keep existing).',
  '- First / Last: the "Name (from URL slug)" field is the person\'s name parsed from the page URL and is',
  '  usually the most reliable source — use it to set First (first word) and Last (last word) when it looks',
  '  like a real person name. Otherwise fix casing, split the existing full name, or derive from the email',
  '  local-part when obvious.',
  '- Title: produce a clean job title ONLY from explicit role evidence in the Title, Position, or Description',
  '  fields. The person\'s NAME is NOT a source of title — treat First/Last purely as a name, never as a role',
  '  or an abbreviation. For example: a first name of "Cooper" does NOT mean "COO", and "Christopher" does NOT',
  '  mean "CTO". If there is no genuine job-title evidence in the Title/Position/Description, return "" (keep',
  '  existing). Never fabricate or guess a role.',
  '- LinkedIn: only return a URL if it is clearly and confidently derivable; otherwise "".',
  '- Do NOT change the phone number — it is not one of your output fields.',
  '',
  'Email rule (important):',
  '- A "professional" email uses the company domain and is named after the person (e.g. jane.doe@acme.com).',
  '- A "role-based" email is generic (info@, sales@, admin@, contact@, hello@). A "personal" email uses a',
  '  consumer domain (gmail, yahoo, outlook, hotmail, icloud, aol).',
  '- If a professional email can be assigned to this contact, prefer it over a role-based or personal address.',
  '- NEVER replace an existing professional email with a role-based or personal one. If the current Email Address',
  '  is already professional, return "" for email.',
  '- Only assign an email you can justify from the provided data — do not guess addresses.',
].join('\n');

function fieldContext(record) {
  const g = (k) => String(record[k] == null ? '' : record[k]).trim();
  return {
    Title: g('Title'),
    Position: g('Position'),
    First: g('First'),
    Last: g('Last'),
    'Name (from URL slug)': nameStringFromPath(record['Last Path']),
    'Last Path': g('Last Path'),
    'Email Address': g('Email Address'),
    'Email Type': g('Email Type'),
    Phone: g('Phone'),
    'Phone Type': g('Phone Type'),
    'LinkedIn URL': g('LinkedIn URL'),
    Domain: g('Domain'),
    'Web Source URL': g('Web Source URL'),
    Description: g('Description').slice(0, 1200),
  };
}

// AI output key -> record field. "title" maps to the visible job-title column (Position).
const FIELD_MAP = {
  title: 'Position',
  first: 'First',
  last: 'Last',
  email: 'Email Address',
  linkedin: 'LinkedIn URL',
};

// -> { updates: {Field: value}, changes: {Field: {from, to}} } — only non-empty diffs.
async function enrichRecord(record) {
  const c = client();
  if (!c) throw new Error('ANTHROPIC_API_KEY is not set');
  const res = await c.messages.create({
    model: MODEL,
    max_tokens: 2048,
    thinking: { type: 'adaptive' },
    output_config: { effort: 'low', format: { type: 'json_schema', schema: SCHEMA } },
    system: SYSTEM,
    messages: [{
      role: 'user',
      content: 'Existing contact record:\n' + JSON.stringify(fieldContext(record), null, 2),
    }],
  });
  const text = (res.content || []).filter((b) => b.type === 'text').map((b) => b.text).join('');
  let out;
  try { out = JSON.parse(text); } catch (e) { throw new Error('model returned non-JSON output'); }

  const updates = {}, changes = {};
  for (const k of Object.keys(FIELD_MAP)) {
    const field = FIELD_MAP[k];
    let next = String(out[k] == null ? '' : out[k]).trim();
    if (k === 'title' && next) next = normalizeTitle(next);   // validate + canonical-case the job title
    const cur = String(record[field] == null ? '' : record[field]).trim();
    if (next && next !== cur) { updates[field] = next; changes[field] = { from: cur, to: next }; }
  }
  return { updates, changes };
}

// Run enrichRecord over many records with bounded concurrency.
async function enrichMany(records, { concurrency = 4 } = {}) {
  const out = new Array(records.length);
  let i = 0;
  async function worker() {
    while (i < records.length) {
      const idx = i++;
      try { out[idx] = { ok: true, ...(await enrichRecord(records[idx])) }; }
      catch (e) { out[idx] = { ok: false, error: e.message || 'enrich failed' }; }
    }
  }
  const n = Math.max(1, Math.min(concurrency, records.length || 1));
  await Promise.all(Array.from({ length: n }, worker));
  return out;
}

module.exports = { enrichRecord, enrichMany, isConfigured };
