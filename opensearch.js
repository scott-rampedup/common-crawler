/**
 * opensearch.js — the PRODUCTION contacts search store (Phase 1 of the two-plane architecture).
 * ----------------------------------------------------------------------------------------------
 * The processing plane (worker fleet / Lambda) writes contacts to its own store; this module is the
 * read-optimized store the front-end UI queries (search / filter / facet / export). Decoupling the two
 * removes the shared-Postgres hotspot that melted under load.
 *
 * Access is IAM/SigV4 (the domain policy allows the cc-athena principal), so every request is signed
 * with the AWS creds already in env (Fly secrets on the app box, ~/.aws locally). Doc _id = normalized
 * email → re-indexing a contact upserts it (dedupe), and higher score wins via a scripted upsert.
 *
 *   const { makeClient, ensureIndex, bulkUpsert, rowToDoc, search } = require('./opensearch');
 *   const os = makeClient(process.env.OPENSEARCH_ENDPOINT);
 */
const { Client } = require('@opensearch-project/opensearch');
const { AwsSigv4Signer } = require('@opensearch-project/opensearch/aws');
const { defaultProvider } = require('@aws-sdk/credential-provider-node');

const INDEX = process.env.OPENSEARCH_INDEX || 'contacts';
const REGION = process.env.AWS_REGION || 'us-east-1';

function makeClient(endpoint) {
  const node = endpoint || process.env.OPENSEARCH_ENDPOINT;
  if (!node) throw new Error('OPENSEARCH_ENDPOINT (or endpoint arg) required');
  return new Client({
    ...AwsSigv4Signer({ region: REGION, service: 'es', getCredentials: () => defaultProvider()() }),
    node: node.startsWith('http') ? node : 'https://' + node,
  });
}

// Index mapping: text for search, keyword for filters/facets/export/sort, integer score for ranking.
const MAPPING = {
  settings: { number_of_shards: 2, number_of_replicas: 1, 'index.mapping.total_fields.limit': 200 },
  mappings: { properties: {
    email:          { type: 'keyword' },
    first:          { type: 'text', fields: { kw: { type: 'keyword' } } },
    last:           { type: 'text', fields: { kw: { type: 'keyword' } } },
    name:           { type: 'text' },                                   // first + last, for search
    title:          { type: 'text', fields: { kw: { type: 'keyword' } } },
    position:       { type: 'keyword' },
    company:        { type: 'text', fields: { kw: { type: 'keyword' } } },
    domain:         { type: 'keyword' },
    description:    { type: 'text' },
    email_type:     { type: 'keyword' },
    gender:         { type: 'keyword' },
    phone:          { type: 'keyword' },
    phone_type:     { type: 'keyword' },
    phone_location: { type: 'keyword' },
    phone_2:        { type: 'keyword' },
    phone_2_type:   { type: 'keyword' },
    phone_2_location:{ type: 'keyword' },
    linkedin_url:   { type: 'keyword' },
    facebook:       { type: 'keyword' },
    twitter:        { type: 'keyword' },
    whatsapp:       { type: 'keyword' },
    google_maps:    { type: 'keyword' },
    vcard:          { type: 'keyword' },
    web_source_url: { type: 'keyword' },
    image_url:      { type: 'keyword' },
    source:         { type: 'keyword' },
    directory:      { type: 'keyword' },
    bio_check:      { type: 'keyword' },
    type:           { type: 'keyword' },
    score:          { type: 'integer' },
    time_stamp:     { type: 'keyword' },
    updated_at:     { type: 'date' },
  } },
};

async function ensureIndex(client) {
  const exists = await client.indices.exists({ index: INDEX });
  if (exists.body) return false;
  await client.indices.create({ index: INDEX, body: MAPPING });
  return true;
}

// Map a Postgres contacts row (snake_case columns) to an OpenSearch document.
function rowToDoc(r) {
  const first = r.first || '', last = r.last || '';
  return {
    email: r.email || r.email_address || '',
    first, last, name: `${first} ${last}`.trim(),
    title: r.title || '', position: r.position || '',
    company: r.company || r.domain || '', domain: r.domain || '',
    description: r.description || '',
    email_type: r.email_type || '', gender: r.gender || '',
    phone: r.phone || '', phone_type: r.phone_type || '', phone_location: r.phone_location || '',
    phone_2: r.phone_2 || '', phone_2_type: r.phone_2_type || '', phone_2_location: r.phone_2_location || '',
    linkedin_url: r.linkedin_url || '', facebook: r.facebook || '', twitter: r.twitter || '',
    whatsapp: r.whatsapp || '', google_maps: r.google_maps || '', vcard: r.vcard || '',
    web_source_url: r.web_source_url || '', image_url: r.image_url || '',
    source: r.source || '', directory: r.directory || '', bio_check: r.bio_check || '', type: r.type || '',
    score: Number(r.score) || 0, time_stamp: r.time_stamp || '',
    updated_at: r.updated_at || null,
  };
}

// Bulk index docs, _id = email. Score-gated upsert: a re-indexed email only overwrites if its score is
// >= the stored score (mirrors the Postgres upsert), so the best record for a person wins.
async function bulkUpsert(client, docs) {
  const body = [];
  for (const d of docs) {
    if (!d.email) continue;
    body.push({ update: { _index: INDEX, _id: d.email.toLowerCase() } });
    body.push({
      scripted_upsert: true, upsert: d,
      script: { lang: 'painless',
        source: "if (params.doc.score >= ctx._source.score) { ctx._source = params.doc }",
        params: { doc: d } },
    });
  }
  if (!body.length) return { indexed: 0, errors: 0 };
  const res = await client.bulk({ body, refresh: false });
  let errors = 0;
  if (res.body.errors) for (const it of res.body.items) if (it.update && it.update.error) errors++;
  return { indexed: docs.length, errors };
}

module.exports = { makeClient, ensureIndex, rowToDoc, bulkUpsert, INDEX, MAPPING };
