// Build a domain->company lookup for enrichment: load the contact-domain set, stream the company NDJSON
// (stdin), and emit one JSON line per matching domain. Each contact domain matches at most once (first win).
//   gzip -dc company.json.zip | node _build-company-lookup.js contact-domains.txt lookup.ndjson
const fs = require('fs');
const readline = require('readline');
const want = new Set();
for (const d of fs.readFileSync(process.argv[2], 'utf8').split('\n')) { const t = d.trim(); if (t) want.add(t); }
console.error('want domains: ' + want.size.toLocaleString());
function norm(w) {
  if (!w) return '';
  return String(w).toLowerCase().trim().replace(/^https?:\/\//, '').replace(/^www\./, '').split('/')[0].split('?')[0];
}
const out = fs.createWriteStream(process.argv[3] || 'company-lookup.ndjson');
const rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
let n = 0, matched = 0;
rl.on('line', (l) => {
  n++; let o; try { o = JSON.parse(l); } catch (e) { return; }
  if (!o.website) return;
  const d = norm(o.website);
  if (!d || !want.has(d)) return;
  want.delete(d);                       // one row per contact domain
  matched++;
  const hq = [o.locality, o.region, o.country].filter(Boolean).join(', ');
  out.write(JSON.stringify({ domain: d, industry: o.industry || '', company_size: o.size || '', hq_location: hq,
    company_country: o.country || '', founded: o.founded || null, company_linkedin: o.linkedin_url || '', company_name: o.name || '' }) + '\n');
});
rl.on('close', () => { out.end(() => console.error(`DONE: scanned ${n.toLocaleString()}, matched ${matched.toLocaleString()} contact domains`)); });
