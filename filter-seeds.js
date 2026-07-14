// Filter company NDJSON (stdin) to target industries x target countries with a website -> deduped seed
// domains (TSV: domain, size, industry, country). Reports counts by industry/country/size band.
const fs = require('fs');
const readline = require('readline');
const TARGET_COUNTRIES = new Set(['united states', 'united kingdom', 'canada', 'australia']);
const TARGET_INDUSTRIES = new Set([
  'accounting',
  'financial services', 'banking', 'capital markets',
  'real estate', 'commercial real estate',
  'insurance',
  'investment banking', 'investment management', 'venture capital & private equity',
  'legal services', 'law practice',
  'government administration', 'government relations', 'international affairs', 'public policy', 'legislative office', 'judiciary',
  'non-profit organization management', 'civic & social organization', 'philanthropy', 'fund-raising',
  'higher education', 'education management',
]);
function normDomain(w) {
  if (!w) return '';
  return String(w).toLowerCase().trim().replace(/^https?:\/\//, '').replace(/^www\./, '').split('/')[0].split('?')[0];
}
const seen = new Set();
const byInd = {}, byCtry = {}, bySize = {};
const out = fs.createWriteStream(process.argv[2] || 'seeds.tsv');
const rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
let n = 0, kept = 0;
rl.on('line', (l) => {
  n++; let o; try { o = JSON.parse(l); } catch (e) { return; }
  if (!o.website || !TARGET_COUNTRIES.has(o.country) || !TARGET_INDUSTRIES.has(o.industry)) return;
  const d = normDomain(o.website); if (!d || seen.has(d)) return;
  seen.add(d); kept++;
  byInd[o.industry] = (byInd[o.industry] || 0) + 1;
  byCtry[o.country] = (byCtry[o.country] || 0) + 1;
  bySize[o.size || '(null)'] = (bySize[o.size || '(null)'] || 0) + 1;
  out.write(d + '\t' + (o.size || '') + '\t' + o.industry + '\t' + o.country + '\n');
});
rl.on('close', () => {
  out.end(() => {
    console.log('scanned ' + n.toLocaleString() + ' | kept unique domains ' + kept.toLocaleString());
    console.log('--- by industry ---'); Object.entries(byInd).sort((a, b) => b[1] - a[1]).forEach(([k, v]) => console.log(String(v).padStart(9), k));
    console.log('--- by country ---'); Object.entries(byCtry).sort((a, b) => b[1] - a[1]).forEach(([k, v]) => console.log(String(v).padStart(9), k));
    console.log('--- by size band ---'); Object.entries(bySize).sort((a, b) => b[1] - a[1]).forEach(([k, v]) => console.log(String(v).padStart(9), k));
  });
});
