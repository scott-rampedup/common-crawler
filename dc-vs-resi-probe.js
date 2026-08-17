/**
 * dc-vs-resi-probe.js — on a sample of our not-in-CC bio URLs, measure the two-tier split:
 * how many pages the cheap DATACENTER proxy handles vs. how many fall through to RESIDENTIAL vs. fail.
 * That ratio sets the true cost of the full 13.6M live-crawl. EVOMI_DC + EVOMI_RES env = proxy URLs.
 */
const fs = require('fs');
const readline = require('readline');
const http = require('http');
const https = require('https');
const { HttpsProxyAgent } = require('https-proxy-agent');
const { HttpProxyAgent } = require('http-proxy-agent');
const DC = process.env.EVOMI_DC, RES = process.env.EVOMI_RES;
const file = process.argv[2]; const CONC = Number(process.argv[3]) || 20;
if (!DC || !RES || !file) { console.error('need EVOMI_DC, EVOMI_RES env + <urls file>'); process.exit(1); }

const CHALLENGE = /just a moment|access denied|reference #\d|attention required|verify you are human|enable javascript and cookies|cf-browser-verification/i;
const realPage = (html) => html && html.length > 1500 && !CHALLENGE.test(html.slice(0, 4000));

function fetch(url, proxy) {
  return new Promise((res) => {
    const t0 = Date.now();
    const isHttps = url.startsWith('https:');
    const mod = isHttps ? https : http;
    const agent = isHttps ? new HttpsProxyAgent(proxy, { keepAlive: false }) : new HttpProxyAgent(proxy, { keepAlive: false });
    const req = mod.get(url, { agent, timeout: 12000 }, (r) => {
      let html = ''; r.on('data', (d) => { html += d; if (html.length > 400000) { r.destroy(); } });
      r.on('end', () => res({ code: r.statusCode, ms: Date.now() - t0, html }));
      r.on('close', () => res({ code: r.statusCode, ms: Date.now() - t0, html }));
    });
    req.on('timeout', () => { req.destroy(); res({ err: 'timeout', ms: Date.now() - t0, html: '' }); });
    req.on('error', (e) => res({ err: e.code || e.message, ms: Date.now() - t0, html: '' }));
  });
}

(async () => {
  const urls = [];
  const rl = readline.createInterface({ input: fs.createReadStream(file), crlfDelay: Infinity });
  for await (const l of rl) { const u = l.trim(); if (u) urls.push(/^https?:/i.test(u) ? u : 'https://' + u); }
  console.log(`probing ${urls.length} URLs (conc ${CONC})`);
  const t = { dcReal: 0, dcMs: 0, resiRescue: 0, bothFail: 0, dc403: 0, dcBlocked: 0, dcFail: 0 };
  let i = 0;
  async function worker() {
    for (;;) {
      const k = i++; if (k >= urls.length) return;
      const u = urls[k];
      const d = await fetch(u, DC); t.dcMs += d.ms;
      if (d.code === 200 && realPage(d.html)) { t.dcReal++; }
      else {
        if (d.code === 403) t.dc403++; else if (d.code === 200) t.dcBlocked++; else t.dcFail++;
        const r = await fetch(u, RES);
        if (r.code === 200 && realPage(r.html)) t.resiRescue++; else t.bothFail++;
      }
      if ((k + 1) % 200 === 0) console.error(`  ${k + 1}/${urls.length} | dc ${t.dcReal} | resi-rescue ${t.resiRescue} | fail ${t.bothFail}`);
    }
  }
  await Promise.all(Array.from({ length: CONC }, worker));
  const n = urls.length; const pct = (x) => (100 * x / n).toFixed(1) + '%';
  console.log('\n===== DC vs Residential (two-tier) =====');
  console.log(`URLs ${n}`);
  console.log(`DC handled (real page):   ${t.dcReal} (${pct(t.dcReal)})  | avg DC ${Math.round(t.dcMs / n)}ms`);
  console.log(`Residential rescued:      ${t.resiRescue} (${pct(t.resiRescue)})  [DC miss: 403 ${t.dc403}, block/short ${t.dcBlocked}, err ${t.dcFail}]`);
  console.log(`Both failed (404/dead):   ${t.bothFail} (${pct(t.bothFail)})`);
  console.log(`\nOverall fetch success:    ${pct(t.dcReal + t.resiRescue)}  | residential share of successes: ${((100 * t.resiRescue / (t.dcReal + t.resiRescue)) || 0).toFixed(1)}%`);
})();
