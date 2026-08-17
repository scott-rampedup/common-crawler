/**
 * evomi-conc-probe.js — empirically probe the Evomi residential proxy's usable concurrency.
 * Fires bursts of N parallel requests (tiny payload) at rising N; where throughput plateaus or
 * errors appear = the effective thread ceiling. Also does a few full-page fetches to gauge per-page
 * latency/size (the bandwidth angle). EVOMI env = the proxy URL.
 */
const https = require('https');
const { HttpsProxyAgent } = require('https-proxy-agent');
const PROXY = process.env.EVOMI;
if (!PROXY) { console.error('set EVOMI=<proxy url>'); process.exit(1); }

function get(url, timeout = 20000) {
  return new Promise((res) => {
    const t0 = Date.now();
    const agent = new HttpsProxyAgent(PROXY, { keepAlive: false });
    const req = https.get(url, { agent, timeout }, (r) => {
      let bytes = 0; r.on('data', (d) => { bytes += d.length; }); r.on('end', () => res({ ok: r.status === 200 || r.statusCode === 200, code: r.statusCode, ms: Date.now() - t0, bytes }));
    });
    req.on('timeout', () => { req.destroy(); res({ ok: false, ms: Date.now() - t0, err: 'timeout' }); });
    req.on('error', (e) => res({ ok: false, ms: Date.now() - t0, err: e.code || e.message }));
  });
}

(async () => {
  console.log('== concurrency ramp (tiny payload: ipinfo.io/json) ==');
  for (const conc of [5, 10, 25, 50, 100, 150]) {
    const t0 = Date.now();
    const rs = await Promise.all(Array.from({ length: conc }, () => get('https://ipinfo.io/json')));
    const ok = rs.filter((r) => r.ok).length;
    const s = (Date.now() - t0) / 1000;
    const avg = Math.round(rs.reduce((a, r) => a + r.ms, 0) / conc);
    const errs = {}; for (const r of rs) if (!r.ok) errs[r.err || r.code] = (errs[r.err || r.code] || 0) + 1;
    console.log(`conc ${String(conc).padStart(3)} | ${ok}/${conc} ok | ${(ok / s).toFixed(1)} req/s | avg ${avg}ms | errs ${JSON.stringify(errs)}`);
  }
  console.log('\n== per-page fetch (real bio-ish page, bandwidth gauge) ==');
  const pages = ['https://www.swlaw.com/people/alina_mooradian/', 'https://www.compass.com/', 'https://www.tblaw.com/'];
  for (const u of pages) { const r = await get(u); console.log(`${u} -> [${r.code || r.err}] ${r.ok ? Math.round(r.bytes / 1024) + 'KB in ' + r.ms + 'ms' : r.err}`); }
})();
