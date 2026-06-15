# Hosting the Contact Finder

## ✅ Current live deployment (Fly.io)

- **URL:** https://common-crawler.fly.dev  (HTTPS, password-protected)
- **Host:** Fly.io app `common-crawler`, region `ewr`, 1 GB persistent volume `data` at `/data`
- **Login:** any username + the `APP_PASSWORD` secret (set via `fly secrets`).
- **Redeploy:** from this folder, `fly deploy --app common-crawler --ha=false`
  (uses `Dockerfile` + `fly.toml`). Change the password with
  `fly secrets set APP_PASSWORD=newpass --app common-crawler`.
- **Note:** the machine is set `auto_stop_machines = false` so background jobs keep
  running between requests.

### Residential proxy (for bot-protected sites)

Some target sites (Akamai/Cloudflare, e.g. `howardhanna.com`) return **403** to a
datacenter IP, so live page fetches need a **residential rotating proxy**. The engine
routes every live fetch through `PROXY_URL` when set, and rolls a fresh exit IP on each
blocked retry. (A datacenter proxy will still be blocked — it must be residential.)

1. Get a residential gateway URL from a provider (NetNut / Bright Data / Oxylabs /
   Decodo). Format: `http://USER:PASS@GATEWAY:PORT` (see `.env.example` for per-provider
   examples). Prefer a **per-request rotating** endpoint.
2. Set it: `fly secrets set PROXY_URL='http://user:pass@gateway:port' --app common-crawler`
   (this restarts the machine; no redeploy needed).
3. Verify: `PROXY_URL='...' node cc-engine.js --proxy-test` (locally) — shows the exit IP
   across 3 calls (should vary) and whether it can fetch a Howard Hanna agent page.
4. For Akamai/Cloudflare-JS sites (e.g. `howardhanna.com`) a plain residential proxy still
   gets a JS/TLS challenge it can't solve. Add a **Website Unblocker** as a third tier:
   `fly secrets set PROXY_UNBLOCKER_URL='http://user:pass@unblocker-endpoint:port' --app common-crawler`.
   It runs a real browser, so it fires **last** (priciest) — only when datacenter + residential
   are both blocked. TLS verification is skipped for this tier (unblockers MITM HTTPS).

Escalation order per live fetch: `PROXY_URL` (datacenter) → `PROXY_FALLBACK_URL` (residential)
→ `PROXY_UNBLOCKER_URL` (unblocker). `--proxy-test` shows all configured tiers.

The Render guide below is an alternative path if you ever want to move hosts.

---

# Hosting on Render (alternative — plain-English guide)

This puts the tool online for you and a few teammates, behind a password. We use
**Render** because it's the simplest managed option: you connect a GitHub repo, it
builds and runs the app, gives you an HTTPS URL, and keeps it running 24/7.

**Rough cost:** ~$7/month (Render "Starter"). The free tier won't work here because
it sleeps when idle and has no persistent disk — and we need both (jobs run in the
background and must survive restarts).

---

## Part A — Put the code on GitHub (private)

1. Create a **private** repo on GitHub (e.g. `rampedup-contact-finder`). Don't add a
   README/.gitignore in the GitHub UI — this folder already has them.
2. From this folder, push the code:
   ```
   git init
   git add .
   git commit -m "Contact finder: engine + live crawl + job dashboard"
   git branch -M main
   git remote add origin https://github.com/<you>/rampedup-contact-finder.git
   git push -u origin main
   ```
   (If you'd rather, I can run these for you.)

What gets uploaded: the app code and the `WIRELESS_BLOCKS.TXT` data file it needs.
What is deliberately **left out** (see `.gitignore`): all scraped results, the
`jobs/` data, and the `.xlsx` files — because those contain personal contact data.

---

## Part B — Deploy on Render

1. Go to <https://render.com>, sign up / log in, and connect your GitHub account.
2. Click **New +  >  Blueprint**, pick your repo. Render reads `render.yaml` and
   proposes a web service named **rampedup-contact-finder** with a 1 GB disk.
3. It will ask you to set **`APP_PASSWORD`** (marked "sync: false" so it's a secret).
   Enter a long random password — this is what you and your team will type to log in.
   - For separate per-person logins instead, skip `APP_PASSWORD` and add an env var
     `AUTH_USERS` = `alice:pw1,bob:pw2` (comma-separated `user:password` pairs).
4. Click **Apply**. Render installs and starts the app (first build ~1–2 min).
5. When it's live, Render shows a URL like `https://rampedup-contact-finder.onrender.com`.
   Open it — your browser will prompt for the password. You're in.

---

## Using it once hosted

- Same dashboard as local: paste domains  >  **FIND CONTACTS**  >  jobs run on the
  server. You can close the tab and come back; results and progress persist on the disk.
- To change the password later: Render dashboard  >  the service  >  **Environment**  >
  edit `APP_PASSWORD`  >  save (it redeploys automatically).
- Every time you `git push` to `main`, Render auto-redeploys the new version.

---

## Good to know / before scaling up

- **Discovery + politeness:** the live crawler reads each site's `robots.txt`, follows
  its `Sitemap:` entries to find every bio/contact page, and **honors `Disallow`**.
  It pauses between requests and sends a clear user-agent. It does not yet honor
  `Crawl-delay` — a possible future refinement.
- **Speed knobs (env vars):**
  - `DOMAIN_CONCURRENCY` (default 6) — how many different domains to crawl at once.
  - `IN_SITE_CONCURRENCY` (default 3) — pages fetched from a single site at once.
  - `CC_CONCURRENCY` (default 1) — global Common Crawl request limit; keep low, it's
    a shared public service.
  - `LIVE_MAX_PAGES` (default 150; 300 on the live deploy) — max pages per site.
  - `LIVE_ONLY=true` — skip Common Crawl entirely (also a per-search checkbox in the UI).
  Higher concurrency = faster but more load/memory; the deploy runs 1 GB / 2 CPUs.
- **PII:** the output is real people's emails/phones. Keep the password private, and
  prefer per-person `AUTH_USERS` if more than a couple of people have access.
- **Persistence detail:** job files are rewritten after each domain. Fine at current
  list sizes; for very large lists, an append-only store would be a future tune-up.
- **Other hosts:** the same setup works on Railway or Fly.io (Node service + a 1 GB
  volume mounted where `DATA_DIR` points + the `APP_PASSWORD`/`AUTH_USERS` env vars).
