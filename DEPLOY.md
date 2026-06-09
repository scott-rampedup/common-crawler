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
