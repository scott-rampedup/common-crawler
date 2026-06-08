# Phase 2 & Phase 3 — Scope (plain English)

_Written 2026-06-05, based on what's actually in this folder today._

## What we learned in Phase 1 (the foundation for everything below)
- The engine **works** — self-test passes 10/10.
- Common Crawl coverage is **uneven**:
  - **Strong** on small / local / service businesses that publish staff bio +
    contact pages (e.g. 1800packouts.com → clean names, professional emails,
    direct phones).
  - **Weak** on big SaaS/corporate sites (Stripe, Shopify) — lots of pages, almost
    no person-level contacts.
  - Many domains simply **aren't in the crawl** at all.
- **Implication:** the archive alone won't cover a real prospect list. That gap is
  exactly what Phase 3 (live gap-fill) is for.

---

## Phase 2 — The Product UI
**Status: ~70% built already.** A working dashboard exists (`ui-server.js`, `ui/`).
It can take pasted/uploaded domains, run a live Common Crawl search, stream results
in, filter them, and download a CSV. What's left is the difference between a "demo"
and a "tool the team uses every day."

### Gaps to close (in priority order)
1. **Results should accumulate, not overwrite.**
   Today every search overwrites `cc-results.csv`. A real tool needs a saved,
   growing database of contacts, de-duplicated across runs.
2. **Background jobs for big lists.**
   Runs are intentionally slow & polite (one domain at a time, with pauses). For a
   list of hundreds/thousands that's fine — but it can't depend on a browser tab
   staying open. Needs: start a job, close the tab, come back, see progress / resume.
3. **Accept Excel (.xlsx) input.**
   Your real lists (e.g. `Directories.xlsx`) are spreadsheets. Today the UI takes
   .csv/.txt only.
4. **Show the coverage number front-and-center.**
   "X of Y domains had usable contacts" is the core value — it should be a headline
   stat, not buried.
5. **Clean export to your CRM / RampedUp import format.**
   The CSV columns already look CRM-shaped (Directory, ID, Email Type, etc.) —
   confirm the exact target format and make export one click.

### Rough effort
Closing gaps 1–5: **a few focused sessions**, not weeks. The hard part (the engine +
live streaming) is done.

---

## Phase 3 — Live Gap-Fill
**Status: net new.** This is the answer to Phase 1's coverage problem.

### The idea
When a domain **isn't in Common Crawl**, or the archive gives **thin results**,
the tool politely visits the **live website** (the usual /about, /team, /contact,
/staff pages) and runs the *same* extractor on the live HTML. Same output format —
the user can't tell whether a contact came from the archive or the live fetch,
except via the "Source" column.

### Why it's high-value
It directly fixes the weak spots: uncrawled domains and stale/missing pages. It
turns "2 of 6 found" into something much closer to full coverage.

### Things to decide before building (these have real consequences)
- **Politeness & legality:** respect robots.txt, rate-limit, set a clear user-agent.
  Live scraping has different ToS/legal footing than reading a public archive — worth
  a deliberate decision, not an accident.
- **Where it runs & cost:** live fetches at scale may want a real host (not a laptop)
  and a budget. Still no paid data feeds — just bandwidth/compute.
- **Trigger rule:** only gap-fill when the archive comes up empty/thin, to keep it
  fast and polite.

### Rough effort
Reuses `extractor.js` and `wireless-block-classifier.js` unchanged. The new work is
a polite live-fetch module + the "when to trigger it" logic. **Comparable to Phase 1
itself** in size.

---

## Suggested sequence
1. **Confirm the CRM/export format** (cheap, unblocks Phase 2 gap #5).
2. **Phase 2 hardening** — persistence, background jobs, xlsx input, coverage stat.
3. **Phase 3 gap-fill** — once Phase 2 is the daily tool and the coverage gap is felt.

A smart cheap first step: **run your real `Directories.xlsx` list through the current
engine** to get a true coverage number. That number tells you how urgently you need
Phase 3 — and it's an afternoon, not a build.
