# Run Phase 1 — Handoff for Your AI Coding Tool

You don't need to write or understand any code to do this. There are two parts:
**Part A** is three setup steps for you. **Part B** is a block of text you copy and
paste to your AI coding tool (Claude Code, Claude Cowork, Cursor, or whatever you
used to build your extensions) — it tells the tool exactly what to do.

---

## Part A — Set up the folder (about 5 minutes, no coding)

1. **Make a new empty folder** on your computer, e.g. `rampedup-phase1`.

2. **Put these five files in it** (all in the same folder, no subfolders):
   - `cc-engine.js`
   - `extractor.js`
   - `wireless-block-classifier.js`
   - `WIRELESS_BLOCKS.TXT`  ← your wireless block file
   - `sample-domains.csv`  ← starter list to test with (replace later with your real domains)

3. **Open your AI coding tool and point it at that folder.** In Claude Code or
   Cursor, that means opening the folder as your project/workspace. Then paste the
   text in **Part B** as your first message and let it work.

That's it. The tool handles everything technical from here — installing what's
needed, running things, and explaining the results back to you.

---

## Part B — Paste this to your AI coding tool

> I'm not a developer, so please do the technical work yourself, explain each step
> in plain English, and don't ask me to run terminal commands unless absolutely
> necessary.
>
> **Goal:** Run a data pipeline called "Phase 1" that reads a list of company
> domains, looks them up in the free Common Crawl web archive, extracts contact
> records (people, emails, phones, etc.) from the archived pages, and writes the
> results to a CSV. Then tell me, in plain English, how good the coverage was.
>
> **The files are all in this folder:**
> - `cc-engine.js` — the main program (the entry point)
> - `extractor.js` — turns a web page into a contact record
> - `wireless-block-classifier.js` — classifies phone numbers as mobile/landline
> - `WIRELESS_BLOCKS.TXT` — data the classifier needs (keep it in this folder)
> - `sample-domains.csv` — the list of domains to start with
>
> **Please do this, in order:**
>
> 1. **Check Node.js.** This is a Node.js project. If Node 18 or newer isn't
>    installed, install it (or tell me the simplest way to). No other packages are
>    needed — the code uses only built-in Node features.
>
> 2. **Run the built-in self-test FIRST, before any internet calls:**
>    `node cc-engine.js --selftest`
>    It should finish with "8 passed, 0 failed". If anything fails, stop and tell me
>    what failed in plain English — do not try to "fix" the extraction logic.
>
> 3. **Do a small real run** on the starter list:
>    `node cc-engine.js sample-domains.csv`
>    This calls Common Crawl over the internet. It's intentionally slow and polite
>    (one request at a time, with pauses) — that's correct, please don't speed it up
>    by making it parallel. It will print a progress line per domain and write
>    `cc-results.csv`.

    If your network blocks Common Crawl, set `HTTPS_PROXY` or `HTTP_PROXY` before
    running, for example on Windows:
    `set HTTPS_PROXY=http://proxy.example.com:3128`
>
> 4. **Report back to me, in plain English:**
>    - The "Coverage" line (how many domains were found in the archive vs. not).
>    - How many total people/records were extracted.
>    - Open `cc-results.csv` and show me 3–5 example rows so I can judge quality —
>      especially the Name, Title, Email, Email Type, Phone, and Phone Type columns.
>
> **Important guardrails:**
> - Don't change the logic in `extractor.js` or `wireless-block-classifier.js` —
>   those are already tested and verified. You're only running the program.
> - Don't sign up for anything paid, don't use AWS, and don't download whole
>   Common Crawl files. This phase only makes small, free index lookups and
>   fetches a few pages per domain.
> - If a domain isn't in the archive, that's expected and fine — just note it.
>
> When the small run looks good, I'll give you my real list of domains to run next.

---

## What success looks like

- The self-test prints **"8 passed, 0 failed."** → the code works.
- The real run prints a **Coverage** line and a **People** count, and creates
  **`cc-results.csv`** with real rows in it.

That coverage number is the whole point of Phase 1. If the archive covers a good
share of your real target domains with clean records, you greenlight Phase 2 (the
product UI) and Phase 3 (live gap-fill). If coverage is thin, you've learned it for
the cost of an afternoon — before building anything else.

## When you have results

Bring the coverage numbers and a few sample rows back to this chat, and we'll read
them together and scope the next phase against what you actually found.
