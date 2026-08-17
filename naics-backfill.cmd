@echo off
REM Off-peak NAICS backfill: submits the async update_by_query that associates naics_code/naics_title
REM onto all ~40M companies from category (or industry). Runs server-side on OpenSearch after submit.
cd /d "C:\Users\scott\OneDrive\Desktop\Common Crawler Code\rampedup-phase1"
set "OPENSEARCH_ENDPOINT=search-contacts-prod-3wdhiegssjafz4v3hmtdbkgmmu.us-east-1.es.amazonaws.com"
"C:\Program Files\nodejs\node.exe" naics-enrich.js --async --rps 8000 > naics-backfill.log 2>&1
