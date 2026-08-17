# run-universe-refresh.ps1 — scheduled wrapper for the whole-universe Common Crawl refresh (Phase 3).
# Idempotent: universe-scheduler.js no-ops until a NEW Common Crawl release drops, then runs the full
# two-hop Lambda routine (waterfall enrich -> load -> bio resolve -> extract -> load + live fallback)
# and advances the watermark (.universe-state.json). Task Scheduler points here.
$ErrorActionPreference = 'Continue'
$repo = 'C:\Users\scott\OneDrive\Desktop\Common Crawler Code\rampedup-phase1'
Set-Location $repo

# Where to read/write. AWS credentials come from the machine's default chain (~/.aws), same as manual runs.
$env:OPENSEARCH_ENDPOINT = 'search-contacts-prod-3wdhiegssjafz4v3hmtdbkgmmu.us-east-1.es.amazonaws.com'
$env:OUT_BUCKET = 'aws-athena-query-results-475987770186-us-east-1'

New-Item -ItemType Directory -Force -Path "$repo\logs" | Out-Null
$log = "$repo\logs\universe-$(Get-Date -Format 'yyyyMMdd-HHmmss').log"
"[$(Get-Date -Format o)] universe-refresh check starting" | Out-File -FilePath $log -Encoding utf8
# Redirect via cmd so node's stdout+stderr land in the log as raw UTF-8 (PowerShell's *>> mangles native
# stderr into error records and writes UTF-16). $LASTEXITCODE still reflects node's exit code.
cmd /c "node universe-scheduler.js >> `"$log`" 2>&1"
"[$(Get-Date -Format o)] finished (exit $LASTEXITCODE)" | Add-Content -Path $log -Encoding utf8
