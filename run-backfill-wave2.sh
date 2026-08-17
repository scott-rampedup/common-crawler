#!/usr/bin/env bash
# Backfill wave 2: re-extract the remaining on-disk WARC pointer sets through the phone-fixed Lambda,
# loading each into OpenSearch (score-gated upsert -> phones onto existing records). Idempotent; some
# overlap with wave 1 (mon) is harmless. Ordered largest-coverage first.
set -o pipefail
cd "c:/Users/scott/OneDrive/Desktop/Common Crawler Code/rampedup-phase1" || exit 1
export OPENSEARCH_ENDPOINT="search-contacts-prod-3wdhiegssjafz4v3hmtdbkgmmu.us-east-1.es.amazonaws.com"
export AWS_REGION="us-east-1"
export OUT_BUCKET="aws-athena-query-results-475987770186-us-east-1"

phones(){ node -e "const os=require('./opensearch');const c=os.makeClient(process.env.OPENSEARCH_ENDPOINT);(async()=>{await c.indices.refresh({index:'contacts'}).catch(()=>{});const t=(await c.count({index:'contacts'})).body.count;const p=(await c.count({index:'contacts',body:{query:{prefix:{phone:'+'}}}})).body.count;console.log('  total',t.toLocaleString(),'| real-phone',p.toLocaleString());})()"; }

echo "=== WAVE 2 BEFORE $(date) ==="; phones
for f in cc-warc-full cc-warc-people-company cc-warc-bio-repo cc-warc-mmiss-all cc-warc-tld-expand; do
  [ -f "$f.jsonl" ] || { echo "skip $f (no file)"; continue; }
  echo "=== drive $f $(date) ==="
  RUN="bf-$f" BATCH=200 CONCURRENCY=250 node lambda-drive.js "$f.jsonl" 2>&1 | tee "logs/backfill-$f-drive.log" || { echo "$f DRIVE-FAILED"; continue; }
  echo "=== load $f $(date) ==="
  node load-extracted.js "cc-extracted/bf-$f/" 2>&1 | tee "logs/backfill-$f-load.log" || { echo "$f LOAD-FAILED"; continue; }
  echo "--- after $f ---"; phones
done
echo "=== WAVE 2 DONE $(date) ==="; phones
