#!/usr/bin/env bash
# Wave 2b: finish the parts that failed on ephemeral-port exhaustion. Small files first (drive+load),
# then re-load people-company (its records are already in S3 from wave 2's drive). Gentler concurrency.
set -o pipefail
cd "c:/Users/scott/OneDrive/Desktop/Common Crawler Code/rampedup-phase1" || exit 1
export OPENSEARCH_ENDPOINT="search-contacts-prod-3wdhiegssjafz4v3hmtdbkgmmu.us-east-1.es.amazonaws.com"
export AWS_REGION="us-east-1"
export OUT_BUCKET="aws-athena-query-results-475987770186-us-east-1"
export LOAD_CONC=6

phones(){ node -e "const os=require('./opensearch');const c=os.makeClient(process.env.OPENSEARCH_ENDPOINT);(async()=>{await c.indices.refresh({index:'contacts'}).catch(()=>{});const t=(await c.count({index:'contacts'})).body.count;const p=(await c.count({index:'contacts',body:{query:{prefix:{phone:'+'}}}})).body.count;console.log('  total',t.toLocaleString(),'| real-phone',p.toLocaleString());})()"; }

echo "=== WAVE 2b BEFORE $(date) ==="; phones
for f in cc-warc-bio-repo cc-warc-mmiss-all cc-warc-tld-expand; do
  [ -f "$f.jsonl" ] || { echo "skip $f"; continue; }
  echo "=== drive $f $(date) ==="
  RUN="bf-$f" BATCH=200 CONCURRENCY=120 node lambda-drive.js "$f.jsonl" 2>&1 | tee "logs/backfill-$f-drive2.log" || { echo "$f DRIVE-FAILED"; continue; }
  echo "=== load $f $(date) ==="
  node load-extracted.js "cc-extracted/bf-$f/" 2>&1 | tee "logs/backfill-$f-load2.log" || { echo "$f LOAD-FAILED"; continue; }
done
echo "=== reload people-company (records already in S3) $(date) ==="
node load-extracted.js "cc-extracted/bf-cc-warc-people-company/" 2>&1 | tee "logs/backfill-people-company-load2.log" || echo "people-company RELOAD-FAILED"
echo "=== WAVE 2b DONE $(date) ==="; phones
