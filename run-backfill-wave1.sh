#!/usr/bin/env bash
# Backfill wave 1: re-extract cc-warc-mon-all.jsonl (6.79M CC pointers) with the phone-fixed Lambda,
# load into OpenSearch (score-gated upsert -> fills phones onto the existing phone-less records).
set -o pipefail
cd "c:/Users/scott/OneDrive/Desktop/Common Crawler Code/rampedup-phase1" || exit 1
export OPENSEARCH_ENDPOINT="search-contacts-prod-3wdhiegssjafz4v3hmtdbkgmmu.us-east-1.es.amazonaws.com"
export AWS_REGION="us-east-1"
export OUT_BUCKET="aws-athena-query-results-475987770186-us-east-1"

echo "=== BEFORE $(date) ==="
node -e "const os=require('./opensearch');const c=os.makeClient(process.env.OPENSEARCH_ENDPOINT);(async()=>{await c.indices.refresh({index:'contacts'}).catch(()=>{});const t=(await c.count({index:'contacts'})).body.count;const p=(await c.count({index:'contacts',body:{query:{prefix:{phone:'+'}}}})).body.count;console.log('total',t.toLocaleString(),'| real-phone',p.toLocaleString());})()"

echo "=== [1/2] lambda-drive cc-warc-mon-all.jsonl $(date) ==="
RUN=backfill-mon BATCH=200 CONCURRENCY=250 node lambda-drive.js cc-warc-mon-all.jsonl 2>&1 | tee logs/backfill-mon-drive.log || { echo DRIVE-FAILED; exit 1; }

echo "=== [2/2] load-extracted cc-extracted/backfill-mon/ $(date) ==="
node load-extracted.js cc-extracted/backfill-mon/ 2>&1 | tee logs/backfill-mon-load.log || { echo LOAD-FAILED; exit 1; }

echo "=== AFTER $(date) ==="
node -e "const os=require('./opensearch');const c=os.makeClient(process.env.OPENSEARCH_ENDPOINT);(async()=>{await c.indices.refresh({index:'contacts'}).catch(()=>{});const t=(await c.count({index:'contacts'})).body.count;const p=(await c.count({index:'contacts',body:{query:{prefix:{phone:'+'}}}})).body.count;console.log('total',t.toLocaleString(),'| real-phone',p.toLocaleString());})()"
echo "=== BACKFILL WAVE 1 DONE $(date) ==="
