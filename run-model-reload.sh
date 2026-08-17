#!/usr/bin/env bash
# Follow-up #2: re-load the existing Lambda backfill outputs WITH --model, so records that have a phone
# but no email get a modeled address (domain pattern from the central index) and land instead of being
# dropped. Gentle LOAD_CONC to avoid the ephemeral-port exhaustion seen in wave 2.
set -o pipefail
cd "c:/Users/scott/OneDrive/Desktop/Common Crawler Code/rampedup-phase1" || exit 1
export OPENSEARCH_ENDPOINT="search-contacts-prod-3wdhiegssjafz4v3hmtdbkgmmu.us-east-1.es.amazonaws.com"
export AWS_REGION="us-east-1"
export OUT_BUCKET="aws-athena-query-results-475987770186-us-east-1"
export LOAD_CONC=4

phones(){ node -e "const os=require('./opensearch');const c=os.makeClient(process.env.OPENSEARCH_ENDPOINT);(async()=>{await c.indices.refresh({index:'contacts'}).catch(()=>{});const t=(await c.count({index:'contacts'})).body.count;const p=(await c.count({index:'contacts',body:{query:{prefix:{phone:'+'}}}})).body.count;console.log('  total',t.toLocaleString(),'| real-phone',p.toLocaleString());})()"; }

echo "=== MODEL-RELOAD BEFORE $(date) ==="; phones
for p in bf-cc-warc-bio-repo bf-cc-warc-mmiss-all bf-cc-warc-tld-expand backfill-mon bf-cc-warc-full bf-cc-warc-people-company; do
  echo "=== reload+model cc-extracted/$p/ $(date) ==="
  node load-extracted.js "cc-extracted/$p/" --model 2>&1 | tee "logs/model-reload-$p.log" || { echo "$p FAILED"; continue; }
done
echo "=== MODEL-RELOAD DONE $(date) ==="; phones
