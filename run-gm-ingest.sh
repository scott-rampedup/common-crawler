#!/usr/bin/env bash
# Google Maps ingest phases 2-4 for the new export (gm-locations.ndjson already built by gm-load).
# gm-build: group by domain, resolve/synthesize HQ, roll up locations + contacts.
# gm-upsert: apply to OpenSearch (HQ + Location companies, contacts -> Master DB). Idempotent.
set -o pipefail
cd "c:/Users/scott/OneDrive/Desktop/Common Crawler Code/rampedup-phase1" || exit 1
export OPENSEARCH_ENDPOINT="search-contacts-prod-3wdhiegssjafz4v3hmtdbkgmmu.us-east-1.es.amazonaws.com"
WORK="/c/Users/scott/gm-work"

echo "=== Phase 2/3/5 gm-build $(date) ==="
node gm-build.js --in "$WORK/gm-locations.ndjson" --out "$WORK" 2>&1 | tee logs/gm-build.log || { echo "BUILD-FAILED"; exit 1; }
echo "  outputs:"; ls -lh "$WORK"/gm-hq.ndjson "$WORK"/gm-loc.ndjson "$WORK"/gm-contacts.ndjson 2>/dev/null | awk '{print "   ",$5,$9}'

echo "=== Phase 4 gm-upsert $(date) ==="
node gm-upsert.js --in "$WORK" 2>&1 | tee logs/gm-upsert.log || { echo "UPSERT-FAILED"; exit 1; }

echo "=== GM INGEST DONE $(date) ==="
