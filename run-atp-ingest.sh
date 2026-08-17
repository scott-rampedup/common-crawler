#!/usr/bin/env bash
# AllThePlaces ingest: fetch all 84 brands' GeoJSON -> Location records -> Company Crawler (company_type=Location,
# source_map=AllThePlaces) via the SAME gm-build/gm-upsert pipeline as Google Maps. Idempotent.
set -o pipefail
cd "c:/Users/scott/OneDrive/Desktop/Common Crawler Code/rampedup-phase1" || exit 1
export OPENSEARCH_ENDPOINT="search-contacts-prod-3wdhiegssjafz4v3hmtdbkgmmu.us-east-1.es.amazonaws.com"
export NODE_OPTIONS="--max-old-space-size=4096"
WORK="/c/Users/scott/atp-work"; mkdir -p "$WORK"

echo "=== Phase 1: atp-load (fetch 84 brand GeoJSONs) $(date) ==="
node atp-load.js --out "$WORK" 2>&1 | tee logs/atp-load.log || { echo "ATP-LOAD FAILED"; exit 1; }

echo "=== Phase 2/3/5: gm-build $(date) ==="
node gm-build.js --in "$WORK/atp-locations.ndjson" --out "$WORK" 2>&1 | tee logs/atp-build.log || { echo "GM-BUILD FAILED"; exit 1; }
ls -lh "$WORK"/gm-hq.ndjson "$WORK"/gm-loc.ndjson "$WORK"/gm-contacts.ndjson 2>/dev/null | awk '{print "   ",$5,$9}'

echo "=== Phase 4: gm-upsert $(date) ==="
node gm-upsert.js --in "$WORK" 2>&1 | tee logs/atp-upsert.log || { echo "GM-UPSERT FAILED"; exit 1; }

echo "=== ATP INGEST DONE $(date) ==="
