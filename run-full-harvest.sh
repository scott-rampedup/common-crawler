#!/usr/bin/env bash
# Full extraction of the 200k-sitemap CC-resolved pointers -> OpenSearch.
# Two files: cc-warc-mon-all.jsonl (6.79M) then cc-warc-mmiss-all.jsonl (0.77M).
# Each: lambda-drive (fan across cc-extract Lambdas -> S3) then load-extracted (S3 -> OpenSearch).
set -o pipefail
cd "c:/Users/scott/OneDrive/Desktop/Common Crawler Code/rampedup-phase1" || exit 1
export OPENSEARCH_ENDPOINT="search-contacts-prod-3wdhiegssjafz4v3hmtdbkgmmu.us-east-1.es.amazonaws.com"
export AWS_REGION="us-east-1"

echo "=== [1/4] lambda-drive cc-warc-mon-all.jsonl (6.79M) $(date) ==="
RUN=s200k-mon BATCH=200 CONCURRENCY=250 node lambda-drive.js cc-warc-mon-all.jsonl 2>&1 | tee logs/harvest-mon-drive.log || { echo "MON DRIVE FAILED"; exit 1; }

echo "=== [2/4] load-extracted cc-extracted/s200k-mon/ $(date) ==="
node load-extracted.js cc-extracted/s200k-mon/ 2>&1 | tee logs/harvest-mon-load.log || { echo "MON LOAD FAILED"; exit 1; }

echo "=== [3/4] lambda-drive cc-warc-mmiss-all.jsonl (0.77M) $(date) ==="
RUN=s200k-mmiss BATCH=200 CONCURRENCY=250 node lambda-drive.js cc-warc-mmiss-all.jsonl 2>&1 | tee logs/harvest-mmiss-drive.log || { echo "MMISS DRIVE FAILED"; exit 1; }

echo "=== [4/4] load-extracted cc-extracted/s200k-mmiss/ $(date) ==="
node load-extracted.js cc-extracted/s200k-mmiss/ 2>&1 | tee logs/harvest-mmiss-load.log || { echo "MMISS LOAD FAILED"; exit 1; }

echo "=== ALL DONE $(date) ==="
