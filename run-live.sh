#!/bin/sh
# Remote live-crawl sample runner (executed on the Fly machine, where the NetNut proxy is reachable).
# $1 = URL list file. Uses the app's env (PROXY_URL / PROXY_FALLBACK_URL / UNBLOCKER_API_URL / OPENSEARCH_ENDPOINT).
cd /app || exit 1
echo "START $(date) file=$1 lines=$(wc -l < "$1") LIVE_CONC=${LIVE_CONC:-20}"
LIVE_CONC=${LIVE_CONC:-20} node extract-from-pointers.js --live "$1" --tag live-sample
echo "END $(date) rc=$?"
