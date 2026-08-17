#!/usr/bin/env bash
# Sample WDC Oct-2024 email-bearing class subsets, extract schema:email addresses, and report
# occurrences + distinct per class + per-file density (for extrapolation to the full corpus).
# Streams each .gz (curl | gunzip | grep) so nothing large is stored — only the extracted emails.
cd "c:/Users/scott/OneDrive/Desktop/Common Crawler Code/rampedup-phase1" || exit 1
BASE="https://data.dws.informatik.uni-mannheim.de/structureddata/2024-12/quads/classspecific"
EMAILRE='<https?://schema\.org/email>'
ADDR='[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}'
rm -f wdc-emails-*.txt

# "Class:sample parts:total files"
for spec in "LocalBusiness:0 58 116 175:176" "Person:0 650 1300 1952:1953" "Organization:0 1024 2048 3071:3072"; do
  cls=${spec%%:*}; rest=${spec#*:}; parts=${rest%%:*}; total=${rest##*:}
  sampled=0
  for p in $parts; do
    echo "[$(date +%H:%M:%S)] fetch $cls/part_$p.gz"
    curl -s --max-time 900 "$BASE/$cls/part_$p.gz" | gunzip -c 2>/dev/null \
      | grep -aE "$EMAILRE" | grep -aoiE "$ADDR" | tr '[:upper:]' '[:lower:]' >> "wdc-emails-$cls.txt" || true
    sampled=$((sampled+1))
  done
  occ=$(wc -l < "wdc-emails-$cls.txt" 2>/dev/null || echo 0)
  uniq=$(sort -u "wdc-emails-$cls.txt" 2>/dev/null | wc -l)
  perfile=$(( occ / (sampled>0?sampled:1) ))
  extrap=$(( perfile * total ))
  echo "RESULT $cls | sampled $sampled/$total files | occ $occ | distinct(sample) $uniq | ~$perfile/file -> extrap occurrences ~$extrap"
done

echo "=== COMBINED (all sampled classes) ==="
cat wdc-emails-*.txt > wdc-emails-all.txt
allocc=$(wc -l < wdc-emails-all.txt)
alluniq=$(sort -u wdc-emails-all.txt | wc -l)
# generic (firm inbox) share among distinct
gen=$(sort -u wdc-emails-all.txt | grep -ciE '^(info|contact|admin|hello|office|sales|support|team|careers?|jobs?|hr|help|service|no-?reply|mail|enquir)' )
echo "sample occurrences $allocc | sample distinct $alluniq | distinct generic-prefix $gen"
echo "DONE $(date)"
