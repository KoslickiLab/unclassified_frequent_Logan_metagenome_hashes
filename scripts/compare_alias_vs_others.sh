#!/bin/bash
/usr/bin/time -v python compare_alias_vs_others.py \
  --manifest /scratch/dmk333_new/known_microbe_hashes/DB_info.json \
  --target-alias genbank_genomes \
  --work-db /scratch/dmk333_new/unclassified_frequent_Logan_metagenome_hashes/data/alias_vs_others.db \
  --tmp-dir /scratch/bucketed_anti_join \
  --threads 128 \
  --memory "3000GB" \
  --buckets 256
