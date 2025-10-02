#!/bin/bash
set -euo pipefail
LOW=25
HIGH=10000

python logan_hash_tools.py percentiles \
  --working-duckdb /scratch/dmk333_new/unclassified_frequent_Logan_metagenome_hashes/data/logan_work.duckdb \
  --out-dir /scratch/dmk333_new/unclassified_frequent_Logan_metagenome_hashes/data/hash_and_sample_id_counts/hash_counts_${LOW}_${HIGH} \
  --counts-source hash \
  --low-cut $LOW \
  --high-cut $HIGH \
  --counts-path /scratch/dmk333_new/unclassified_frequent_Logan_metagenome_hashes/data/hash_and_sample_id_counts/hash_counts.parquet \
  --emit-mapping-for-hashes \
  --map-out /scratch/dmk333_new/unclassified_frequent_Logan_metagenome_hashes/data/hash_and_sample_id_counts/hash_counts_${LOW}_${HIGH}/hash_pct_${LOW}_${HIGH}_lists \
  --attach-db /scratch/shared_data_new/Logan_yacht_data/processed_data/database_all.db \
  --db-schema sigs_dna --db-table signature_mins --attach-alias ro \
  --ksize 31 \
  --pairs-as-lists \
  --pairs-file-prefix pairs_lists_shard_ \
  --pairs-shards 128 \
  --pairs-row-group-size 5000000 \
  --pairs-sort-lists \
  --threads 256 --memory-limit "3500GB" --temp-dir /scratch/duck_tmp \
  --overwrite
