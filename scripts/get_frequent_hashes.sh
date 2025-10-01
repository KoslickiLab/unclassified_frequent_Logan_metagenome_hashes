#!/bin/bash
set -eou pipefail
TOPP=0.90
BOTTOMP=0.75
python logan_hash_tools.py percentiles \
  --working-duckdb /scratch/dmk333_new/unclassified_frequent_Logan_metagenome_hashes/data/logan_work.duckdb \
  --out-dir /scratch/dmk333_new/unclassified_frequent_Logan_metagenome_hashes/data/hash_and_sample_id_counts/hash_counts_${BOTTOMP}_${TOPP} \
  --counts-source hash \
  --percentiles $BOTTOMP $TOPP \
  --counts-path /scratch/dmk333_new/unclassified_frequent_Logan_metagenome_hashes/data/hash_and_sample_id_counts/hash_counts.parquet \
  --emit-mapping-for-hashes \
  --map-out /scratch/dmk333_new/unclassified_frequent_Logan_metagenome_hashes/data/hash_and_sample_id_counts/hash_counts_${BOTTOMP}_${TOPP}/hash_pct_${BOTTOMP}_${TOPP}_pairs_2 \
  --attach-db /scratch/shared_data_new/Logan_yacht_data/processed_data/database_all.db \
  --db-schema sigs_dna --db-table signature_mins --attach-alias ro \
  --ksize 31 \
  --threads 256 --memory-limit "3500GB" --temp-dir /scratch/duck_tmp \
  --overwrite
