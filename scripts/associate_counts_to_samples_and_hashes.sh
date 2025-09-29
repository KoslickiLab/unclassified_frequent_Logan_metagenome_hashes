#!/bin/bash
set -eou pipefail

python logan_hash_tools.py prepare-hashes \
  --hash-parquet-root /scratch/dmk333_new/unclassified_frequent_Logan_metagenome_hashes/data/anti_join_parquets/logan_metagenomes_minus_rights_k31_20250925_141847 \
  --hash-glob "bucket=*/data_*.parquet" \
  --working-duckdb /scratch/dmk333_new/unclassified_frequent_Logan_metagenome_hashes/data/logan_work.duckdb \
  --threads 128 --memory-limit "2000GB" --temp-dir /scratch/duck_tmp \
  --recreate-hashes --deduplicate-hashes --overwrite


false && python logan_hash_tools.py compute-counts \
  --working-duckdb /scratch/dmk333_new/unclassified_frequent_Logan_metagenome_hashes/data/logan_work.duckdb \
  --attach-db /scratch/shared_data_new/Logan_yacht_data/processed_data/database_all.db \
  --attach-alias ro --db-schema sigs_dna --db-table signature_mins \
  --ksize 31 \
  --out-dir /scratch/dmk333_new/unclassified_frequent_Logan_metagenome_hashes/data/hash_and_sample_id_counts \
  --threads 512 --memory-limit "3500GB" --temp-dir /scratch/duck_tmp \
  --overwrite
