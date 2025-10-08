#!/usr/bin/env bash
python -m logan_darkmatter.cli build-index \
  --parquet /scratch/dmk333_new/unclassified_frequent_Logan_metagenome_hashes/data/hash_and_sample_id_counts/hash_counts_25_10000/filtered_hashes_no_Serratus.parquet \
  --index-root /scratch/logan_darkmatter/novel_index \
  --shard-bits 8 \
  --parallel 16
