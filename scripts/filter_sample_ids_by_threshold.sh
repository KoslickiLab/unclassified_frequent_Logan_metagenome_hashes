#!/usr/bin/env bash
NUMSAMPLES=1000
python filter_sample_ids_by_threshold.py \
      --hashes /scratch/dmk333_new/unclassified_frequent_Logan_metagenome_hashes/data/hash_and_sample_id_counts/hash_counts_25_10000/filtered_hashes_no_Serratus.parquet \
      --counts /scratch/dmk333_new/unclassified_frequent_Logan_metagenome_hashes/data/hash_and_sample_id_counts/sample_counts.parquet \
      --out /scratch/dmk333_new/unclassified_frequent_Logan_metagenome_hashes/data/hash_and_sample_id_counts/hash_counts_25_10000/filtered_hashes_geq${NUMSAMPLES}.parquet \
      --threshold $NUMSAMPLES \
      --drop-empty

