#!/bin/bash
python plot_sample_count_histogram.py -i /scratch/dmk333_new/unclassified_frequent_Logan_metagenome_hashes/data/hash_and_sample_id_counts/hash_counts.parquet -o ../data/sample_counts_histogram.png -b 200 -m 10000
