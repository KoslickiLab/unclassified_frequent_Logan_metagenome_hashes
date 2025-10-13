# Frequently appearing, unclassified Logan metagenome hashes

Requires `sigs_dna/signature_mins/` on ICDS as:
```
/storage/home/dmk333/group_storage/staging_dir_all/sigs_dna/
```
and on the GPU server as:
```
/scratch/dmk333_new/unclassified_frequent_Logan_metagenome_hashes/
```

Also requires `/scratch/wgs_logan_diff/parquet/minhash31_db1/` which can be generated with:
```
analysis/diff_bet_wgs_and_logan.py
```
from [this repository](https://github.com/KoslickiLab/GenBank_WGS_analysis)

# Where things live
The unique hashes for each of the databases lives in the [known_microbe_hashes](https://github.com/KoslickiLab/known_microbe_hashes) repo.

The unique hashes for the Logan metagenomes was created using [python diff_bet_wgs_and_logan.py](https://github.com/KoslickiLab/GenBank_WGS_analysis/blob/main/analysis/diff_bet_wgs_and_logan.py). In the midst of that script, we dumped the hashes into buckets. These buckets are located at `/scratch/shared_data_new/Logan_yacht_data/processed_data/unique_hashes/minhash31_db1` (and in the `/scratch/wgs_logan_diff/parquet` original location) and are naturally deduplicated.

# How things work

First, I used `run_better_output_format.sh` which calls `top_minhashes_spark.py` which will get the hashes that are 
in a lot of the samples. After doing this, I realized that lots of these were from things like conserved regions of 
16S rRNA genes. In any case, I stuck them all in a duckdb database using `create_duck_db_from_hive_partition.sh`.

So then I went and created the [Known_microbe_hashes](https://github.com/KoslickiLab/known_microbe_hashes) repo which has the unique hashes for all the microbial databases I could get my hands on. 

Then, I removed all the known hashes using `get_hashes_in_db_not_in_others.sh`.

I also added a generic left/right anti-join to get statistics about which reference database had the most hashes 
unique to it; see `count_wgs_vs_others.sh` and `compare_alias_vs_others.sh`.

I then wanted to decorate the novel Logan hashes with which samples they are in, as well as which samples contain 
the most novel hashes. I used `associate_counts_to_samples_and_hashes.sh` to do this. This prepares the database for 
the next step.

Next, I plotted the histogram of some of this data with `plot_sample_count_histogram.py`. I quickly learned 
(`calculate_percentiles.py`) that 
taking a percentile approach (eg. "give me the hashes that occur between the 75th and 90th percentile of sample 
counts"; see `get_frequent_hashes.sh`) was not a good idea, as the distribution is very long-tailed. So I went with 
`get_frequent_hashes_cuttoff.sh` which takes a hard cutoff (eg. "give me the hashes that occur in at least 100 
samples but no more than 1,000,000"). Note that I also did it the other way around: Find me samples that have lots 
of these novel hashes.

I then had a realization that I did not include the Serratus expansion of the Viral population genomes. So I got those 
unique hashes in the [Known_microbe_hashes](https://github.com/KoslickiLab/known_microbe_hashes). I then needed to 
remove these hashes from the novel logan hashes between the cuttoff. This was accomplished with 
`remove_Serratus_hashes_from_frequent_novel_hash_parquets.py` which can be run with its default parameters.

This left me with my target set of novel, frequently appearing, unclassified Logan metagenome hashes, currently 
located at: 
```
/scratch/dmk333_new/unclassified_frequent_Logan_metagenome_hashes/data/hash_and_sample_id_counts/hash_counts_25_10000/filtered_hashes_no_Serratus.parquet
```

Next, we can look at samples which are enriched for these hashes, pull down their contigs and decorate 
them with how many of these novel hashes they contain. This is accomplished with the preparation script 
`logan_dark_matter_extractor/scripts/0_build_index.sh` and then actually running it with 
`logan_dark_matter_extractor/scripts/1_annotate_contigs_from_accession.sh`.

Next, I discovered that I still have way too many candidates to sift through, so the plan is:

1. For the frequently appearing, novel hashes, only count/include those samples that themselves have lots of novelty
2. Consider including only WGS samples
3. Consider only including those that have persisted over time

For 1. above, I will use `filter_by_sample_count.sh` to start with the file `filtered_hashes_no_Serratus.parquet`
(which contains the hashes that show up in many (>=25) samples but not too many (<=10,000 samples) and then filter the 
samples to only those that have lots of novelty (currently set to be "has 1000 or more novel hashes in that sample").
This will give me a new set of hashes that are more enriched for novelty and are in samples that are enriched for 
novelty.
