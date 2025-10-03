0. `run_better_output_format.sh` is old and shouldn't be used: it was just getting frequent hashes
1. `get_hashes_in_logan_not_in_others.sh` extracts the hashes that are in the Logan metagenomes and not any of the other reference DBs
2. `count_wgs_vs_others.sh`: I was running this just out of curiousity of what contributed the most novely in the RHS of the known reference DBs (it was GenBank WGS, and then GenBank genomes.
3. `compare_alias_vs_others.sh` is so I can do full Venn diagram counts of what's unique to each DB and what's common. GenBank WGS has 33.7M novel hashes, GenoBank genomes has 1M novel hashes.
4. `associate_counts_to_samples_and_hashes.sh` prep the databas (index) to make it faster to associate counts to samples and hashes: for each hash, how many sample IDs does it show up in, and for each sample_id, how many of the "logan only" hashes show up in them (enriched for novelty).
5. `get_frequent_hashes.sh` materialize the counts for samples and hashes for certain percentiles (frequent-ish, most frequent, etc.). Higher percentiles equate to most frequent. Can also associate actual lists of sample ids to hashes and visaversa.
