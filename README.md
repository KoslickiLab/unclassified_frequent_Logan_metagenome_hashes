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
