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