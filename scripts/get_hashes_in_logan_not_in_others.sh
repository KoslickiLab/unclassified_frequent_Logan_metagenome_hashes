#!/bin/bash
/usr/bin/time -v python get_hashes_in_db_not_in_others.py --manifest /scratch/dmk333_new/known_microbe_hashes/DB_info.json \
	--work-db /scratch/dmk333_new/unclassified_frequent_Logan_metagenome_hashes/data/bucketed_anti_join.db \
	--tmp-dir /scratch/bucketed_anti_join \
	--threads 128 \
	--memory "3000GB" \
	--buckets 256 \
	--out-dir /scratch/dmk333_new/unclassified_frequent_Logan_metagenome_hashes/data/anti_join_parquets > get_hashes_in_logan_not_in_others.log 2>&1
