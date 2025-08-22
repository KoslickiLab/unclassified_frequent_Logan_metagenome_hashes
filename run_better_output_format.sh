#!/bin/bash
export JAVA_HOME="$CONDA_PREFIX"
export PATH="$JAVA_HOME/bin:$PATH"
export SPARK_LOCAL_DIRS="/scratch/spark_local"

# finished
#python top_minhashes_spark.py \
	#--input signature_mins \
	#--out full_output \
	#--stage counts \
	#--topN 100000000 \
	#--num-buckets 512 \
	#--ksize 31 \
	#--local-cores 128 \
	#--driver-memory 1500g \
	#--executor-memory 1500g \
	#--shuffle-partitions 4096 > counts.log 2>&1

# find IDs
/usr/bin/time -v python top_minhashes_spark.py\
	--input signature_mins \
	--out full_output \
	--stage topn \
	--topN 100000000 \
	--num-buckets 512 \
        --ksize 31 \
        --local-cores 128 \
        --driver-memory 2500g \
        --executor-memory 1500g \
	--compress-json \
	--coalesce-json 256 \
        --shuffle-partitions 4096 > topN.log 2>&1
