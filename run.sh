export JAVA_HOME="$CONDA_PREFIX"           
export PATH="$JAVA_HOME/bin:$PATH"
export SPARK_LOCAL_DIRS="/scratch/spark_local"
#/usr/bin/time -v python top_minhashes_spark.py --input signature_mins/ --out full_output --stage counts --topN 100000000 --ksize 31 --local-cores 64 --driver-memory 500g --executor-memory 3000g --shuffle-partitions 32768 --max-partition-bytes 64m --num-buckets 8192 > counts.log 2>&1

# Worked, but was slow
#/usr/bin/time -v python top_minhashes_spark.py \
#  --input signature_mins/ \
#  --out full_output \
#  --stage counts \
#  --ksize 31 \
#  --num-buckets 8192 \
#  --local-cores 128 \
#  --driver-memory 3000g \
#  --executor-memory 1000g \
#  --max-partition-bytes 1g \
#  --auto-shuffle \
#  --target-shuffle-blocks 50000000 \
#  --local-dirs "/scratch/spark_local" > counts.log 2>&1

# new attempt at "dicy" commit
python top_minhashes_spark.py \
  --input signature_mins \
  --out /scratch/full_output \
  --stage counts \
  --ksize 31 > counts.log 2>&1


#/usr/bin/time -v python top_minhashes_spark.py --input signature_mins/ --out full_output --stage topn --topN 100000000 --ksize 31 --local-cores 64 --driver-memory 500g --executor-memory 3000g --shuffle-partitions 32768 --max-partition-bytes 64m --coalesce-json 512 > topn.log 2>&1

# Note that this remoes the step ids from the --stage flag, as I am told I should only do this if necessary
