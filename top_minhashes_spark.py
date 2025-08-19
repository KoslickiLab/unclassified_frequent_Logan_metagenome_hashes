#!/usr/bin/env python3
"""
top_minhashes_spark.py

Exact, scalable computation of the top-N min_hash values by the number of distinct sample_ids
they appear in, plus the actual list of sample_ids for those top-N hashes.

Stages
------
1) counts:   Read raw Parquet, compute n_samples = countDistinct(sample_id) per min_hash,
             write as a reusable Parquet dataset partitioned by bucket.

2) topn:     Read counts dataset, find global top N, write (min_hash, n_samples).

3) ids:      Using top N min_hashes, read raw Parquet again, filter to those hashes,
             dropDuplicates(min_hash, sample_id), and write JSON with sample_id lists.

Dry-run
-------
Use --sample-fraction or --file-limit for small test runs.
All operations are read-only on input parquet files.

Requirements
------------
- Python 3.8+
- PySpark 3.3+ (recommended)

Examples
--------
# Full run (exact), computing counts then topN=100 and the sample lists:
python top_minhashes_spark.py \
  --input sigs_dna/signature_mins \
  --out /scratch/results/top_minhashes \
  --stage counts topn ids \
  --topN 100 \
  --num-buckets 512 \
  --local-cores 128 \
  --driver-memory 256g \
  --executor-memory 3000g \
  --shuffle-partitions 8192

# Dry-run on 1% of rows to gauge resources:
python top_minhashes_spark.py \
  --input sigs_dna/signature_mins \
  --out /scratch/results/top_minhashes_dryrun \
  --stage counts topn ids \
  --topN 20 \
  --sample-fraction 0.01 \
  --num-buckets 64 \
  --local-cores 64 \
  --driver-memory 32g \
  --executor-memory 256g \
  --shuffle-partitions 1024

# Re-use counts with a different N, skipping the heavy counts pass:
python top_minhashes_spark.py \
  --input sigs_dna/signature_mins \
  --out /scratch/results/top_minhashes \
  --stage topn ids \
  --topN 500 \
  --num-buckets 512 \
  --local-cores 128 \
  --driver-memory 64g \
  --executor-memory 512g \
  --shuffle-partitions 4096
"""

import argparse
import os
import sys
from typing import List

from pyspark.sql import SparkSession, DataFrame, functions as F, types as T


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Find top-N min_hashes by distinct sample_id count and list their sample_ids.")

    p.add_argument("--input", required=True,
                   help="Input Parquet directory with columns including min_hash (int64), sample_id (string).")
    p.add_argument("--out", required=True,
                   help="Output directory. Script creates subdirs: counts/, topn/, topn_ids_json/ etc.")
    p.add_argument("--stage", nargs="+", default=["counts", "topn", "ids"],
                   choices=["counts", "topn", "ids"],
                   help="Stages to run. Use 'counts' once, then later reuse with 'topn ids'.")
    p.add_argument("--topN", type=int, default=100,
                   help="Number of top min_hashes to output with sample lists (used in topn and ids stages).")
    p.add_argument("--num-buckets", type=int, default=512,
                   help="How many buckets to partition counts by (pmod(min_hash, num_buckets)). Default 512.")
    p.add_argument("--ksize", type=int, default=None,
                   help="Optional filter on ksize column if present (e.g., 31). If omitted, use all ksizes.")
    p.add_argument("--sample-fraction", type=float, default=None,
                   help="Optional row-level sampling fraction in (0,1] for dry-run sanity checks.")
    p.add_argument("--file-limit", type=int, default=None,
                   help="Optional: limit number of input Parquet files scanned (dry-run).")
    p.add_argument("--seed", type=int, default=1337, help="Random seed for sampling.")
    p.add_argument("--local-cores", type=int, default=None,
                   help="If set, run Spark in local mode with this many cores, e.g., 128.")
    p.add_argument("--driver-memory", default=None,
                   help="Spark driver memory, e.g., '256g'.")
    p.add_argument("--executor-memory", default=None,
                   help="Spark executor memory, e.g., '3000g'. In local mode, applies to the single executor.")
    p.add_argument("--shuffle-partitions", type=int, default=4096,
                   help="spark.sql.shuffle.partitions (default 4096). Increase for large shuffles.")
    p.add_argument("--max-partition-bytes", default="512m",
                   help="spark.sql.files.maxPartitionBytes (default 512m).")
    p.add_argument("--compress-json", action="store_true",
                   help="Write gzip-compressed NDJSON for the top-N sample lists.")
    p.add_argument("--coalesce-json", type=int, default=1,
                   help="Coalesce partitions when writing top-N JSON (default 1 => one file).")
    p.add_argument("--skip-distinct-pairs-in-ids", action="store_true",
                   help="(Not recommended) Skip distinct on (min_hash, sample_id) when building sample lists.")
    return p.parse_args()


def build_spark(args: argparse.Namespace) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("TopMinhashesDistinctSamples")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.files.maxPartitionBytes", args.max_partition_bytes)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.driver.maxResultSize", "0")  # unlimited (be careful)
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.sql.parquet.mergeSchema", "false")
        .config("spark.sql.parquet.enableVectorizedReader", "true")
    )

    if args.local_cores:
        builder = builder.master(f"local[{args.local_cores}]")

    if args.driver_memory:
        builder = builder.config("spark.driver.memory", args.driver_memory)
    if args.executor_memory:
        builder = builder.config("spark.executor.memory", args.executor_memory)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def list_some_files(spark: SparkSession, input_path: str, limit: int) -> List[str]:
    """
    Return up to 'limit' leaf Parquet files under input_path.
    """
    # Use Spark to list files (works with HDFS/object stores too).
    # Alternatively, python os.walk for local FS.
    files_df = spark.read.format("parquet").load(input_path).inputFiles()
    files = list(files_df)  # inputFiles returns a list already
    if limit is not None and limit > 0:
        return files[:limit]
    return files


def read_raw_dataset(spark: SparkSession, args: argparse.Namespace) -> DataFrame:
    """
    Read the raw Parquet dataset and produce a DataFrame with at least:
    - min_hash: long
    - sample_id: string

    Applies optional filters (ksize), sampling, and file limiting.
    """
    if args.file_limit:
        files = list_some_files(spark, args.input, args.file_limit)
        if not files:
            raise RuntimeError("No files found under input path.")
        df = spark.read.parquet(*files)
    else:
        df = spark.read.parquet(args.input)

    # Ensure required columns exist and cast types
    cols = df.columns
    if "min_hash" not in cols or "sample_id" not in cols:
        raise RuntimeError("Input parquet must contain columns: min_hash (int64), sample_id (string).")

    df = df.select(
        F.col("min_hash").cast(T.LongType()).alias("min_hash"),
        F.col("sample_id").cast(T.StringType()).alias("sample_id"),
        *(F.col("ksize") if "ksize" in cols else [])
    )

    if args.ksize is not None and "ksize" in df.columns:
        df = df.filter(F.col("ksize") == F.lit(args.ksize))

    # Drop nulls defensively
    df = df.dropna(subset=["min_hash", "sample_id"])

    # Optional sampling (row-level)
    if args.sample_fraction is not None:
        if not (0 < args.sample_fraction <= 1.0):
            raise ValueError("--sample-fraction must be in (0, 1].")
        df = df.sample(withReplacement=False, fraction=args.sample_fraction, seed=args.seed)

    return df


def stage_counts(spark: SparkSession, args: argparse.Namespace, df_raw: DataFrame) -> None:
    """
    Compute exact distinct counts per min_hash and write partitioned Parquet.
    """
    out_counts = os.path.join(args.out, "counts_parquet")
    bucket_col = F.pmod(F.col("min_hash"), F.lit(args.num_buckets)).cast(T.IntegerType()).alias("bucket")

    counts_df = (
        df_raw
        .select("min_hash", "sample_id")
        .groupBy("min_hash")
        .agg(F.countDistinct("sample_id").alias("n_samples"))
        .withColumn("bucket", bucket_col)
    )

    # Repartition by bucket to balance output files
    counts_df = counts_df.repartition(args.num_buckets, "bucket")

    (
        counts_df
        .write
        .mode("overwrite")
        .partitionBy("bucket")
        .parquet(out_counts)
    )
    print(f"[counts] wrote: {out_counts}")


def read_counts(spark: SparkSession, args: argparse.Namespace) -> DataFrame:
    path = os.path.join(args.out, "counts_parquet")
    if not os.path.exists(path) and not path.startswith("hdfs://") and not path.startswith("s3://"):
        raise RuntimeError(f"Counts parquet not found at {path}. Did you run the 'counts' stage?")
    return spark.read.parquet(path).select("min_hash", "n_samples")


def stage_topn(spark: SparkSession, args: argparse.Namespace) -> DataFrame:
    """
    Read counts parquet, compute top-N, write both Parquet + JSON summaries, and return topN DataFrame.
    """
    out_topn_parquet = os.path.join(args.out, "topn_parquet")
    out_topn_json = os.path.join(args.out, "topn_json")

    counts_df = read_counts(spark, args)

    topn_df = counts_df.orderBy(F.col("n_samples").desc(), F.col("min_hash").asc()).limit(args.topN)

    # Persist both Parquet (for re-use) and a small JSON summary
    topn_df.write.mode("overwrite").parquet(out_topn_parquet)

    writer = topn_df.coalesce(1).write.mode("overwrite").json(out_topn_json)
    print(f"[topn] wrote: {out_topn_parquet} and {out_topn_json}")

    return topn_df


def stage_ids(spark: SparkSession, args: argparse.Namespace, df_raw: DataFrame, topn_df: DataFrame) -> None:
    """
    For the top-N min_hashes, produce the list of distinct sample_ids per hash and write NDJSON.
    """
    out_ids = os.path.join(args.out, "topn_ids_json")
    if args.compress_json:
        out_ids = out_ids + "_gz"

    # Broadcast top-N for efficient filtering
    topn_keys = topn_df.select("min_hash")
    filtered = df_raw.join(F.broadcast(topn_keys), on="min_hash", how="inner").select("min_hash", "sample_id")

    if not args.skip_distinct_pairs_in_ids:
        filtered = filtered.dropDuplicates(["min_hash", "sample_id"])

    # Aggregate sample_id lists per min_hash; keep n_samples as a sanity check
    agg_df = (
        filtered
        .groupBy("min_hash")
        .agg(
            F.collect_set("sample_id").alias("sample_ids"),
            F.count(F.lit(1)).alias("n_samples_exact")  # after distinct pairs, this equals len(sample_ids)
        )
    )

    # Join with precomputed counts to include n_samples (exact) and to sort as before
    counts_df = read_counts(spark, args)
    result_df = (
        agg_df.join(counts_df, on="min_hash", how="left")
              .select("min_hash", "n_samples", "sample_ids")
              .orderBy(F.col("n_samples").desc(), F.col("min_hash").asc())
    )

    # Coalesce and write NDJSON (optionally gzipped)
    result_df = result_df.coalesce(args.coalesce_json)
    writer = result_df.write.mode("overwrite")
    if args.compress_json:
        writer = writer.option("compression", "gzip")

    writer.json(out_ids)
    print(f"[ids] wrote: {out_ids}")


def main():
    args = parse_args()
    spark = build_spark(args)

    # Eagerly check output base dir
    if not (args.out.startswith("hdfs://") or args.out.startswith("s3://")):
        os.makedirs(args.out, exist_ok=True)

    df_raw = None
    if "counts" in args.stage or "ids" in args.stage:
        # Only read raw data when needed
        df_raw = read_raw_dataset(spark, args)

    if "counts" in args.stage:
        stage_counts(spark, args, df_raw)

    topn_df = None
    if "topn" in args.stage:
        topn_df = stage_topn(spark, args)

    if "ids" in args.stage:
        if topn_df is None:
            # If user skipped 'topn' stage in this run, read it back
            topn_df = spark.read.parquet(os.path.join(args.out, "topn_parquet")).select("min_hash", "n_samples")
        stage_ids(spark, args, df_raw, topn_df)

    spark.stop()


if __name__ == "__main__":
    sys.exit(main())
