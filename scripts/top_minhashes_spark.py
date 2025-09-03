#!/usr/bin/env python3
"""
top_minhashes_spark.py

Exact, scalable computation of the top-N min_hash values by the number of distinct sample_ids
they appear in, plus the actual list of sample_ids for those top-N hashes.

Key features in this version:
- Versioned outputs (no stomping): results for each run go into .../run=<run-id>/ subdirs.
- Clean file names: rename Spark's part-*.json(.gz) to readable names with topN/ksize/run-id.
- Cleanup: remove _SUCCESS and extra part files by default.

Usage examples are at the end of this file’s docstring.
"""

import argparse
import os
import sys
import glob
from datetime import datetime, timezone
from typing import List, Optional

from pyspark.sql import SparkSession, DataFrame, functions as F, types as T


# -----------------------
# CLI
# -----------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Find top-N min_hashes by distinct sample_id count and list their sample_ids.")

    p.add_argument("--input", required=True,
                   help="Input Parquet directory with columns including min_hash (int64), sample_id (string).")
    p.add_argument("--out", required=True,
                   help="Output base directory. Script creates subdirs under this path.")
    p.add_argument("--stage", nargs="+", default=["counts", "topn", "ids"],
                   choices=["counts", "topn", "ids"],
                   help="Stages to run. Use 'counts' once, then later reuse with 'topn ids'.")
    p.add_argument("--topN", type=int, default=100,
                   help="Number of top min_hashes to output with sample lists.")
    p.add_argument("--num-buckets", type=int, default=512,
                   help="How many buckets to partition counts by (pmod(min_hash, num_buckets)). Default 512.")
    p.add_argument("--ksize", type=int, default=None,
                   help="Optional filter on ksize column if present (e.g., 31). If omitted, use all ksizes.")
    p.add_argument("--sample-fraction", type=float, default=None,
                   help="Optional row-level sampling fraction in (0,1] for dry-run sanity checks.")
    p.add_argument("--file-limit", type=int, default=None,
                   help="Optional: limit number of input Parquet files scanned (dry-run).")

    # Spark resources / tuning
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

    # Output formatting / housekeeping
    p.add_argument("--compress-json", action="store_true",
                   help="Write gzip-compressed NDJSON for the top-N sample lists.")
    p.add_argument("--coalesce-json", type=int, default=1,
                   help="Coalesce partitions when writing top-N JSON (default 1 => one file).")
    p.add_argument("--run-tag", default=None,
                   help="Optional label for this run; if not set, uses UTC timestamp like 20250819T160455Z.")
    p.add_argument("--keep-success", action="store_true",
                   help="Keep Hadoop _SUCCESS marker files. Default: delete them.")
    p.add_argument("--no-cleanup", action="store_true",
                   help="Do not delete extra part files; leave Spark output as-is.")
    p.add_argument("--overwrite-existing", action="store_true",
                   help="Allow overwriting a run directory if it already exists (default: error if exists).")

    # Advanced (not generally needed)
    p.add_argument("--skip-distinct-pairs-in-ids", action="store_true",
                   help="(Not recommended) Skip distinct on (min_hash, sample_id) when building sample lists.")
    return p.parse_args()


def utc_run_id(custom: Optional[str]) -> str:
    if custom:
        return custom
    # Example: 20250819T160455Z
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


# -----------------------
# Spark session
# -----------------------
def build_spark(args: argparse.Namespace) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("TopMinhashesDistinctSamples")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.files.maxPartitionBytes", args.max_partition_bytes)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.driver.maxResultSize", "0")
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.sql.parquet.mergeSchema", "false")
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        # Avoid writing _SUCCESS in many cases (still delete it explicitly later to be safe)
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
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


# -----------------------
# I/O helpers (general FS via Hadoop)
# -----------------------
def _jvm(spark: SparkSession):
    return spark._jvm


def get_hadoop_fs(spark: SparkSession, path: str):
    jvm = _jvm(spark)
    hconf = spark._jsc.hadoopConfiguration()
    uri = jvm.java.net.URI.create(path)
    return jvm.org.apache.hadoop.fs.FileSystem.get(uri, hconf)


def hpath(spark: SparkSession, path: str):
    return _jvm(spark).org.apache.hadoop.fs.Path(path)


def path_exists(spark: SparkSession, path: str) -> bool:
    fs = get_hadoop_fs(spark, path)
    return fs.exists(hpath(spark, path))


def ensure_dir_absent_or_writable(spark: SparkSession, path: str, overwrite: bool):
    exists = path_exists(spark, path)
    if exists and not overwrite:
        raise FileExistsError(f"Output path exists (won't overwrite): {path}")


def list_status(spark: SparkSession, dir_path: str):
    fs = get_hadoop_fs(spark, dir_path)
    return list(fs.listStatus(hpath(spark, dir_path)))


def rename_spark_single_part(
    spark: SparkSession,
    out_dir: str,
    final_filename_base: str,
    compressed: bool,
    keep_success: bool,
    cleanup_extra: bool
) -> str:
    """
    Find the single part-*.json(.gz) in out_dir, rename it to final_filename_base + ext,
    optionally delete _SUCCESS and any other aux files. Returns final file path.
    """
    statuses = list_status(spark, out_dir)
    part_path = None
    part_name = None
    ext_json = ".json.gz" if compressed else ".json"

    # Find the part-*.json or .json.gz
    for st in statuses:
        name = st.getPath().getName()
        if name.startswith("part-") and (name.endswith(".json") or name.endswith(".json.gz")):
            part_path = st.getPath()
            part_name = name
            break

    if part_path is None:
        raise RuntimeError(f"No part-*.json(.gz) found under {out_dir}. "
                           f"Did you set --coalesce-json 1?")

    final_name = final_filename_base + ext_json
    final_path = hpath(spark, os.path.join(out_dir, final_name))

    fs = get_hadoop_fs(spark, out_dir)
    if not fs.rename(part_path, final_path):
        raise RuntimeError(f"Failed to rename {part_name} to {final_name} in {out_dir}")

    # Cleanup: remove _SUCCESS and other stray files (unless requested to keep them)
    if cleanup_extra:
        for st in list_status(spark, out_dir):
            name = st.getPath().getName()
            if name == final_name:
                continue
            if (not keep_success and name == "_SUCCESS") or \
               name.startswith("part-") or \
               name.endswith(".crc") or \
               name.startswith("."):
                fs.delete(st.getPath(), False)

    return final_path.toString()


# -----------------------
# Read input
# -----------------------
def read_raw_dataset(spark: SparkSession, args: argparse.Namespace) -> DataFrame:
    """
    Read the raw Parquet dataset and produce a DataFrame with:
      min_hash: long
      sample_id: string
      (optional) ksize: int
    Apply ksize filter, sampling, and optional file limiting.
    """
    if args.file_limit:
        paths = sorted(glob.glob(os.path.join(args.input, "**/*.parquet"), recursive=True))[:args.file_limit]
        if not paths:
            raise RuntimeError("No files found under input path.")
        df = spark.read.parquet(*paths)
    else:
        df = spark.read.parquet(args.input)

    cols = df.columns
    if "min_hash" not in cols or "sample_id" not in cols:
        raise RuntimeError("Input parquet must contain: min_hash (int64), sample_id (string).")

    extra = [F.col("ksize")] if "ksize" in cols else []
    df = df.select(
        F.col("min_hash").cast(T.LongType()).alias("min_hash"),
        F.col("sample_id").cast(T.StringType()).alias("sample_id"),
        *extra
    )

    if args.ksize is not None and "ksize" in df.columns:
        df = df.filter(F.col("ksize") == F.lit(args.ksize))

    df = df.dropna(subset=["min_hash", "sample_id"])

    if args.sample_fraction is not None:
        if not (0 < args.sample_fraction <= 1.0):
            raise ValueError("--sample-fraction must be in (0, 1].")
        df = df.sample(False, args.sample_fraction, seed=args.seed)

    return df


# -----------------------
# Stages
# -----------------------
def stage_counts(spark: SparkSession, args: argparse.Namespace, df_raw: DataFrame) -> None:
    """
    Compute exact distinct counts per min_hash and write partitioned Parquet under:
      <out>/counts_parquet/
    (We keep counts as a reusable dataset; overwriting is okay because counts are an intermediate.)
    """
    out_counts = os.path.join(args.out, "counts_parquet")

    # Deduplicate (min_hash, sample_id) BEFORE counting to reduce shuffle volume
    pairs = df_raw.select("min_hash", "sample_id").dropDuplicates(["min_hash", "sample_id"])

    counts_df = (
        pairs
        .groupBy("min_hash")
        .agg(F.count(F.lit(1)).alias("n_samples"))
        .withColumn("bucket", F.pmod(F.col("min_hash"), F.lit(args.num_buckets)).cast(T.IntegerType()))
    )

    counts_df = counts_df.repartition(args.num_buckets, "bucket")

    # Overwrite counts (intermediate). If you prefer versioned counts, we can also add run-id here.
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
    if not (path.startswith("hdfs://") or path.startswith("s3://")) and not os.path.exists(path):
        raise RuntimeError(f"Counts parquet not found at {path}. Did you run the 'counts' stage?")
    return spark.read.parquet(path).select("min_hash", "n_samples")


def stage_topn(spark: SparkSession, args: argparse.Namespace, run_id: str) -> DataFrame:
    """
    Read counts parquet, compute top-N, write both Parquet + JSON summaries under versioned paths, return topN DataFrame.
    """
    counts_df = read_counts(spark, args)
    topn_df = counts_df.orderBy(F.col("n_samples").desc(), F.col("min_hash").asc()).limit(args.topN)

    # Versioned output paths
    sub_topn_parquet = os.path.join(args.out, f"topn_parquet/topN={args.topN}/run={run_id}")
    sub_topn_json = os.path.join(args.out, f"topn_json/topN={args.topN}/run={run_id}")

    ensure_dir_absent_or_writable(spark, sub_topn_parquet, overwrite=args.overwrite_existing)
    ensure_dir_absent_or_writable(spark, sub_topn_json, overwrite=args.overwrite_existing)

    # Write parquet (structured, for re-use)
    topn_df.write.mode("overwrite" if args.overwrite_existing else "errorifexists").parquet(sub_topn_parquet)

    # Write JSON (human-facing summary)
    topn_df.coalesce(1).write.mode("overwrite" if args.overwrite_existing else "errorifexists").json(sub_topn_json)

    # Rename the single part file to a nice name (and clean up)
    base_name = f"topn.summary.topN={args.topN}"
    if args.ksize is not None:
        base_name += f".ksize={args.ksize}"
    base_name += f".run={run_id}"

    final_path = rename_spark_single_part(
        spark=spark,
        out_dir=sub_topn_json,
        final_filename_base=base_name,
        compressed=False,
        keep_success=args.keep_success,
        cleanup_extra=(not args.no_cleanup),
    )

    print(f"[topn] wrote parquet: {sub_topn_parquet}")
    print(f"[topn] wrote json:    {final_path}")

    return topn_df


def stage_ids(spark: SparkSession, args: argparse.Namespace, df_raw: DataFrame, topn_df: DataFrame, run_id: str) -> None:
    """
    For the top-N min_hashes, produce the list of distinct sample_ids per hash and write NDJSON
    under a versioned directory with a clean filename.
    """
    # Filter raw to top-N keys
    topn_keys = topn_df.select("min_hash")
    filtered = df_raw.join(F.broadcast(topn_keys), on="min_hash", how="inner").select("min_hash", "sample_id")

    if not args.skip_distinct_pairs_in_ids:
        filtered = filtered.dropDuplicates(["min_hash", "sample_id"])

    agg_df = (
        filtered
        .groupBy("min_hash")
        .agg(
            F.sort_array(F.collect_set("sample_id")).alias("sample_ids"),
            F.count(F.lit(1)).alias("n_samples_exact")
        )
    )

    counts_df = read_counts(spark, args)
    result_df = (
        agg_df.join(counts_df, on="min_hash", how="left")
              .select("min_hash", "n_samples", "sample_ids")
              .orderBy(F.col("n_samples").desc(), F.col("min_hash").asc())
    )

    # Versioned output path
    sub_ids_json = os.path.join(args.out, f"topn_ids_json/topN={args.topN}/run={run_id}")
    ensure_dir_absent_or_writable(spark, sub_ids_json, overwrite=args.overwrite_existing)

    writer = result_df.coalesce(args.coalesce_json).write.mode("overwrite" if args.overwrite_existing else "errorifexists")
    if args.compress_json:
        writer = writer.option("compression", "gzip")
    writer.json(sub_ids_json)

    # Rename to a clean filename and clean up aux files
    base_name = f"topn_ids.topN={args.topN}"
    if args.ksize is not None:
        base_name += f".ksize={args.ksize}"
    base_name += f".run={run_id}"

    final_path = rename_spark_single_part(
        spark=spark,
        out_dir=sub_ids_json,
        final_filename_base=base_name,
        compressed=args.compress_json,
        keep_success=args.keep_success,
        cleanup_extra=(not args.no_cleanup),
    )

    print(f"[ids] wrote json: {final_path}")


# -----------------------
# Main
# -----------------------
def main():
    args = parse_args()
    spark = build_spark(args)

    # Base out dir (local FS path gets created so OS tools can see it immediately)
    if not (args.out.startswith("hdfs://") or args.out.startswith("s3://")):
        os.makedirs(args.out, exist_ok=True)

    run_id = utc_run_id(args.run_tag)

    df_raw = None
    if "counts" in args.stage or "ids" in args.stage:
        df_raw = read_raw_dataset(spark, args)

    if "counts" in args.stage:
        stage_counts(spark, args, df_raw)

    topn_df = None
    if "topn" in args.stage:
        topn_df = stage_topn(spark, args, run_id=run_id)

    if "ids" in args.stage:
        if topn_df is None:
            # If the user skipped 'topn' in this run but wants 'ids', read the most recent topn parquet.
            # Preferably point to a specific run dir; here we default to the latest by lexical sort.
            base = os.path.join(args.out, f"topn_parquet/topN={args.topN}")
            if not (base.startswith("hdfs://") or base.startswith("s3://")) and not os.path.exists(base):
                raise RuntimeError(f"No topn_parquet for topN={args.topN} under {base}. Run 'topn' or specify correct N.")
            if base.startswith("hdfs://") or base.startswith("s3://"):
                # For object stores/HDFS, you may want to pass a specific run path instead.
                # As a fallback, just read the directory (most providers list child dirs in arbitrary order).
                topn_df = spark.read.parquet(base).select("min_hash", "n_samples")
            else:
                run_dirs = sorted([d for d in os.listdir(base) if d.startswith("run=")])
                if not run_dirs:
                    raise RuntimeError(f"No run= subdirs in {base}.")
                latest = os.path.join(base, run_dirs[-1])
                topn_df = spark.read.parquet(latest).select("min_hash", "n_samples")
        stage_ids(spark, args, df_raw, topn_df, run_id=run_id)

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
