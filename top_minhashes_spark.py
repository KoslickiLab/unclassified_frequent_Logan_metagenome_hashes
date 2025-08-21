#!/usr/bin/env python3
"""
Exact top-N min_hash by distinct sample_id count, plus (for chosen N) the list of sample_ids per hash.

- Multi-executor single-host via Spark's local-cluster master (recommended).
- Falls back to single-JVM local[...] if local-cluster is unavailable.
- Exact counts without early global dropDuplicates (uses 2-level GROUP BY to reduce memory spikes).
- Forces sort-based aggregates to avoid big in-memory hash sets; clamps shuffle page size to 32 MiB.
- Auto-tunes shuffle partitions from input split estimate to keep #shuffle blocks reasonable.
- SSD spill dirs, extended timeouts, versioned outputs with clean filenames (no stomping).
"""

import argparse
import os
import sys
import math
import glob
from datetime import datetime, timezone
from typing import Optional, List

from pyspark.sql import SparkSession, DataFrame, functions as F, types as T


# ----------------------- CLI -----------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Find top-N min_hashes by distinct sample_id count and list their sample_ids (Spark).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # Required I/O + stages
    p.add_argument("--input", required=True, help="Input Parquet root (requires min_hash:int64, sample_id:string).")
    p.add_argument("--out",   required=True, help="Output root directory.")
    p.add_argument("--stage", nargs="+", required=True, choices=["counts", "topn", "ids"],
                   help="Stages to run. Run 'counts' once; reuse for 'topn' and 'ids'.")

    # Analysis parameters
    p.add_argument("--topN", type=int, default=100, help="Top N hashes (for topn and ids).")
    p.add_argument("--ksize", type=int, default=None, help="Optional filter on ksize if present.")

    # Run identity
    p.add_argument("--run-tag", default=None, help="Optional run label; else UTC timestamp.")

    # Execution mode (multi-executor by default)
    p.add_argument("--mode", choices=["local-cluster", "local"], default="local-cluster",
                   help="Use multi-executor local-cluster (recommended) or single-JVM local.")

    # local-cluster knobs (Spark wants MB; we accept g/m and convert)
    p.add_argument("--executors", type=int, default=16, help="Executors (workers) for local-cluster.")
    p.add_argument("--cores-per-exec", type=int, default=8, help="Cores per executor.")
    p.add_argument("--mem-per-exec", default="128g",
                   help="Memory per executor (e.g., 128g, 96g, or MB integer).")

    # local (single-JVM) fallback knobs
    p.add_argument("--local-cores", type=int, default=128, help="Cores in single-JVM local mode.")
    p.add_argument("--driver-memory", default="32g", help="Driver heap (kept moderate for JDK11).")
    p.add_argument("--executor-memory", default="1500g", help="Executor heap in local mode (single JVM).")

    # Spill dirs (SSD)
    p.add_argument("--local-dirs", default="/scratch/spark_local",
                   help="Comma-separated spill dirs (created if missing).")

    # Performance
    p.add_argument("--max-partition-bytes", default="512m",
                   help="Read split size (e.g., 256m, 512m, 1g). Larger => fewer map tasks.")
    p.add_argument("--target-shuffle-blocks", type=int, default=30_000_000,
                   help="Upper bound for (#map_splits × #reduce_partitions).")

    # Output shaping
    p.add_argument("--json-coalesce", type=int, default=1,
                   help="How many JSON files to create (raise for large N).")
    p.add_argument("--compress-json", action="store_true",
                   help="Write JSON compressed with gzip.")

    # Dry-run
    p.add_argument("--sample-fraction", type=float, default=None,
                   help="Row-level sample fraction (0 < f ≤ 1) for a quick test.")
    p.add_argument("--limit-files", type=int, default=None,
                   help="Limit # of Parquet files read (quick test).")

    return p.parse_args()


# ----------------------- Helpers -----------------------

def utc_run_id(custom: Optional[str]) -> str:
    return custom or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def ensure_dirs(paths_csv: str) -> None:
    for d in paths_csv.split(","):
        d = d.strip()
        if d and not os.path.exists(d):
            os.makedirs(d, exist_ok=True)


def parse_size_to_bytes(s: str) -> int:
    s = s.strip().lower()
    if s.endswith("k"): return int(s[:-1]) * (1 << 10)
    if s.endswith("m"): return int(s[:-1]) * (1 << 20)
    if s.endswith("g"): return int(s[:-1]) * (1 << 30)
    return int(s)


def parse_size_to_mb(s: str) -> int:
    s = s.strip().lower()
    if s.endswith("k"): return max(1, int(int(s[:-1]) // 1024))
    if s.endswith("m"): return int(s[:-1])
    if s.endswith("g"): return int(s[:-1]) * 1024
    return int(s)  # assume already MB


def estimate_input_splits_localfs(input_path: str, max_partition_bytes: str) -> int:
    mpb = parse_size_to_bytes(max_partition_bytes)
    splits = 0
    for root, _, files in os.walk(input_path):
        for f in files:
            if f.endswith(".parquet"):
                p = os.path.join(root, f)
                try:
                    size = os.path.getsize(p)
                except OSError:
                    size = 0
                splits += max(1, math.ceil(size / mpb)) if size > 0 else 1
    return max(1, splits)


# Hadoop FS helpers (rename/cleanup)
def _jvm(spark: SparkSession):
    return spark._jvm

def get_hadoop_fs(spark: SparkSession, path: str):
    jvm = _jvm(spark)
    conf = spark._jsc.hadoopConfiguration()
    uri = jvm.java.net.URI.create(path)
    return jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)

def hpath(spark: SparkSession, path: str):
    return _jvm(spark).org.apache.hadoop.fs.Path(path)

def list_status(spark: SparkSession, dir_path: str):
    fs = get_hadoop_fs(spark, dir_path)
    return list(fs.listStatus(hpath(spark, dir_path)))

def rename_and_clean_parts(
    spark: SparkSession,
    out_dir: str,
    final_filename_base: str,
    compressed: bool
) -> List[str]:
    statuses = list_status(spark, out_dir)
    parts = []
    for st in statuses:
        name = st.getPath().getName()
        if name.startswith("part-") and (name.endswith(".json") or name.endswith(".json.gz")):
            parts.append(name)
    parts.sort()
    if not parts:
        raise RuntimeError(f"No part-*.json(.gz) found in {out_dir}.")

    fs = get_hadoop_fs(spark, out_dir)
    ext = ".json.gz" if compressed else ".json"
    finals = []
    for i, pname in enumerate(parts):
        src = hpath(spark, os.path.join(out_dir, pname))
        suffix = "" if len(parts) == 1 else f".part-{i:05d}"
        dst_name = f"{final_filename_base}{suffix}{ext}"
        dst = hpath(spark, os.path.join(out_dir, dst_name))
        if not fs.rename(src, dst):
            raise RuntimeError(f"Failed to rename {pname} -> {dst_name}")
        finals.append(dst.toString())

    # Clean leftovers
    for st in list_status(spark, out_dir):
        name = st.getPath().getName()
        if name in [os.path.basename(p) for p in finals]:
            continue
        if name == "_SUCCESS" or name.startswith("part-") or name.endswith(".crc") or name.startswith("."):
            fs.delete(st.getPath(), False)
    return finals


# ----------------------- Spark session -----------------------

def build_spark(args: argparse.Namespace) -> SparkSession:
    # Ensure spill dirs exist and advertise to Spark
    ensure_dirs(args.local_dirs)
    os.environ["SPARK_LOCAL_DIRS"] = args.local_dirs

    base = (
        SparkSession.builder
        .appName("TopMinhashesDistinctSamples")
        # IO/perf
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.files.maxPartitionBytes", args.max_partition_bytes)
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.sql.parquet.mergeSchema", "false")
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.driver.maxResultSize", "0")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        # Memory behavior
        .config("spark.memory.fraction", "0.80")
        .config("spark.memory.storageFraction", "0.20")
        .config("spark.buffer.pageSize", "32m")  # clamp shuffle pages
        # Prefer sort-based aggregation to avoid huge in-memory hash sets
        .config("spark.sql.execution.useObjectHashAggregate", "false")
        .config("spark.sql.execution.sortBeforeRepartition", "true")
        # AQE guidance
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "512m")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        # Shuffle/IO
        .config("spark.shuffle.compress", "true")
        .config("spark.shuffle.spill.compress", "true")
        .config("spark.shuffle.file.buffer", "512k")
        # Heartbeats/RPC
        .config("spark.network.timeout", "800s")
        .config("spark.executor.heartbeatInterval", "20s")
        .config("spark.rpc.message.maxSize", "512")
        .config("spark.sql.broadcastTimeout", "3600s")
        # Spill dirs
        .config("spark.local.dir", args.local_dirs)
        # JDK11-friendly GC knob: larger G1 regions reduce memory-mapping pressure
        .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:G1HeapRegionSize=32m")
        .config("spark.driver.extraJavaOptions",   "-XX:+UseG1GC -XX:G1HeapRegionSize=32m")
    )

    if args.mode == "local-cluster":
        mem_mb = parse_size_to_mb(args.mem_per_exec)  # MB required
        master = f"local-cluster[{args.executors},{args.cores_per_exec},{mem_mb}]"
        builder = base.master(master).config("spark.driver.memory", args.driver_memory)
    else:
        master = f"local[{args.local_cores}]"
        builder = (base.master(master)
                        .config("spark.driver.memory", args.driver_memory)
                        .config("spark.executor.memory", args.executor_memory))

    try:
        spark = builder.getOrCreate()
    except Exception as e:
        if args.mode == "local-cluster":
            print(f"[warn] Failed to start master {master}: {e}")
            print("[warn] Falling back to single-JVM local mode.")
            master_fallback = f"local[{args.local_cores}]"
            builder2 = (base.master(master_fallback)
                             .config("spark.driver.memory", args.driver_memory)
                             .config("spark.executor.memory", args.executor_memory))
            spark = builder2.getOrCreate()
        else:
            raise

    print(f"[spark] master = {spark.sparkContext.master}")

    # Auto-tune reduce partitions: (#splits × #reduces) <= target
    try:
        splits = estimate_input_splits_localfs(args.input, args.max_partition_bytes)
        target = max(10_000_000, int(args.target_shuffle_blocks))
        reduces = max(1024, min(16384, max(1, target // max(1, splits))))
        spark.conf.set("spark.sql.shuffle.partitions", str(reduces))
        print(f"[auto-shuffle] input_splits≈{splits}, targetBlocks={target} -> shuffle.partitions={reduces}")
    except Exception as e:
        print(f"[auto-shuffle] Could not estimate input splits ({e}); using Spark default.")

    spark.sparkContext.setLogLevel("WARN")
    return spark


# ----------------------- Data I/O -----------------------

def read_raw_dataset(spark: SparkSession, args: argparse.Namespace) -> DataFrame:
    if args.limit_files is not None and args.limit_files > 0:
        files = sorted(glob.glob(os.path.join(args.input, "**/*.parquet"), recursive=True))[: args.limit_files]
        if not files:
            raise RuntimeError("No Parquet files found for --limit-files.")
        df = spark.read.parquet(*files)
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
        if not (0.0 < args.sample_fraction <= 1.0):
            raise ValueError("--sample-fraction must be in (0, 1].")
        df = df.sample(False, args.sample_fraction, seed=1337)

    return df


# ----------------------- Stages -----------------------

COUNTS_PARTS = 8192  # partitions for counts output (bucketed)

def stage_counts(spark: SparkSession, args: argparse.Namespace, df_raw: DataFrame) -> None:
    """
    Exact counts without early global dropDuplicates:
      1) pairs = GROUP BY (min_hash, sample_id)
      2) counts = GROUP BY min_hash over the collapsed pairs
    """
    out_counts = os.path.join(args.out, "counts_parquet")

    # 1) Collapse duplicate pairs exactly
    pairs = (
        df_raw
        .select("min_hash", "sample_id")
        .groupBy("min_hash", "sample_id")
        .agg(F.count(F.lit(1)).alias("pair_count"))   # value unused; forces exact dedup via sort-based agg
        .select("min_hash", "sample_id")
    )

    # 2) Count distinct sample_ids per min_hash exactly
    counts_df = (
        pairs
        .groupBy("min_hash")
        .agg(F.count(F.lit(1)).alias("n_samples"))
        .withColumn("bucket", F.pmod(F.col("min_hash"), F.lit(COUNTS_PARTS)).cast(T.IntegerType()))
        .repartition(COUNTS_PARTS, "bucket")
    )

    counts_df.write.mode("overwrite").partitionBy("bucket").parquet(out_counts)
    print(f"[counts] wrote: {out_counts}")


def read_counts(spark: SparkSession, args: argparse.Namespace) -> DataFrame:
    path = os.path.join(args.out, "counts_parquet")
    if not (path.startswith("hdfs://") or path.startswith("s3://")) and not os.path.exists(path):
        raise RuntimeError(f"Counts parquet not found at {path}. Run 'counts' first.")
    return spark.read.parquet(path).select("min_hash", "n_samples")


def stage_topn(spark: SparkSession, args: argparse.Namespace, run_id: str) -> DataFrame:
    counts_df = read_counts(spark, args)
    topn_df = counts_df.orderBy(F.col("n_samples").desc(), F.col("min_hash").asc()).limit(args.topN)

    sub_topn_parquet = os.path.join(args.out, f"topn_parquet/topN={args.topN}/run={run_id}")
    sub_topn_json    = os.path.join(args.out, f"topn_json/topN={args.topN}/run={run_id}")

    topn_df.write.mode("errorifexists").parquet(sub_topn_parquet)

    writer = topn_df.coalesce(args.json_coalesce).write.mode("errorifexists")
    writer = writer.option("compression", "gzip") if args.compress_json else writer
    writer.json(sub_topn_json)

    base = f"topn.summary.topN={args.topN}"
    if args.ksize is not None:
        base += f".ksize={args.ksize}"
    base += f".run={run_id}"

    finals = rename_and_clean_parts(spark, sub_topn_json, base, compressed=args.compress_json)
    print(f"[topn] wrote parquet: {sub_topn_parquet}")
    for f in finals:
        print(f"[topn] wrote json:    {f}")

    return topn_df


def stage_ids(spark: SparkSession, args: argparse.Namespace, df_raw: DataFrame, run_id: str,
              topn_df: Optional[DataFrame] = None) -> None:
    if topn_df is None:
        base = os.path.join(args.out, f"topn_parquet/topN={args.topN}")
        if not (base.startswith("hdfs://") or base.startswith("s3://")) and not os.path.exists(base):
            raise RuntimeError(f"No topn_parquet for topN={args.topN} under {base}. Run 'topn' first.")
        run_dirs = sorted([d for d in os.listdir(base) if d.startswith("run=")])
        if not run_dirs:
            raise RuntimeError(f"No run= subdirs under {base}.")
        latest = os.path.join(base, run_dirs[-1])
        topn_df = spark.read.parquet(latest).select("min_hash", "n_samples")

    topn_keys = topn_df.select("min_hash")

    # When N is very large, avoid broadcast to conserve driver memory
    if args.topN <= 5_000_000:
        join_keys = F.broadcast(topn_keys)
    else:
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
        join_keys = topn_keys

    filtered = df_raw.join(join_keys, on="min_hash", how="inner").select("min_hash", "sample_id")

    # Collapse duplicate pairs for these keys (exact)
    distinct_pairs = (
        filtered
        .groupBy("min_hash", "sample_id")
        .agg(F.count(F.lit(1)).alias("pair_count"))
        .select("min_hash", "sample_id")
    )

    # Build sample lists per min_hash
    agg_df = (
        distinct_pairs
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

    sub_ids_json = os.path.join(args.out, f"topn_ids_json/topN={args.topN}/run={run_id}")
    writer = result_df.coalesce(args.json_coalesce).write.mode("errorifexists")
    writer = writer.option("compression", "gzip") if args.compress_json else writer
    writer.json(sub_ids_json)

    base = f"topn_ids.topN={args.topN}"
    if args.ksize is not None:
        base += f".ksize={args.ksize}"
    base += f".run={run_id}"

    finals = rename_and_clean_parts(spark, sub_ids_json, base, compressed=args.compress_json)
    for f in finals:
        print(f"[ids] wrote json: {f}")


# ----------------------- Main -----------------------

def main() -> int:
    args = parse_args()

    if not (args.out.startswith("hdfs://") or args.out.startswith("s3://")):
        os.makedirs(args.out, exist_ok=True)

    run_id = utc_run_id(args.run_tag)
    spark = build_spark(args)

    df_raw = None
    if "counts" in args.stage or "ids" in args.stage:
        df_raw = read_raw_dataset(spark, args)

    if "counts" in args.stage:
        stage_counts(spark, args, df_raw)

    topn_df = None
    if "topn" in args.stage:
        topn_df = stage_topn(spark, args, run_id)

    if "ids" in args.stage:
        stage_ids(spark, args, df_raw, run_id, topn_df)

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
