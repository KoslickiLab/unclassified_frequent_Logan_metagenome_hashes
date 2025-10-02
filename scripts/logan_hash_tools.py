
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Logan Hash Analyzer
===================

High-scale workflow to answer:
 1) For every hash in a parquet collection, in how many samples does it occur?
 2) For every sample, how many of those hashes does it contain?
 3) Given percentile ranges (e.g., 0.10–0.25), emit:
      a) the sample names associated with each hash whose sample-count falls in that range; or
      b) the sample names of samples whose matched-hash-count falls in that range.
 
Key design points
-----------------
- Uses DuckDB and **index join** (ART index) against a read-only attached DB.
- Avoids building a 33.9B-element in-memory hash table by indexing the smaller "target hashes".
- Treats the massive DB (`database_all.db`) as the streaming side of the join.
- Externalizes heavy operations to disk and limits memory to a user limit (default 3.5TB).
- Produces tidy Parquet outputs and persists intermediate results as requested.
- Includes progress bars in DuckDB (best-effort) and step-wise logging/ETA sampling.
- Fails safe by default (won't overwrite unless `--overwrite` is explicitly set).

Tested entry points (subcommands)
---------------------------------
- `prepare-hashes`  : Ingest the parquet hash collection into a DuckDB working DB and create an index.
- `compute-counts`  : Compute (1) hash->distinct-sample counts and (2) sample->distinct-hash counts.
- `percentiles`     : Compute percentile cutpoints and optionally materialize (a) or (b) subsets.
- `all`             : Convenience driver: prepare hashes -> counts -> percentiles in one run.
- `inspect`         : Quick sanity checks (row counts, schema, head).

Recommended usage (large run)
-----------------------------
1) Prepare hashes once (fast to re-use):
   python logan_hash_tools.py prepare-hashes \
       --hash-parquet-root /scratch/.../anti_join_parquets \
       --hash-glob "bucket=*/data_*.parquet" \
       --working-duckdb /scratch/logan_work.duckdb \
       --threads 512 --memory-limit "3500GB" --temp-dir /scratch/duck_tmp

2) Compute counts (ksize=31 inferred from your path name "k31", but set explicitly):
   python logan_hash_tools.py compute-counts \
       --attach-db /scratch/shared_data_new/Logan_yacht_data/processed_data/database_all.db \
       --attach-alias ro --db-schema sigs_dna --db-table signature_mins \
       --ksize 31 \
       --working-duckdb /scratch/logan_work.duckdb \
       --out-dir /scratch/logan_outputs \
       --threads 512 --memory-limit "3500GB" --temp-dir /scratch/duck_tmp

3) Percentiles + (optional) materialize subsets:
   python logan_hash_tools.py percentiles \
       --working-duckdb /scratch/logan_work.duckdb \
       --counts-source hash --percentiles 0.10 0.25 \
       --attach-db /scratch/shared_data_new/Logan_yacht_data/processed_data/database_all.db \
       --attach-alias ro --db-schema sigs_dna --db-table signature_mins \
       --ksize 31 \
       --emit-mapping-for-hashes \
       --map-out /scratch/logan_outputs/hash_pct_10_25_pairs \
       --threads 512 --memory-limit "3500GB" --temp-dir /scratch/duck_tmp

All outputs are Parquet, compressed with ZSTD. Each file contains a small schema doc in the Parquet
key-value metadata.

Notes
-----
- This script aims for correctness and robustness. At your scale (449B DB rows, 33.9B hashes), 
  runtimes will still be long. The approach intentionally scans the big DB only once per task
  and uses the small indexed 'hashes' table for index joins.
- If `COUNT(DISTINCT ...)` is too memory-intensive, consider the `--materialize-pairs` option in
  `compute-counts`, which first writes the distinct (sample_id, min_hash) pairs to Parquet partitions,
  and then derives counts from those parquet partitions. This spills early and may be friendlier on RAM,
  at the cost of extra I/O.
- For k-mer size, pass `--ksize` explicitly; the code does not try to guess.

Copyright
---------
MIT License. © 2025.
"""

import argparse
import os
import sys
import time
import math
import shutil
import duckdb
from pathlib import Path
from typing import Optional, Tuple, List

# -------------------------------
# Helpers
# -------------------------------

def echo(msg: str):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

def human_bytes(n: int) -> str:
    if n is None:
        return "unknown"
    units = ["B","KB","MB","GB","TB","PB"]
    i = 0
    x = float(n)
    while x >= 1024 and i < len(units)-1:
        i += 1
        x /= 1024.0
    return f"{x:.2f} {units[i]}"

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def check_overwrite(path: Path, overwrite: bool, what: str):
    if path.exists():
        if not overwrite:
            raise SystemExit(f"{what} already exists: {path}\nRefusing to overwrite (use --overwrite to allow).")

def duckdb_connect(db_path: Optional[Path], read_only: bool, threads: int, memory_limit: str, temp_dir: Path) -> duckdb.DuckDBPyConnection:
    # db_path: None -> in-memory
    if db_path is None:
        con = duckdb.connect(database=":memory:", read_only=read_only)
    else:
        con = duckdb.connect(database=str(db_path), read_only=read_only)
    # Configure execution
    con.execute(f"SET threads={threads};")
    # DuckDB accepts e.g. '3500GB' or '3.5TB' as strings
    con.execute(f"SET memory_limit='{memory_limit}';")
    con.execute("SET enable_progress_bar=true;")     # Best-effort progress in Python (prints to stderr/stdout)
    con.execute("PRAGMA progress_bar_time=1000;")    # Show after 1s
    con.execute("PRAGMA temp_directory=?;", [str(temp_dir)])
    con.execute("PRAGMA enable_object_cache=true;")
    con.execute("PRAGMA force_compression='ZSTD';")
    # Prefer external hashing/sorting if needed
    #con.execute("PRAGMA enable_external_access=true;")  # allow file IO
    #con.execute("PRAGMA allow_unsigned_extensions=true;")  # sometimes needed
    return con

def attach_readonly(con: duckdb.DuckDBPyConnection, db_path: Path, alias: str):
    con.execute(f"ATTACH '{db_path}' AS {alias} (READ_ONLY);")

def parquet_copy(con: duckdb.DuckDBPyConnection, sql: str, out_path: Path, overwrite: bool,
                 row_group_size: int = 100_0000):
    # COPY statement writes query result to parquet
    check_overwrite(out_path, overwrite, "Output file")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # Use string formatting instead of parameter placeholder
    copy_sql = f"COPY ({sql}) TO '{str(out_path)}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {row_group_size});"
    con.execute(copy_sql)

def add_kv_metadata(con: duckdb.DuckDBPyConnection, parquet_path: Path, kv: dict):
    # DuckDB doesn't support editing parquet KV metadata in-place; skip for now.
    # (We include schema docs in a sidecar .json instead.)
    meta_json = parquet_path.with_suffix(parquet_path.suffix + ".metadata.json")
    with open(meta_json, "w") as f:
        import json
        json.dump(kv, f, indent=2)

def discover_parquet_glob(hash_parquet_root: Path, hash_glob: str) -> str:
    # Build glob for read_parquet; ensure at least one file exists
    glob_path = hash_parquet_root / hash_glob
    files = list(hash_parquet_root.rglob("*.parquet")) if "*" in hash_glob else list(Path(hash_parquet_root).glob(hash_glob))
    if not files:
        # still return the composed glob, but warn
        echo(f"WARNING: No parquet files found under {hash_parquet_root} with pattern '{hash_glob}'. Continuing; DuckDB will error if truly empty.")
    return str(glob_path)

# -------------------------------
# Core steps
# -------------------------------

def prepare_hashes(args):
    """
    Ingest the hash parquet collection into a DuckDB working DB and create an index.
    """
    working_db = Path(args.working_duckdb)
    temp_dir = Path(args.temp_dir)
    ensure_dir(temp_dir)
    ensure_dir(working_db.parent)

    echo(f"Connecting to working DB: {working_db}")
    con = duckdb_connect(working_db, read_only=False, threads=args.threads, memory_limit=args.memory_limit, temp_dir=temp_dir)

    parquet_glob = discover_parquet_glob(Path(args.hash_parquet_root), args.hash_glob)
    echo(f"Preparing 'hashes' table from read_parquet('{parquet_glob}')")

    # Create schema and table
    con.execute("CREATE SCHEMA IF NOT EXISTS working;")

    if args.recreate_hashes:
        echo("Dropping existing working.hashes (if it exists) ...")
        con.execute("DROP TABLE IF EXISTS working.hashes;")

    # Create table if not exists
    con.execute("""
        CREATE TABLE IF NOT EXISTS working.hashes AS
        SELECT CAST(hash AS BIGINT) AS min_hash
        FROM read_parquet(?, hive_partitioning=true);
    """, [parquet_glob])

    # Deduplicate defensively (fast if already unique)
    if args.deduplicate_hashes:
        echo("Ensuring uniqueness of working.hashes.min_hash ...")
        con.execute("""
            CREATE OR REPLACE TABLE working.hashes AS
            SELECT DISTINCT min_hash FROM working.hashes;
        """)

    # Create ART index (non-unique to be safe)
    echo("Creating index on working.hashes(min_hash) ...")
    # If index already exists, this will create an additional one; check first
    existing_idx = con.execute("SELECT * FROM duckdb_indexes() WHERE table_name='hashes' AND schema_name='working';").fetchall()
    if not existing_idx:
        con.execute("CREATE INDEX hashes_min_hash_idx ON working.hashes(min_hash);")
    else:
        echo("Index already present; skipping CREATE INDEX.")

    # Summaries
    n = con.execute("SELECT COUNT(*) FROM working.hashes;").fetchone()[0]
    echo(f"working.hashes row count: {n:,}")

    # Optionally export a small head preview
    if args.preview_out:
        preview_sql = "SELECT * FROM working.hashes LIMIT 10"
        parquet_copy(con, preview_sql, Path(args.preview_out), overwrite=args.overwrite)

    echo("Done: prepare-hashes")

def check_overwrite_dir(path: Path, overwrite: bool, what: str):
    # Refuse to write into a non-empty existing directory unless --overwrite
    if path.exists() and any(path.iterdir()):
        if not overwrite:
            raise SystemExit(f"{what} already exists and is non-empty: {path}\n"
                             f"Refusing to overwrite (use --overwrite to allow).")

def sql_quote(s: str) -> str:
    return s.replace("'", "''")

def compute_counts(args):
    """
    Compute:
      - hash_counts:   min_hash -> count(distinct sample_id)
      - sample_counts: sample_id -> count(distinct min_hash)
    Either directly via the read-only DB join, or via an optional materialized distinct pair step.
    """
    working_db = Path(args.working_duckdb)
    out_dir = Path(args.out_dir)
    ensure_dir(out_dir)
    temp_dir = Path(args.temp_dir)
    ensure_dir(temp_dir)

    con = duckdb_connect(working_db, read_only=False, threads=args.threads, memory_limit=args.memory_limit, temp_dir=temp_dir)

    # Attach the giant DB read-only
    attach_readonly(con, Path(args.attach_db), args.attach_alias)

    # Identify table path
    fq = f"{args.attach_alias}.{args.db_schema}.{args.db_table}"
    echo(f"Computing counts from {fq} with ksize={args.ksize}")

    # Optional materialization of distinct sample-hash pairs (spills earlier; heavy on IO)
    if args.materialize_pairs:
        pairs_dir = Path(args.pairs_out)
        ensure_dir(pairs_dir)
        check_overwrite(pairs_dir, args.overwrite, "Pairs output dir")

        echo("Materializing DISTINCT (sample_id, min_hash) pairs for matches ...")
        # Write to partitioned parquet by sample_id hash-prefix to improve downstream grouping locality
        # Using COPY statement to Parquet partitioned by 'part' computed from hash bits
        con.execute("CREATE TEMPORARY VIEW _pairs AS "
                    f"SELECT DISTINCT s.sample_id, CAST(s.min_hash AS BIGINT) AS min_hash "
                    f"FROM {fq} s JOIN working.hashes h ON h.min_hash = s.min_hash "
                    f"WHERE s.ksize = {args.ksize};")

        # Partition key for manageable file sizes
        con.execute("""
            COPY (SELECT sample_id, min_hash, (min_hash % 1024) AS part FROM _pairs)
            TO ? (FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (part), ROW_GROUP_SIZE 1000000);
        """, [str(pairs_dir)])

        # Counts from pairs parquet
        echo("Deriving counts from materialized pairs ...")
        # Hash counts
        con.execute("CREATE OR REPLACE TEMPORARY VIEW _pairs_scan AS "
                    "SELECT sample_id, min_hash FROM read_parquet(?, hive_partitioning=true);", [str(pairs_dir / "*/*.parquet")])

        hash_counts_sql = "SELECT min_hash, COUNT(*) AS sample_count FROM _pairs_scan GROUP BY min_hash"
        sample_counts_sql = "SELECT sample_id, COUNT(*) AS matched_hash_count FROM _pairs_scan GROUP BY sample_id"

        parquet_copy(con, hash_counts_sql, out_dir / "hash_counts.parquet", overwrite=args.overwrite)
        parquet_copy(con, sample_counts_sql, out_dir / "sample_counts.parquet", overwrite=args.overwrite)

    else:
        # Direct path: two group-bys with COUNT DISTINCT
        # Hash->distinct sample count
        echo("Computing hash_counts (min_hash -> count(distinct sample_id)) ...")
        hash_counts_sql = (
            f"SELECT s.min_hash AS min_hash, COUNT(DISTINCT s.sample_id) AS sample_count "
            f"FROM {fq} s JOIN working.hashes h ON h.min_hash = s.min_hash "
            f"WHERE s.ksize = {args.ksize} "
            f"GROUP BY s.min_hash"
        )
        parquet_copy(con, hash_counts_sql, out_dir / "hash_counts.parquet", overwrite=args.overwrite)

        # Sample->distinct hash count
        echo("Computing sample_counts (sample_id -> count(distinct min_hash)) ...")
        sample_counts_sql = (
            f"SELECT s.sample_id AS sample_id, COUNT(DISTINCT s.min_hash) AS matched_hash_count "
            f"FROM {fq} s JOIN working.hashes h ON h.min_hash = s.min_hash "
            f"WHERE s.ksize = {args.ksize} "
            f"GROUP BY s.sample_id"
        )
        parquet_copy(con, sample_counts_sql, out_dir / "sample_counts.parquet", overwrite=args.overwrite)

    echo("Done: compute-counts")

def check_overwrite_dir(path: Path, overwrite: bool, what: str):
    # Refuse to write into a non-empty existing directory unless --overwrite
    if path.exists() and any(path.iterdir()):
        if not overwrite:
            raise SystemExit(f"{what} already exists and is non-empty: {path}\n"
                             f"Refusing to overwrite (use --overwrite to allow).")

def sql_quote(s: str) -> str:
    return s.replace("'", "''")

def percentiles(args):
    """
    Compute percentile cutpoints for counts and optionally emit mappings for filtered ranges.
    - When --counts-source=hash  : work on working.hash_counts (or the parquet you point to)
    - When --counts-source=sample: work on working.sample_counts (or the parquet you point to)
    Optionally emits:
      --emit-mapping-for-hashes : (hash in range) -> sample_id pairs
      --emit-sample-list        : sample_ids in range only (no hash pairs)
    """
    working_db = Path(args.working_duckdb)
    temp_dir = Path(args.temp_dir)
    ensure_dir(temp_dir)
    con = duckdb_connect(working_db, read_only=False, threads=args.threads, memory_limit=args.memory_limit, temp_dir=temp_dir)

    def detect_list_agg(con) -> str:
        """
        Pick a list aggregation function supported by this DuckDB build.
        DuckDB v1.3.x supports list(); some builds also support array_agg().
        """
        try:
            con.execute("SELECT list(x) FROM (VALUES (1)) t(x);")
            return "list"
        except Exception:
            try:
                con.execute("SELECT array_agg(x) FROM (VALUES (1)) t(x);")
                return "array_agg"
            except Exception as e:
                raise RuntimeError("Neither list() nor array_agg() is available for list aggregation") from e

    # Load counts
    if args.counts_path:
        counts_path = str(Path(args.counts_path))
    else:
        # Counts previously written into out_dir (hash_counts.parquet / sample_counts.parquet)
        if args.counts_source == "hash":
            counts_path = str(Path(args.out_dir) / "hash_counts.parquet")
        else:
            counts_path = str(Path(args.out_dir) / "sample_counts.parquet")
        if not Path(counts_path).exists():
            raise SystemExit(f"Could not find counts parquet at {counts_path}. Use --counts-path to point to it.")

    echo(f"Loading counts from: {counts_path}")
    counts_path_q = sql_quote(counts_path)
    con.execute(f"CREATE OR REPLACE TEMPORARY VIEW _counts AS SELECT * FROM read_parquet('{counts_path_q}');")
    try:
        n_counts = con.execute("SELECT COUNT(*) FROM _counts;").fetchone()[0]
        echo(f"_counts loaded: {n_counts:,} rows")
    except Exception as _e:
        echo(f"WARNING: could not count _counts rows: {_e}")

    p_low, p_high = args.percentiles
    if not (0.0 <= p_low <= 1.0 and 0.0 <= p_high <= 1.0 and p_low <= p_high):
        raise SystemExit("--percentiles must be two floats in [0,1], with low <= high.")

    # Identify the count column
    if args.counts_source == "hash":
        count_col = "sample_count"
        key_col = "min_hash"
    else:
        count_col = "matched_hash_count"
        key_col = "sample_id"

    # Compute quantiles
    echo(f"Computing quantiles for {count_col}: p_low={p_low}, p_high={p_high}")
    row = con.execute(f"SELECT quantile_cont({count_col}, {p_low}), quantile_cont({count_col}, {p_high}) FROM _counts;").fetchone()
    low_cut, high_cut = row[0], row[1]
    echo(f"Percentile cutpoints: low={low_cut}, high={high_cut}")

    # Persist cutpoints
    cp_path = Path(args.out_dir) / f"{args.counts_source}_percentiles.json"
    Path(args.out_dir).mkdir(parents=True, exist_ok=True)
    with open(cp_path, "w") as f:
        import json
        json.dump({"source": args.counts_source, "p_low": p_low, "p_high": p_high, "low_cut": low_cut, "high_cut": high_cut}, f, indent=2)

    # Emit subsets if requested
    if args.counts_source == "hash" and args.emit_mapping_for_hashes:
        # Attach read-only DB for mapping
        attach_readonly(con, Path(args.attach_db), args.attach_alias)
        fq = f"{args.attach_alias}.{args.db_schema}.{args.db_table}"

        echo("Materializing (hash in range) -> sample_id pairs ...")
        # Selected hashes
        con.execute(f"""
            CREATE OR REPLACE TEMPORARY VIEW _selected_hashes AS
            SELECT {key_col} AS min_hash
            FROM _counts
            WHERE {count_col} >= {low_cut} AND {count_col} <= {high_cut};
        """)
        try:
            n_sel = con.execute("SELECT COUNT(*) FROM _selected_hashes;").fetchone()[0]
            echo(f"Selected hashes in percentile range: {n_sel:,}")
        except Exception as _e:
            echo(f"WARNING: could not count _selected_hashes rows: {_e}")

        map_dir = Path(args.map_out)
        check_overwrite_dir(map_dir, args.overwrite, "Map output dir")
        ensure_dir(map_dir)

        # Controlled sharding options
        num_shards = int(getattr(args, "pairs_shards", 128))
        rg_size = int(getattr(args, "pairs_row_group_size", 1_000_000))
        shard_key = getattr(args, "pairs_shard_key", "hash")  # 'hash' or 'sample'
        if shard_key not in ("hash", "sample"):
            raise SystemExit("--pairs-shard-key must be 'hash' or 'sample'")

        if getattr(args, "pairs_as_lists", False):
            # --- Aggregated LIST form per shard ---
            agg_fn = detect_list_agg(con)
            prefix = getattr(args, "pairs_file_prefix", "pairs_lists_shard_")
            sort_lists = getattr(args, "pairs_sort_lists", False)
            echo(f"Writing aggregated hash→sample lists: {num_shards} shards (ROW_GROUP_SIZE={rg_size}, shard_key={shard_key})")
            for shard in range(num_shards):
                shard_path = map_dir / f"{prefix}{shard:04d}.parquet"
                check_overwrite(shard_path, args.overwrite, "Shard parquet")
                shard_path_q = sql_quote(str(shard_path))
                echo(f"  -> shard {shard+1}/{num_shards} (id={shard}) -> {shard_path.name}")
                if shard_key == "hash":
                    shard_pred = f"(s.min_hash % {num_shards}) = {shard}"
                else:
                    shard_pred = f"(hash(s.sample_id) % {num_shards}) = {shard}"
                list_expr = f"{agg_fn}(sample_id)"
                if sort_lists:
                    list_expr = f"list_sort({list_expr})"
                # Use DISTINCT pairs to avoid duplicate sample_ids per hash, then aggregate
                con.execute(f"""
                    COPY (
                      SELECT
                        min_hash,
                        COUNT(*) AS sample_count,
                        {list_expr} AS sample_ids
                      FROM (
                        SELECT DISTINCT s.min_hash AS min_hash, s.sample_id AS sample_id
                        FROM {fq} s
                        JOIN _selected_hashes sh ON sh.min_hash = s.min_hash
                        WHERE s.ksize = {args.ksize} AND {shard_pred}
                      ) d
                      GROUP BY min_hash
                    )
                    TO '{shard_path_q}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {rg_size});
                """)
            echo("Finished writing aggregated hash→sample lists (sharded).")
        else:
            # --- Flat pairs form per shard (existing behavior) ---
            prefix = getattr(args, "pairs_file_prefix", "pairs_shard_")
            echo(f"Writing hash→sample pairs: {num_shards} shards (ROW_GROUP_SIZE={rg_size}, shard_key={shard_key})")
            for shard in range(num_shards):
                shard_path = map_dir / f"{prefix}{shard:04d}.parquet"
                check_overwrite(shard_path, args.overwrite, "Shard parquet")
                shard_path_q = sql_quote(str(shard_path))
                echo(f"  -> shard {shard+1}/{num_shards} (id={shard}) -> {shard_path.name}")
                if shard_key == "hash":
                    shard_pred = f"(s.min_hash % {num_shards}) = {shard}"
                else:
                    shard_pred = f"(hash(s.sample_id) % {num_shards}) = {shard}"

    if args.counts_source == "sample" and args.emit_sample_list:
        echo("Materializing sample_id list in percentile range ...")
        out_path = Path(args.sample_list_out)
        ensure_dir(out_path.parent)
        parquet_copy(con, f"SELECT {key_col} AS sample_id FROM _counts WHERE {count_col} >= {low_cut} AND {count_col} <= {high_cut}", out_path, overwrite=args.overwrite)

    echo("Done: percentiles")


def inspect(args):
    """
    Lightweight inspection helper for sanity checks.
    """
    working_db = Path(args.working_duckdb)
    con = duckdb_connect(working_db, read_only=False, threads=args.threads, memory_limit=args.memory_limit, temp_dir=Path(args.temp_dir))
    attach = None
    if args.attach_db:
        attach_readonly(con, Path(args.attach_db), args.attach_alias)
        attach = f"{args.attach_alias}.{args.db_schema}.{args.db_table}"
    echo("---- Inspect: working.hashes ----")
    try:
        print(con.execute("SELECT COUNT(*) AS n FROM working.hashes;").df())
        print(con.execute("SELECT * FROM working.hashes LIMIT 5;").df())
    except Exception as e:
        echo(f"Could not inspect working.hashes: {e}")
    if attach:
        echo(f"---- Inspect: {attach} (ksize={args.ksize}) ----")
        try:
            print(con.execute(f"SELECT COUNT(*) AS n_ksize FROM {attach} WHERE ksize={args.ksize};").df())
            print(con.execute(f"SELECT * FROM {attach} WHERE ksize={args.ksize} LIMIT 5;").df())
        except Exception as e:
            echo(f"Could not inspect attached table: {e}")
    echo("Done: inspect")


# -------------------------------
# CLI
# -------------------------------

def main(argv=None):
    p = argparse.ArgumentParser(
        prog="logan_hash_tools",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Scalable DuckDB pipeline for metagenome hash/sample analysis."
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    # prepare-hashes
    p_prep = sub.add_parser("prepare-hashes", help="Ingest parquet hashes into working DuckDB and create index")
    p_prep.add_argument("--hash-parquet-root", required=True, help="Root directory containing hash parquet partitions")
    p_prep.add_argument("--hash-glob", default="bucket=*/data_*.parquet", help="Glob relative to root for read_parquet")
    p_prep.add_argument("--working-duckdb", required=True, help="Path to working DuckDB file (created if missing)")
    p_prep.add_argument("--deduplicate-hashes", action="store_true", help="Ensure DISTINCT min_hash in working.hashes")
    p_prep.add_argument("--recreate-hashes", action="store_true", help="Drop & re-create working.hashes")
    p_prep.add_argument("--preview-out", default="", help="Optional small parquet preview of working.hashes")
    p_prep.add_argument("--overwrite", action="store_true", help="Allow overwriting outputs (preview only)")
    p_prep.add_argument("--threads", type=int, default=os.cpu_count() or 64, help="DuckDB threads")
    p_prep.add_argument("--memory-limit", default="3500GB", help="DuckDB memory limit (e.g., '3500GB' or '3.5TB')")
    p_prep.add_argument("--temp-dir", default="/scratch/duck_tmp", help="Directory for DuckDB temp & spills")
    p_prep.set_defaults(func=prepare_hashes)

    # compute-counts
    p_cnt = sub.add_parser("compute-counts", help="Compute counts for questions (1) and (2)")
    p_cnt.add_argument("--working-duckdb", required=True, help="Path to working DuckDB (with working.hashes)")
    p_cnt.add_argument("--attach-db", required=True, help="Path to giant database_all.db")
    p_cnt.add_argument("--attach-alias", default="ro", help="Alias name for attached DB")
    p_cnt.add_argument("--db-schema", default="sigs_dna", help="Schema name in attached DB")
    p_cnt.add_argument("--db-table", default="signature_mins", help="Table name in attached DB")
    p_cnt.add_argument("--ksize", type=int, required=True, help="Filter k-mer size (required)")
    p_cnt.add_argument("--out-dir", required=True, help="Directory for outputs (parquet)")
    p_cnt.add_argument("--overwrite", action="store_true", help="Allow overwriting parquet outputs")
    p_cnt.add_argument("--threads", type=int, default=os.cpu_count() or 64, help="DuckDB threads")
    p_cnt.add_argument("--memory-limit", default="3500GB", help="DuckDB memory limit (e.g., '3500GB' or '3.5TB')")
    p_cnt.add_argument("--temp-dir", default="/scratch/duck_tmp", help="Directory for DuckDB temp & spills")

    # optional pair materialization
    p_cnt.add_argument("--materialize-pairs", action="store_true", help="First write DISTINCT(sample_id,min_hash) matches to partitioned parquet")
    p_cnt.add_argument("--pairs-out", default="", help="Output directory for materialized pairs parquet (required if --materialize-pairs)")
    p_cnt.set_defaults(func=compute_counts)

    # percentiles
    p_pct = sub.add_parser("percentiles", help="Compute percentiles and optionally emit mappings/lists")
    p_pct.add_argument("--working-duckdb", required=True, help="Path to working DuckDB (with counts or point via --counts-path)")
    p_pct.add_argument("--out-dir", required=True, help="Directory where counts parquet live (or where percentiles JSON will be written)")
    p_pct.add_argument("--counts-source", choices=["hash","sample"], required=True, help="Which counts are we analyzing")
    p_pct.add_argument("--counts-path", default="", help="Explicit path to counts parquet; otherwise uses out-dir default")
    p_pct.add_argument("--percentiles", nargs=2, type=float, required=True, metavar=("LOW","HIGH"), help="Two floats in [0,1]")
    p_pct.add_argument("--threads", type=int, default=os.cpu_count() or 64, help="DuckDB threads")
    p_pct.add_argument("--memory-limit", default="3500GB", help="DuckDB memory limit (e.g., '3500GB' or '3.5TB')")
    p_pct.add_argument("--temp-dir", default="/scratch/duck_tmp", help="Directory for DuckDB temp & spills")
    p_pct.add_argument("--overwrite", action="store_true", help="Allow overwriting outputs")

    # optional mapping for hash ranges
    p_pct.add_argument("--emit-mapping-for-hashes", action="store_true", help="Emit DISTINCT(min_hash,sample_id) for hashes in percentile range")
    p_pct.add_argument("--map-out", default="", help="Output dir for hash->sample_id pairs parquet (required if --emit-mapping-for-hashes)")
    p_pct.add_argument("--ksize", type=int, default=31, help="K-mer size filter for mapping")
    # fewer, bigger files: write a fixed number of shards instead of per-thread fragments
    p_pct.add_argument("--pairs-shards", type=int, default=128,
                       help="Number of output parquet shards for hash→sample pairs (smaller = fewer, bigger files)")
    p_pct.add_argument("--pairs-row-group-size", type=int, default=1_000_000,
                       help="ROW_GROUP_SIZE to use when writing shards")
    p_pct.add_argument("--pairs-file-prefix", default="pairs_shard_", help="File prefix for shard parquet files")
    p_pct.add_argument("--pairs-shard-key", default="hash", choices=["hash","sample"],
                       help="Sharding key: by min_hash or by sample_id hash()")
    p_pct.add_argument("--pairs-as-lists", action="store_true",
                       help="Write aggregated list form per shard: (min_hash, sample_count, sample_ids LIST<VARCHAR>)")
    p_pct.add_argument("--pairs-sort-lists", action="store_true",
                       help="Sort values inside each aggregated list")

    # attach DB for mapping step
    p_pct.add_argument("--attach-db", default="", help="Path to giant database_all.db (required if --emit-mapping-for-hashes)")
    p_pct.add_argument("--attach-alias", default="ro", help="Alias name for attached DB")
    p_pct.add_argument("--db-schema", default="sigs_dna", help="Schema name in attached DB")
    p_pct.add_argument("--db-table", default="signature_mins", help="Table name in attached DB")

    # optional sample list for sample-range selection
    p_pct.add_argument("--emit-sample-list", action="store_true", help="Emit sample_id list whose counts fall within the percentile range")
    p_pct.add_argument("--sample-list-out", default="", help="Output parquet for sample list")

    p_pct.set_defaults(func=percentiles)

    # inspect
    p_ins = sub.add_parser("inspect", help="Sanity checks for working.hashes and attached DB")
    p_ins.add_argument("--working-duckdb", required=True, help="Path to working DuckDB")
    p_ins.add_argument("--attach-db", default="", help="Optional: path to DB to attach")
    p_ins.add_argument("--attach-alias", default="ro", help="Alias name for attached DB")
    p_ins.add_argument("--db-schema", default="sigs_dna", help="Schema name in attached DB")
    p_ins.add_argument("--db-table", default="signature_mins", help="Table name in attached DB")
    p_ins.add_argument("--ksize", type=int, default=31, help="K-mer size filter for quick checks")
    p_ins.add_argument("--threads", type=int, default=os.cpu_count() or 64, help="DuckDB threads")
    p_ins.add_argument("--memory-limit", default="3500GB", help="DuckDB memory limit (e.g., '3500GB' or '3.5TB')")
    p_ins.add_argument("--temp-dir", default="/scratch/duck_tmp", help="Directory for DuckDB temp & spills")
    p_ins.set_defaults(func=inspect)

    args = p.parse_args(argv)

    # Basic validations
    if args.cmd == "compute-counts":
        if args.materialize_pairs and not args.pairs_out:
            p.error("--materialize-pairs requires --pairs-out")
    if args.cmd == "percentiles":
        if args.emit_mapping_for_hashes and not args.map_out:
            p.error("--emit-mapping-for-hashes requires --map-out")
        if args.emit_mapping_for_hashes and not args.attach_db:
            p.error("--emit-mapping-for-hashes requires --attach-db")
        if args.emit_sample_list and not args.sample_list_out:
            p.error("--emit-sample-list requires --sample-list-out")

    # Execute
    try:
        args.func(args)
    except Exception as e:
        # Print full traceback for easier debugging (line numbers & stack)
        import traceback
        echo(f"ERROR: {type(e).__name__}: {e}")
        tb = traceback.format_exc()
        print(tb, file=sys.stderr, flush=True)
        sys.exit(2)


if __name__ == "__main__":
    main()
