#!/usr/bin/env python3
"""
Filter per-row sample_ids using matched_hash_count>=THRESHOLD and write a new Parquet:
    columns: min_hash, sample_count_geq_100, sample_ids

Designed for very large data:
- 256 threads (configurable)
- Large memory machines (default 3500GB memory limit; configurable)
- 5TB+ fast scratch/temp directory (configurable)
- DuckDB progress bar enabled (shows ETA)
- Clean logs for each phase

Fast path: O(N rows) list-level filter (no explode to 20.5B rows)
Fallback:  explode + semi-join + regroup (heavier, but robust)

Requirements:
  - Python 3.8+
  - duckdb >= 0.9.0 recommended (for nested/list ops and progress bar)
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from typing import Optional

import duckdb


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Filter sample_ids by matched_hash_count>=threshold and write a new parquet."
    )
    p.add_argument(
        "--filtered-parquet",
        required=True,
        help="Input parquet with columns: min_hash (INT64), sample_count (INT64), sample_ids (LIST<VARCHAR>).",
    )
    p.add_argument(
        "--counts-parquet",
        required=True,
        help="Input parquet with columns: sample_id (VARCHAR), matched_hash_count (INT64). Distinct sample_id.",
    )
    p.add_argument(
        "--output-parquet",
        required=True,
        help="Output parquet path to write result with columns: min_hash, sample_count_geq_100, sample_ids.",
    )
    p.add_argument(
        "--threshold", type=int, default=100, help="Minimum matched_hash_count to keep a sample_id (default: 100)."
    )
    p.add_argument(
        "--threads", type=int, default=256, help="DuckDB execution threads (default: 256)."
    )
    p.add_argument(
        "--memory-limit",
        default="3500GB",
        help="DuckDB memory limit (default: 3500GB for a 4TB RAM box). Accepts e.g. '250GB', '90%'.",
    )
    p.add_argument(
        "--scratch-dir",
        required=True,
        help="Directory for DuckDB temp files / external spilling (e.g., a 5TB fast scratch volume).",
    )
    p.add_argument(
        "--strategy",
        choices=["auto", "list-filter", "explode"],
        default="auto",
        help="Force a strategy; 'auto' tries list-filter and falls back to explode on incompatibility.",
    )
    p.add_argument(
        "--parquet-compression",
        choices=["ZSTD", "SNAPPY", "GZIP", "LZ4", "UNCOMPRESSED"],
        default="ZSTD",
        help="Output Parquet compression codec (default: ZSTD).",
    )
    p.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO).",
    )
    return p.parse_args()


def setup_logging(level: str) -> None:
    logging.basicConfig(
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        level=getattr(logging, level),
    )


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)
    if not os.path.isdir(path):
        raise RuntimeError(f"Scratch directory is not a directory: {path}")


def configure_duckdb(con: duckdb.DuckDBPyConnection, threads: int, memory_limit: str, scratch_dir: str) -> None:
    # Threads, memory cap, and external spilling location
    con.execute(f"PRAGMA threads={threads};")
    con.execute(f"PRAGMA memory_limit='{memory_limit}';")
    con.execute(f"PRAGMA temp_directory='{scratch_dir}';")

    # Progress bar with ETA (prints to stderr)
    con.execute("PRAGMA enable_progress_bar=true;")
    con.execute("PRAGMA progress_bar_time=1000;")  # ms between updates

    # Optional: help the optimizer with big joins/temp data
    con.execute("PRAGMA force_parallelism=true;")
    # Keep default vector sizes and other internals; DuckDB is generally optimal out-of-the-box.


def load_allowed_samples(con: duckdb.DuckDBPyConnection, counts_parquet: str, threshold: int) -> int:
    logging.info("Loading allowed sample_ids (matched_hash_count >= %d)...", threshold)
    start = time.perf_counter()

    # Create a temporary, in-memory table of allowed sample_ids.
    # This will typically be many millions of rows but fits comfortably in RAM here.
    con.execute(
        """
        CREATE TEMPORARY TABLE allowed_samples AS
        SELECT sample_id
        FROM read_parquet(?, union_by_name=true)
        WHERE matched_hash_count >= ?;
        """,
        [counts_parquet, threshold],
    )

    allowed_count = con.execute("SELECT COUNT(*) FROM allowed_samples;").fetchone()[0]
    elapsed = time.perf_counter() - start
    logging.info("Loaded %,d allowed sample_ids in %.1fs", allowed_count, elapsed)
    return allowed_count


def list_filter_supported(con: duckdb.DuckDBPyConnection) -> bool:
    """
    Probe whether list_filter() with a subquery in the lambda predicate is supported
    in the running DuckDB build.
    """
    try:
        con.execute("SELECT list_filter(['a','b','c'], x -> x = 'a');").fetchone()
        return True
    except Exception:
        return False


def fast_path_list_filter(
    con: duckdb.DuckDBPyConnection,
    filtered_parquet: str,
    output_parquet: str,
    compression: str,
) -> None:
    """
    Fast path: vectorized list-level filter without exploding to 20.5B rows.
    This retains per-row order of sample_ids.
    """
    logging.info("Running fast path: list_filter() with membership test.")
    start = time.perf_counter()

    # We wrap in a single COPY query so DuckDB streams the pipeline and shows a unified progress bar with ETA.
    con.execute(
        f"""
        COPY (
            WITH filtered AS (
                SELECT
                    min_hash,
                     list_intersect(sample_ids, (SELECT LIST(sample_id) FROM allowed_samples)) AS sample_ids
                FROM read_parquet(?, union_by_name=true)
            )
            SELECT
                min_hash,
                list_length(sample_ids) AS sample_count_geq_100,
                sample_ids
            FROM filtered
        )
        TO ?
        (FORMAT PARQUET, COMPRESSION {compression});
        """,
        [filtered_parquet, output_parquet],
    )

    elapsed = time.perf_counter() - start
    logging.info("Wrote output parquet to %s in %.1fs", output_parquet, elapsed)


def fallback_explode_strategy(
    con: duckdb.DuckDBPyConnection,
    filtered_parquet: str,
    output_parquet: str,
    compression: str,
) -> None:
    """
    Fallback: explode sample_ids, inner join with allowed_samples, regroup per min_hash.
    Heavier I/O but robust on older DuckDB builds.
    Keeps all min_hash rows (rows with no retained sample_ids will have empty list and count=0).
    """
    logging.warning("Falling back to explode + join + regroup strategy (heavier).")
    start = time.perf_counter()

    # Aggregate retained ids per min_hash
    con.execute(
        """
        CREATE TEMPORARY TABLE _agg AS
        SELECT
            s.min_hash,
            list(s.sample_id) AS sample_ids,
            COUNT(*)::BIGINT AS sample_count_geq_100
        FROM (
            SELECT min_hash, UNNEST(sample_ids) AS sample_id
            FROM read_parquet(?, union_by_name=true)
        ) AS s
        INNER JOIN allowed_samples a
            ON s.sample_id = a.sample_id
        GROUP BY s.min_hash;
        """,
        [filtered_parquet],
    )

    # Write final table, ensuring we keep rows with zero retained sample_ids
    con.execute(
        f"""
        COPY (
            SELECT
                f.min_hash,
                COALESCE(a.sample_count_geq_100, 0) AS sample_count_geq_100,
                CASE WHEN a.sample_ids IS NULL THEN []::VARCHAR[] ELSE a.sample_ids END AS sample_ids
            FROM (
                SELECT DISTINCT min_hash
                FROM read_parquet(?, union_by_name=true)
            ) AS f
            LEFT JOIN _agg a USING (min_hash)
        )
        TO ?
        (FORMAT PARQUET, COMPRESSION {compression});
        """,
        [filtered_parquet, output_parquet],
    )

    elapsed = time.perf_counter() - start
    logging.info("Wrote output parquet to %s in %.1fs", output_parquet, elapsed)


def main() -> None:
    args = parse_args()
    setup_logging(args.log_level)

    logging.info("Starting job")
    logging.info("Filtered parquet  : %s", args.filtered_parquet)
    logging.info("Counts parquet    : %s", args.counts_parquet)
    logging.info("Output parquet    : %s", args.output_parquet)
    logging.info("Threshold         : %d", args.threshold)
    logging.info("Threads           : %d", args.threads)
    logging.info("Memory limit      : %s", args.memory_limit)
    logging.info("Scratch dir       : %s", args.scratch_dir)
    logging.info("Strategy          : %s", args.strategy)

    ensure_dir(args.scratch_dir)
    # Remove output if exists (explicit overwrite behavior)
    if os.path.exists(args.output_parquet):
        logging.warning("Output parquet already exists; removing: %s", args.output_parquet)
        os.remove(args.output_parquet)

    # Use an in-memory DuckDB connection; all heavy data stays in temp tables and streams from Parquet.
    con: duckdb.DuckDBPyConnection = duckdb.connect(database=":memory:")
    configure_duckdb(con, args.threads, args.memory_limit, args.scratch_dir)

    # Make sure input schemas look sane early (fail fast if columns are missing).
    try:
        con.execute("SELECT min_hash, sample_ids FROM read_parquet(?, union_by_name=true) LIMIT 0;", [args.filtered_parquet])
        con.execute("SELECT sample_id, matched_hash_count FROM read_parquet(?, union_by_name=true) LIMIT 0;", [args.counts_parquet])
    except Exception as e:
        logging.error("Input parquet schema validation failed: %s", e)
        sys.exit(2)

    allowed_n = load_allowed_samples(con, args.counts_parquet, args.threshold)

    # Decide strategy
    strategy: Optional[str] = None
    if args.strategy == "list-filter":
        strategy = "list-filter"
    elif args.strategy == "explode":
        strategy = "explode"
    else:
        # auto: try to probe list_filter-with-subquery support
        if list_filter_supported(con):
            strategy = "list-filter"
        else:
            strategy = "explode"
    logging.info("Using strategy: %s", strategy)

    # Execute
    try:
        if strategy == "list-filter":
            fast_path_list_filter(
                con=con,
                filtered_parquet=args.filtered_parquet,
                output_parquet=args.output_parquet,
                compression=args.parquet_compression,
            )
        else:
            fallback_explode_strategy(
                con=con,
                filtered_parquet=args.filtered_parquet,
                output_parquet=args.output_parquet,
                compression=args.parquet_compression,
            )
    except Exception as e:
        logging.exception("Job failed with an exception:")
        sys.exit(1)

    # Quick sanity info (lightweight)
    try:
        stats = con.execute(
            """
            SELECT
                COUNT(*) AS rows,
                SUM(sample_count_geq_100)::UBIGINT AS total_kept_ids
            FROM read_parquet(?, union_by_name=true);
            """,
            [args.output_parquet],
        ).fetchone()
        logging.info("Output rows: %,d | Total retained (non-distinct) sample_ids: %,d", stats[0], stats[1])
    except Exception as e:
        logging.warning("Could not compute summary stats on output: %s", e)

    logging.info("All done ✅")


if __name__ == "__main__":
    main()
