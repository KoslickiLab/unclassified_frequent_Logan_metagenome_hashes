#!/usr/bin/env python3
"""
Filter per-hash sample_ids by a per-sample matched_hash_count threshold.

Inputs:
  - filtered_hashes_no_Serratus.parquet  (columns: min_hash, sample_count, sample_ids)
  - sample_counts.parquet                (columns: sample_id, matched_hash_count)

Output:
  - Parquet with columns: min_hash, sample_count_geq_100, sample_ids
    where sample_ids contains only those sample_id with matched_hash_count >= threshold
    and sample_count_geq_100 is the length of that filtered list.

Usage:
  python filter_sample_ids_by_threshold.py \
      --hashes filtered_hashes_no_Serratus.parquet \
      --counts sample_counts.parquet \
      --out filtered_hashes_geq100.parquet \
      --threshold 100 \
      [--drop-empty]
"""
from __future__ import annotations

import argparse
import pathlib
import sys
import duckdb
import os


def build_sql(drop_empty: bool) -> str:
    """
    Returns the parameterized SQL that:
      1) finds eligible sample_ids by threshold,
      2) unnests each row's sample_ids with ordinal positions to keep a stable order,
      3) left-joins against the eligible list,
      4) re-aggregates filtered sample_ids and counts per min_hash,
      5) preserves all min_hashes and writes empty lists when no samples qualify
         (unless --drop-empty was requested).
    """
    # NOTE:
    # - We use ARRAY_AGG ... FILTER (WHERE ...) to keep only eligible samples.
    # - We use UNNEST(... ) WITH ORDINALITY and ORDER BY that ord to keep original order.
    # - We COALESCE the count to 0 and the list to []::VARCHAR[] when no samples qualify.
    # - If your DuckDB is very old and complains about FILTER on aggregates or []::VARCHAR[],
    #   consider upgrading DuckDB (recommended) or replace the empty list with NULL.
    base_query = r"""
WITH eligible AS (
  SELECT sample_id
  FROM read_parquet($sample_counts)
  WHERE matched_hash_count >= $threshold
),
expanded AS (
  SELECT
      f.min_hash,
      sid AS sample_id,
      ord
  FROM read_parquet($hashes) AS f,
       UNNEST(f.sample_ids) WITH ORDINALITY AS t(sid, ord)
),
grouped AS (
  SELECT
      e.min_hash,
      -- Only keep eligible sample_ids; maintain original order via ord
      ARRAY_AGG(e.sample_id ORDER BY e.ord) FILTER (WHERE el.sample_id IS NOT NULL) AS filtered_sample_ids,
      COUNT(el.sample_id) AS sample_count_geq_100
  FROM expanded e
  LEFT JOIN eligible el
    ON el.sample_id = e.sample_id
  GROUP BY e.min_hash
),
base AS (
  SELECT DISTINCT min_hash
  FROM read_parquet($hashes)
),
final AS (
  SELECT
      b.min_hash,
      COALESCE(g.sample_count_geq_100, 0) AS sample_count_geq_100,
      CASE
        WHEN COALESCE(g.sample_count_geq_100, 0) = 0
          THEN CAST([] AS VARCHAR[])
        ELSE g.filtered_sample_ids
      END AS sample_ids
  FROM base b
  LEFT JOIN grouped g USING (min_hash)
)
SELECT *
FROM final
"""
    if drop_empty:
        base_query += "\nWHERE sample_count_geq_100 > 0\n"
    return base_query


def main() -> int:
    ap = argparse.ArgumentParser(description="Filter per-hash sample_ids by matched_hash_count threshold.")
    ap.add_argument("--hashes", required=True, help="Path to filtered_hashes_no_Serratus.parquet")
    ap.add_argument("--counts", required=True, help="Path to sample_counts.parquet")
    ap.add_argument("--out", required=True, help="Path to write the output parquet")
    ap.add_argument("--threshold", type=int, default=100, help="Threshold on matched_hash_count (default: 100)")
    ap.add_argument("--drop-empty", action="store_true",
                    help="Drop rows where no sample_ids meet the threshold (default: keep them with an empty list)")
    args = ap.parse_args()

    hashes = pathlib.Path(args.hashes)
    counts = pathlib.Path(args.counts)
    out = pathlib.Path(args.out)

    if not hashes.exists():
        print(f"ERROR: hashes parquet not found: {hashes}", file=sys.stderr)
        return 2
    if not counts.exists():
        print(f"ERROR: counts parquet not found: {counts}", file=sys.stderr)
        return 2

    sql = build_sql(drop_empty=args.drop_empty)

    con = duckdb.connect()
    # Optional: speed up if you want parallel reads
    con.execute("PRAGMA threads=%d" % os.cpu_count())
    con.execute("PRAGMA enable_progress_bar")

    # Execute parameterized SQL and write to parquet in one go
    # (DuckDB allows binding parameters into read_parquet and the WHERE threshold.)
    con.execute(
        f"COPY ({sql}) TO $out (FORMAT 'parquet')",
        {
            "hashes": str(hashes),
            "sample_counts": str(counts),
            "threshold": args.threshold,
            "out": str(out),
        },
    )

    # Small sanity check message
    res = con.execute(
        "SELECT COUNT(*) AS n_rows, SUM(sample_count_geq_100) AS total_kept_samples FROM read_parquet($out)",
        {"out": str(out)},
    ).fetchone()
    n_rows, total_kept = res
    print(f"Wrote: {out}")
    print(f"Rows: {n_rows:,} | Total kept sample_ids across all rows: {total_kept:,}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
