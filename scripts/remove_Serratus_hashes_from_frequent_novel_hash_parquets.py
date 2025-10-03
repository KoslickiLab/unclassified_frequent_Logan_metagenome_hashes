#!/usr/bin/env python3
import argparse
import os
import sys
import glob
import duckdb
from multiprocessing import cpu_count

def escape_sql_str(s: str) -> str:
    """Escape single quotes for SQL string literals."""
    return s.replace("'", "''")

def quote_ident(ident: str) -> str:
    """Quote an SQL identifier (e.g., table name)."""
    return '"' + ident.replace('"', '""') + '"'

def main():
    ap = argparse.ArgumentParser(
        description="Union sharded Parquet files and exclude rows whose min_hash is present in a DuckDB table (ksize=31 by default)."
    )
    ap.add_argument("--input-dir", required=False,
                    help="Directory containing pairs_lists_shard_*.parquet files.",
                    default="/scratch/dmk333_new/unclassified_frequent_Logan_metagenome_hashes/data/hash_and_sample_id_counts/hash_counts_25_10000/hash_pct_25_10000_lists")
    ap.add_argument("--duckdb-path", required=False,
                    help="Path to Serratus_viruses_unique_hashes.db (DuckDB database).",
                    default="/scratch/dmk333_new/known_microbe_hashes/Serratus_viruses/data/Serratus_viruses_unique_hashes.db")
    ap.add_argument("--output-parquet", required=False,
                    help="Path to write the single output Parquet file.",
                    default="/scratch/dmk333_new/unclassified_frequent_Logan_metagenome_hashes/data"
                            "/hash_and_sample_id_counts/hash_counts_25_10000/filtered_hashes_no_Serratus.parquet")
    ap.add_argument("--ksize", type=int, default=31,
                    help="ksize to filter on in the DuckDB table (default: 31).")
    ap.add_argument("--table", default="unique_hashes",
                    help="Table name inside the DuckDB database (default: unique_hashes).")
    ap.add_argument("--threads", type=int, default=max(1, cpu_count() - 1),
                    help="Number of threads DuckDB can use (default: CPU-1).")
    args = ap.parse_args()

    parquet_glob = os.path.join(args.input_dir, "pairs_lists_shard_*.parquet")
    matches = glob.glob(parquet_glob)
    if not matches:
        print(f"[error] No matching .parquet files found at pattern: {parquet_glob}", file=sys.stderr)
        sys.exit(1)

    out_dir = os.path.dirname(os.path.abspath(args.output_parquet)) or "."
    os.makedirs(out_dir, exist_ok=True)

    con = duckdb.connect(database=":memory:")
    con.execute(f"PRAGMA threads={int(args.threads)}")
    try:
        con.execute("PRAGMA enable_progress_bar=true")
    except Exception:
        pass

    # Attach the Serratus database
    con.execute(f"ATTACH '{escape_sql_str(args.duckdb_path)}' AS serr")

    serr_table = f"serr.{quote_ident(args.table)}"
    ksize_val = int(args.ksize)

    # Sanity check: rows at requested ksize
    count_sql = f"SELECT COUNT(*) FROM {serr_table} WHERE ksize = {ksize_val}"
    (n_ksize_rows,) = con.execute(count_sql).fetchone()
    if n_ksize_rows == 0:
        print(f"[warn] No rows found in {serr_table} with ksize={ksize_val}. Nothing will be excluded.", file=sys.stderr)

    # Create a temp view of hashes to exclude
    create_view_sql = f"""
        CREATE TEMP VIEW serratus_exclude AS
        SELECT CAST(hash AS UBIGINT) AS hash
        FROM {serr_table}
        WHERE ksize = {ksize_val}
    """
    con.execute(create_view_sql)

    # Perform streaming anti-join and write to a single Parquet
    glob_literal = escape_sql_str(parquet_glob)
    out_literal = escape_sql_str(args.output_parquet)
    copy_sql = f"""
        COPY (
            SELECT l.*
            FROM parquet_scan('{glob_literal}', HIVE_PARTITIONING=0) AS l
            ANTI JOIN serratus_exclude s
              ON CAST(l.min_hash AS UBIGINT) = s.hash
        )
        TO '{out_literal}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """
    print(copy_sql)
    con.execute(copy_sql)

    # Optional: confirm output row count
    try:
        (out_rows,) = con.execute(f"SELECT COUNT(*) FROM parquet_scan('{out_literal}')").fetchone()
        print(f"[ok] Wrote {out_rows:,} rows to {args.output_parquet}")
    except Exception:
        print(f"[ok] Wrote Parquet to {args.output_parquet}")

if __name__ == "__main__":
    main()
