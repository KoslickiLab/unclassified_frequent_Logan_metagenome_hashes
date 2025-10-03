#!/usr/bin/env env python3
"""
Filter genomic hash data by excluding viral hashes.

This script:
1. Loads viral hashes from a DuckDB database (ksize=31)
2. Reads all sharded parquet files containing hash data
3. Filters out hashes that appear in the viral database
4. Writes the filtered result to a single parquet file
"""

import duckdb
from pathlib import Path
import time


def filter_and_union_hashes(
        parquet_dir: str,
        duckdb_path: str,
        output_path: str,
        ksize: int = 31
) -> None:
    """
    Filter hash data by excluding hashes from DuckDB database.

    Args:
        parquet_dir: Directory containing pairs_lists_shard_*.parquet files
        duckdb_path: Path to DuckDB database with hashes to exclude
        output_path: Path for output parquet file
        ksize: K-mer size to filter from DuckDB (default: 31)
    """
    start_time = time.time()

    # Initialize DuckDB connection
    con = duckdb.connect()

    print(f"Step 1: Loading viral hashes from {duckdb_path} (ksize={ksize})...")

    # Attach the DuckDB database
    con.execute(f"ATTACH '{duckdb_path}' AS viral_db (READ_ONLY)")

    # Load the viral hashes into a temporary table
    # Only select ksize=31 hashes
    con.execute(f"""
        CREATE TEMP TABLE viral_hashes AS
        SELECT hash
        FROM viral_db.unique_hashes
        WHERE ksize = {ksize}
    """)

    viral_hash_count = con.execute("SELECT COUNT(*) FROM viral_hashes").fetchone()[0]
    print(f"   Loaded {viral_hash_count:,} viral hashes")

    print(f"\nStep 2: Reading parquet files from {parquet_dir}...")

    # Find all parquet files
    parquet_pattern = str(Path(parquet_dir) / "pairs_lists_shard_*.parquet")

    # Count input files
    file_count = con.execute(f"""
        SELECT COUNT(*) 
        FROM glob('{parquet_pattern}')
    """).fetchone()[0]
    print(f"   Found {file_count} parquet files")

    # Get total row count before filtering
    total_rows = con.execute(f"""
        SELECT COUNT(*) 
        FROM '{parquet_pattern}'
    """).fetchone()[0]
    print(f"   Total rows before filtering: {total_rows:,}")

    print(f"\nStep 3: Filtering hashes and creating union...")

    # Read all parquet files, filter out viral hashes, and write to output
    # Using LEFT ANTI JOIN for efficient exclusion
    con.execute(f"""
        COPY (
            SELECT 
                p.min_hash,
                p.sample_count,
                p.sample_ids
            FROM '{parquet_pattern}' p
            LEFT ANTI JOIN viral_hashes v
                ON p.min_hash = v.hash
            ORDER BY p.min_hash
        ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    # Get statistics on output
    output_rows = con.execute(f"""
        SELECT COUNT(*) 
        FROM '{output_path}'
    """).fetchone()[0]

    filtered_count = total_rows - output_rows

    print(f"\nStep 4: Complete!")
    print(f"   Rows after filtering: {output_rows:,}")
    print(f"   Rows filtered out: {filtered_count:,} ({filtered_count / total_rows * 100:.2f}%)")
    print(f"   Output written to: {output_path}")

    elapsed = time.time() - start_time
    print(f"   Total time: {elapsed:.2f} seconds")

    con.close()


if __name__ == "__main__":
    # Configuration
    PARQUET_DIR = "/scratch/dmk333_new/unclassified_frequent_Logan_metagenome_hashes/data/hash_and_sample_id_counts/hash_counts_25_10000/hash_pct_25_10000_lists"
    DUCKDB_PATH = "/scratch/dmk333_new/known_microbe_hashes/Serratus_viruses/data/Serratus_viruses_unique_hashes.db"
    OUTPUT_PATH = "/scratch/dmk333_new/filtered_hashes_no_viruses.parquet"

    # Run the filtering
    filter_and_union_hashes(
        parquet_dir=PARQUET_DIR,
        duckdb_path=DUCKDB_PATH,
        output_path=OUTPUT_PATH,
        ksize=31
    )