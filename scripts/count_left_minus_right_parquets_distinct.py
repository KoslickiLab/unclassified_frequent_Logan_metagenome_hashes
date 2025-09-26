#!/usr/bin/env python3
"""
Count the number of DISTINCT hashes across Parquet files in bucket=*/ directories.

Usage:
  python distinct_hash_count.py /path/to/logan_metagenomes_minus_rights_k31_...
If no path is given, uses the current directory.
"""

import os
import sys
import duckdb

def main():
    root = sys.argv[1] if len(sys.argv) > 1 else "."
    pattern = os.path.join(os.path.abspath(root), "bucket=*", "*.parquet")

    con = duckdb.connect()
    # Use all cores; optionally allow spilling if needed.
    con.execute(f"PRAGMA threads={os.cpu_count() or 1};")
    # Optional: set a temp directory to spill if this is huge.
    con.execute("PRAGMA temp_directory='/scratch/tmp_duckdb_spill';")
    # Optional: cap memory if desired, e.g., 80%
    con.execute("PRAGMA memory_limit='3.1TB';")

    n = con.execute(
        f"SELECT COUNT(DISTINCT hash) AS n FROM read_parquet('{pattern}')"
    ).fetchone()[0]
    print(n)

if __name__ == "__main__":
    main()

