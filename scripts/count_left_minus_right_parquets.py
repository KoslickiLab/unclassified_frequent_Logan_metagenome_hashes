#!/usr/bin/env python3
"""
Count total number of rows ("hashes") across DuckDB-written Parquet files
organized as bucket=*/data_*.parquet.

Usage:
  python count_parquet_rows.py /path/to/logan_metagenomes_minus_rights_k31_...

If no path is given, defaults to the current directory.
"""

import os
import sys
import glob

def main():
    root = sys.argv[1] if len(sys.argv) > 1 else "."
    pattern = os.path.join(os.path.abspath(root), "bucket=*", "*.parquet")

    # Try fast path: read counts from Parquet metadata (no data load).
    try:
        import pyarrow.parquet as pq
        total = 0
        files = glob.glob(pattern)
        if not files:
            print(f"No Parquet files found matching: {pattern}")
            sys.exit(1)
        for path in files:
            total += pq.ParquetFile(path).metadata.num_rows
        print(total)
        return
    except Exception as e:
        # Fallbacks if pyarrow.parquet isn't available for some reason.
        try:
            import duckdb
            # DuckDB will read metadata to get count()
            q = f"SELECT COUNT(*) FROM read_parquet('{pattern}')"
            total = duckdb.sql(q).fetchone()[0]
            print(total)
            return
        except Exception:
            pass

        # Last resort: pandas (reads the column; slower)
        try:
            import pandas as pd
            total = 0
            for path in glob.glob(pattern):
                # read just the 'hash' column to minimize IO
                total += pd.read_parquet(path, columns=["hash"]).shape[0]
            print(total)
            return
        except Exception as e2:
            print("Failed to count rows via pyarrow, duckdb, or pandas.")
            print("First error:", repr(e))
            print("Second error:", repr(e2))
            sys.exit(2)

if __name__ == "__main__":
    main()

