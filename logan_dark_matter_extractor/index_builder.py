"""
Build a *sharded, sorted, disk-backed* u64 index from a Parquet column of int64
(min_hash) values. Designed for 32B+ rows.

Two phases:
  1) split_parquet_to_shards: stream and write 8-byte little-endian uint64 to
     shard files by top SHARD_BITS of hash.
  2) sort_shards: in-place sort per-shard to enable fast binary search.

The resulting index looks like:

  index_root/
    meta.json
    shards/
      shard_000.bin        # unsorted (kept for restart; can be deleted after sort)
      shard_000.sorted.u64 # sorted uint64 values (little endian)
      shard_001.sorted.u64
      ...
"""

from __future__ import annotations

import json, os
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List
import numpy as np
import pyarrow.parquet as pq
from tqdm import tqdm
from .config import SHARD_BITS_DEFAULT
from .log import get_logger

LOGGER = get_logger("index_builder")

@dataclass
class IndexMeta:
    parquet_path: str
    total_rows: int
    shard_bits: int
    shards: int
    dtype: str = "uint64"
    version: int = 1

def _ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def split_parquet_to_shards(parquet_path: Path,
                            index_root: Path,
                            shard_bits: int = SHARD_BITS_DEFAULT,
                            rowgroup_batch: int = 1 << 14):
    """
    Stream the Parquet 'min_hash' column and write to shard files.

    rowgroup_batch controls how many rows are pulled per batch when reading
    arrow RecordBatches (NOT the Parquet row group size).
    """
    shards_dir = index_root / "shards"
    _ensure_dir(shards_dir)

    pf = pq.ParquetFile(parquet_path)
    total = pf.metadata.num_rows
    shards = 1 << shard_bits
    LOGGER.info("Splitting %s rows into %s shards (%d bits).",
                f"{total:,}", shards, shard_bits)

    # Open all shard files; keep open to reduce overhead (256 FDs default)
    fhs = [open(shards_dir / f"shard_{i:03d}.bin", "ab", buffering=1024*1024) for i in range(shards)]
    counts = np.zeros(shards, dtype=np.int64)

    try:
        for rg_idx in range(pf.num_row_groups):
            table = pf.read_row_groups([rg_idx], columns=["min_hash"])
            col = table.column(0).to_numpy(zero_copy_only=False)  # int64
            # cast to uint64 safely
            u = col.astype(np.uint64, copy=False)

            # shard id = top bits of 64-bit value
            shift = np.uint64(64 - shard_bits)
            shard_ids = (u >> shift).astype(np.int64, copy=False)

            # Group by shard id and write contiguous blocks
            # We sort shard_ids and u jointly by shard to write in fewer syscalls.
            order = np.argsort(shard_ids, kind="mergesort")
            shard_ids_sorted = shard_ids[order]
            u_sorted = u[order]

            i = 0
            while i < len(u_sorted):
                sid = int(shard_ids_sorted[i])
                j = i + 1
                # advance j while shard id is the same
                while j < len(u_sorted) and shard_ids_sorted[j] == sid:
                    j += 1
                block = u_sorted[i:j].tobytes()  # little-endian platform default
                fhs[sid].write(block)
                counts[sid] += (j - i)
                i = j

            # progress
            LOGGER.info("Row group %d/%d done. Total so far: %s",
                        rg_idx + 1, pf.num_row_groups, f"{int(counts.sum()):,}")
    finally:
        for fh in fhs:
            fh.close()

    meta = IndexMeta(
        parquet_path=str(parquet_path),
        total_rows=int(counts.sum()),
        shard_bits=shard_bits,
        shards=shards,
    )
    with open(index_root / "meta.json", "w") as f:
        json.dump(asdict(meta), f, indent=2)

    # Save per-shard counts for logging/restarts
    np.save(index_root / "counts.npy", counts)
    LOGGER.info("Split complete. Wrote %s total items into %d shards.",
                f"{int(counts.sum()):,}", shards)

def _sort_one_shard(args):
    """
    Top-level worker (picklable) for ProcessPoolExecutor.
    Args:
      args: tuple (i:int, shards_dir:str)
    Returns:
      (i, n_items, status)
    """
    i, shards_dir_str = args
    shards_dir = Path(shards_dir_str)
    src = shards_dir / f"shard_{i:03d}.bin"
    dst = shards_dir / f"shard_{i:03d}.sorted.u64"
    if not src.exists():
        return (i, 0, "missing")
    if dst.exists():
        return (i, os.path.getsize(dst) // 8, "exists")
    # read all, sort, write
    arr = np.fromfile(src, dtype=np.uint64)
    if arr.size == 0:
        # create an empty sorted file to mark completion
        arr.tofile(dst)
        return (i, 0, "empty")
    arr.sort(kind="quicksort")
    arr.tofile(dst)
    return (i, int(arr.size), "sorted")

def sort_shards(index_root: Path, parallel: int = 4):
    """
    Sort each shard file into a *.sorted.u64 file, optionally in parallel.

    For a 32B-row dataset with 256 shards, each shard averages ~1GB (125M u64).
    Sorting that many 64-bit ints in RAM is feasible and very fast on your machine.
    """
    shards_dir = index_root / "shards"
    meta = json.load(open(index_root / "meta.json"))
    shards = int(meta["shards"])

    tasks = [(i, str(shards_dir)) for i in range(shards)]
    if parallel <= 1:
        # single-process fallback (no multiprocessing/pickling)
        for t in tqdm(tasks, total=shards, desc="sorting shards"):
            i, n, status = _sort_one_shard(t)
            LOGGER.info("Shard %03d: %s (%s items)", i, status, f"{n:,}")
