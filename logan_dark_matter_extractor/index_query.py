"""
Query side for the sharded, sorted index.

We maintain a small LRU cache of np.memmap arrays for shards to limit open FDs.
Each query routes by top bits; membership is via binary search (searchsorted).
"""

from __future__ import annotations
from pathlib import Path
import json
from functools import lru_cache
from typing import Dict, Iterable, List, Tuple
import numpy as np
from .log import get_logger

LOGGER = get_logger("index_query")

class ShardedSortedIndex:
    def __init__(self, index_root: Path):
        self.index_root = index_root
        meta = json.load(open(index_root / "meta.json"))
        self.shard_bits = int(meta["shard_bits"])
        self.shards = int(meta["shards"])
        self.strategy = meta.get("shard_strategy", "topbits")  # default back-compat
        self.shards_dir = index_root / "shards"
        # Precompute masks/constants
        self._mask = np.uint64((1 << self.shard_bits) - 1)
        self._shift = np.uint64(64 - self.shard_bits)

    def _shard_id(self, h: np.ndarray) -> np.ndarray:
        """
        Compute shard id array from uint64 'h', consistent with builder.
        """
        if self.strategy == "lowbits":
            return (h & self._mask).astype(np.int64, copy=False)
        else:
            # legacy (topbits) fallback for old indexes
            return (h >> self._shift).astype(np.int64, copy=False)

    @lru_cache(maxsize=64)  # keep up to 64 shard arrays mmapped
    def _load_shard(self, sid: int) -> np.memmap:
        path = self.shards_dir / f"shard_{sid:03d}.sorted.u64"
        if not path.exists():
            # allow empty shard (return zero-length array)
            LOGGER.warning("Shard %03d not found; treating as empty.", sid)
            return np.memmap(path, dtype=np.uint64, mode="w+", shape=(0,))
        arr = np.memmap(path, dtype=np.uint64, mode="r")
        return arr

    def contains_many(self, hashes: np.ndarray) -> np.ndarray:
        """
        Vectorized membership test for uint64 array 'hashes'.
        Returns a boolean array of same length.
        """
        assert hashes.dtype == np.uint64
        sids = self._shard_id(hashes)
        out = np.zeros(len(hashes), dtype=bool)
        # group by shard id
        order = np.argsort(sids, kind="mergesort")
        sids_sorted = sids[order]
        hs_sorted = hashes[order]
        i = 0
        while i < len(hs_sorted):
            sid = int(sids_sorted[i])
            j = i + 1
            while j < len(hs_sorted) and sids_sorted[j] == sid:
                j += 1
            shard_arr = self._load_shard(sid)
            if shard_arr.size:
                # binary search
                idx = np.searchsorted(shard_arr, hs_sorted[i:j], side="left")
                # guard idx in bounds
                inb = idx < shard_arr.size
                hits = np.zeros(j - i, dtype=bool)
                # compare where in bounds
                hits[inb] = shard_arr[idx[inb]] == hs_sorted[i:j][inb]
                out[order[i:j]] = hits
            # else: all remain False
            i = j
        return out
