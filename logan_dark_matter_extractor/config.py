"""
Global configuration and constants for the Logan microbial-dark-matter pipeline.
"""

from dataclasses import dataclass
from pathlib import Path

# sourmash parameters you used
K = 31
SCALED = 1000
HASH_SEED = 42  # sourmash default

# For FracMinHash, keep hashes strictly less than this threshold:
MAX_HASH = (1 << 64) // SCALED  # floor(2**64 / scaled)

# Sharding: number of high bits to route a 64-bit hash into shard files
# 8 bits -> 256 shards; average ~1 GB shard for 32B total hashes
SHARD_BITS_DEFAULT = 8

# IO defaults
ZSTD_LEVEL = 19

@dataclass
class Paths:
    """Bundle of project paths, adjustable on CLI."""
    scratch: Path
    # directories will be created as needed
    index_root: Path
    tmp_root: Path
    results_root: Path

    @classmethod
    def from_root(cls, scratch="/scratch/logan_darkmatter"):
        root = Path(scratch)
        return cls(
            scratch=root,
            index_root=root / "novel_index",
            tmp_root=root / "tmp",
            results_root=root / "results",
        )
