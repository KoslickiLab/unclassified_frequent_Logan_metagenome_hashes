"""
Command-line interface for building/querying the novel-hash index and annotating contigs.

Examples:

# 1) Build a sharded, sorted index from the 32B-row Parquet:
python -m logan_darkmatter.cli build-index \
  --parquet /scratch/.../hash_counts.parquet \
  --index-root /scratch/logan_darkmatter/novel_index \
  --shard-bits 8 --parallel 8

# 2) For a list of accessions, fetch contigs & signatures and count per-contig:
python -m logan_darkmatter.cli annotate \
  --index-root /scratch/logan_darkmatter/novel_index \
  --workdir /scratch/logan_darkmatter/work \
  --sig-dir /path/to/signatures \
  --accessions ERR10298221 ERR10298244 ...
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List, Optional
from .log import get_logger
from .config import Paths
from .io_aws import download_contigs, decompress_zstd
from .index_builder import split_parquet_to_shards, sort_shards
from .contig_counter import count_novel_hashes_for_accession

LOGGER = get_logger("cli")

def _confirm(msg: str) -> bool:
    return input(f"{msg} [y/N] ").strip().lower() == "y"

def cmd_build_index(args):
    index_root = Path(args.index_root)
    index_root.mkdir(parents=True, exist_ok=True)
    parquet_path = Path(args.parquet)
    #split_parquet_to_shards(parquet_path, index_root, shard_bits=args.shard_bits)
    sort_shards(index_root, parallel=args.parallel)

def cmd_annotate(args):
    index_root = Path(args.index_root)
    workdir = Path(args.workdir)
    sig_dir = Path(args.sig_dir) if args.sig_dir else None
    out_dir = workdir / "results"
    out_dir.mkdir(parents=True, exist_ok=True)

    for acc in args.accessions:
        LOGGER.info("Processing accession: %s", acc)
        zst = download_contigs(acc, workdir / "contigs", overwrite=args.overwrite)
        fasta = decompress_zstd(zst, overwrite=args.overwrite)

        sig_zip = None
        if sig_dir:
            cand = sig_dir / f"{acc}.unitigs.fa.sig.zip"
            if cand.exists():
                sig_zip = cand
            else:
                LOGGER.warning("Signature not found: %s", cand)

        out = count_novel_hashes_for_accession(
            accession=acc,
            fasta_path=fasta,
            index_root=index_root,
            out_dir=out_dir,
            signature_zip=sig_zip
        )
        LOGGER.info("Wrote: %s", out)

def main():
    ap = argparse.ArgumentParser(prog="logan_darkmatter")
    sub = ap.add_subparsers(dest="cmd", required=True)

    ap_build = sub.add_parser("build-index", help="Build sharded, sorted index from a Parquet of min_hash values.")
    ap_build.add_argument("--parquet", required=True, help="Path to hash_counts.parquet (32B rows).")
    ap_build.add_argument("--index-root", required=True, help="Output directory for the index.")
    ap_build.add_argument("--shard-bits", type=int, default=8, help="High-bit partitioning (default 8 → 256 shards).")
    ap_build.add_argument("--parallel", type=int, default=4, help="Parallel shard sorting workers.")
    ap_build.set_defaults(func=cmd_build_index)

    ap_ann = sub.add_parser("annotate", help="Count per-contig novel-hash matches for accessions.")
    ap_ann.add_argument("--index-root", required=True, help="Path to built index root.")
    ap_ann.add_argument("--workdir", required=True, help="Workspace (downloads, intermediate CSVs, results).")
    ap_ann.add_argument("--sig-dir", required=False, help="Directory containing [accession].unitigs.fa.sig.zip.")
    ap_ann.add_argument("--overwrite", action="store_true", help="Overwrite downloads/decompressions if they exist.")
    ap_ann.add_argument("accessions", nargs="+", help="SRA accessions to process.")
    ap_ann.set_defaults(func=cmd_annotate)

    args = ap.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
