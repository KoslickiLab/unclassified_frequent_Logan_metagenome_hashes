"""
Count (per contig) how many k-mers have hash values present in the 'novel' index.

Two modes:
  A) If a signature zip is provided and sourmash CLI is available:
     - use 'sourmash signature kmers' to get (contig, hashval) pairs already
       downsampled by 'scaled=1000', then membership-test each hash.
  B) Fallback: parse FASTA and compute hashes in Python (sourmash API),
     as the sourmash python API allows us to create a FMH sketch on the fly and return k-mers and hashes.

Outputs a Parquet with:
  accession, contig, contig_length, candidate_hashes, novel_hashes, frac_novel
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple, List
from collections import defaultdict

import pandas as pd
import numpy as np
from tqdm import tqdm
from Bio import SeqIO

from .index_query import ShardedSortedIndex
from .sourmash_helpers import have_sourmash_cli, run_sig_kmers, iter_sig_kmers_hashes, iter_hashes_python
from .config import K
from .log import get_logger

LOGGER = get_logger("contig_counter")

@dataclass
class ContigStats:
    contig: str
    length: int
    candidate_hashes: int
    novel_hashes: int

def _write_parquet(rows: List[Dict], out_path: Path):
    df = pd.DataFrame(rows)
    table = df
    out_path.parent.mkdir(parents=True, exist_ok=True)
    table.to_parquet(out_path, compression="zstd", index=False)

def _count_from_kmer_csv(csv_path: Path, index: ShardedSortedIndex) -> Dict[str, ContigStats]:
    # accumulate per-contig
    by_contig_candidates = defaultdict(int)
    by_contig_hits = defaultdict(int)

    # Stream & batch membership checks for throughput.
    # We'll buffer per 1e6 rows (tunable).
    buf_names, buf_hashes = [], []
    BATCH = 1_000_000

    total_rows = 0
    for seqname, h in iter_sig_kmers_hashes(csv_path):
        buf_names.append(seqname)
        buf_hashes.append(np.uint64(h))
        if len(buf_hashes) >= BATCH:
            arr = np.array(buf_hashes, dtype=np.uint64)
            hits = index.contains_many(arr)
            for name, hit in zip(buf_names, hits):
                by_contig_candidates[name] += 1
                if hit:
                    by_contig_hits[name] += 1
            total_rows += len(buf_hashes)
            LOGGER.info("Processed %s k-mer rows...", f"{total_rows:,}")
            buf_names.clear(); buf_hashes.clear()

    if buf_hashes:
        arr = np.array(buf_hashes, dtype=np.uint64)
        hits = index.contains_many(arr)
        for name, hit in zip(buf_names, hits):
            by_contig_candidates[name] += 1
            if hit:
                by_contig_hits[name] += 1

    # We also want contig lengths; read once (stream)
    contig_lengths = {}
    # assume the FASTA used to produce csv_path lives next to it (pass it in if needed)
    fasta_guess = csv_path.with_suffix("").with_suffix(".fasta")
    if not fasta_guess.exists():
        LOGGER.warning("Could not locate FASTA to compute lengths next to %s; lengths set to -1.", csv_path)
    else:
        for rec in SeqIO.parse(str(fasta_guess), "fasta"):
            contig_lengths[rec.id] = len(rec.seq)

    out = {}
    for name, cand in by_contig_candidates.items():
        hits = by_contig_hits.get(name, 0)
        out[name] = ContigStats(contig=name, length=contig_lengths.get(name, -1),
                                candidate_hashes=cand, novel_hashes=hits)
    return out

def _count_from_fasta(fasta_path: Path, index: ShardedSortedIndex) -> Dict[str, ContigStats]:
    out: Dict[str, ContigStats] = {}
    for rec in tqdm(SeqIO.parse(str(fasta_path), "fasta"), desc="hashing contigs"):
        contig = rec.id
        seq = str(rec.seq).upper()
        # streaming hashes from sourmash python API
        hashes = np.fromiter(iter_hashes_python(seq), dtype=np.uint64)
        if hashes.size:
            hits = index.contains_many(hashes)
            out[contig] = ContigStats(
                contig=contig,
                length=len(seq),
                candidate_hashes=int(hashes.size),
                novel_hashes=int(hits.sum()),
            )
        else:
            out[contig] = ContigStats(contig=contig, length=len(seq),
                                      candidate_hashes=0, novel_hashes=0)
    return out

def count_novel_hashes_for_accession(
    accession: str,
    fasta_path: Path,
    index_root: Path,
    out_dir: Path,
    signature_zip: Optional[Path] = None,
) -> Path:
    """
    Main entry: produce a Parquet for one accession with columns:
      accession, contig, contig_length, candidate_hashes, novel_hashes, frac_novel

    If 'signature_zip' is given and sourmash CLI is available, use it to extract k-mers.
    """
    index = ShardedSortedIndex(index_root)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{accession}.novel_counts.parquet"

    if signature_zip and have_sourmash_cli():
        csv_path = out_dir / f"{accession}.kmer-matches.csv"
        run_sig_kmers(signature_zip, fasta_path, csv_path, overwrite=False)
        stats = _count_from_kmer_csv(csv_path, index)
    else:
        LOGGER.warning("Signature zip not provided or sourmash not found; falling back to Python hashing.")
        stats = _count_from_fasta(fasta_path, index)

    rows = []
    for contig, st in stats.items():
        frac = (st.novel_hashes / st.candidate_hashes) if st.candidate_hashes else 0.0
        rows.append({
            "accession": accession,
            "contig": contig,
            "contig_length": st.length,
            "candidate_hashes": st.candidate_hashes,
            "novel_hashes": st.novel_hashes,
            "frac_novel": frac,
        })
    _write_parquet(rows, out_path)
    return out_path
