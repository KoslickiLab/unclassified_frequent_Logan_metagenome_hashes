"""
Sanity test: our approach should produce hash values identical to sourmash.

This uses sourmash API & CLI and verifies consistent (k-mer, hashval) pairs from:
  - sourmash.MinHash(...).kmers_and_hashes(force=True)
  - 'sourmash signature kmers' CSV output

Run with: pytest -q

Note: Provide a small FASTA & a signature built with '-p dna,k=31,scaled=1000,noabund'.
"""

from pathlib import Path
import subprocess
import sourmash
from sourmash import MinHash
from logan_darkmatter.sourmash_helpers import run_sig_kmers

def test_python_vs_cli(tmp_path: Path):
    # Build a tiny FASTA and signature on-the-fly
    fasta = tmp_path / "tiny.fa"
    fasta.write_text(">c1\nACGT" * 100 + "\n")

    sig = tmp_path / "tiny.sig"
    subprocess.run(["sourmash", "sketch", "dna", str(fasta), "-p", "k=31,scaled=1000,noabund", "-o", str(sig)],
                   check=True)

    csv = tmp_path / "kmers.csv"
    run_sig_kmers(sig, fasta, csv, overwrite=True)

    # load CLI hashes
    cli_hashes = set()
    import csv as _csv
    with csv.open(csv, "r") as fh:
        rdr = _csv.DictReader(fh)
        for row in rdr:
            cli_hashes.add(int(row["hashval"]))

    # load Python hashes
    mh = MinHash(n=0, ksize=31, scaled=1)
    py_hashes = set()
    for kmer, h in mh.kmers_and_hashes(fasta.read_text().splitlines()[1], force=True):
        if h < (1 << 64) // 1000:
            py_hashes.add(h)

    # they shouldn't be identical sets because CLI emits only those present in signature,
    # but python-side set must be a superset, and all CLI hashes must be in python set.
    assert cli_hashes.issubset(py_hashes)

    # for the fun of it, check if they are identical in terms of sets (having the same elements)
    assert cli_hashes == py_hashes

