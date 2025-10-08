"""
S3 download utilities for Logan public bucket, with careful overwrite prompts.
"""

from pathlib import Path
import subprocess
from .log import get_logger

LOGGER = get_logger("io_aws")

def _confirm_overwrite(path: Path, yes: bool) -> bool:
    if yes:
        return True
    if not path.exists():
        return True
    resp = input(f"[confirm] {path} exists; overwrite? [y/N] ").strip().lower()
    return resp == "y"

def download_contigs(accession: str, out_dir: Path, overwrite: bool = False) -> Path:
    """
    Fetch {accession}.contigs.fa.zst from public Logan S3 bucket.

    Returns path to the downloaded .zst file.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    s3_uri = f"s3://logan-pub/c/{accession}/{accession}.contigs.fa.zst"
    dest = out_dir / f"{accession}.contigs.fa.zst"
    if not _confirm_overwrite(dest, overwrite):
        raise RuntimeError("User declined overwrite.")
    cmd = ["aws", "s3", "cp", s3_uri, str(dest), "--no-sign-request"]
    LOGGER.info("Downloading: %s -> %s", s3_uri, dest)
    subprocess.run(cmd, check=True)
    return dest

def decompress_zstd(zst_path: Path, overwrite: bool = False) -> Path:
    """
    Decompress .zst to .fa using zstd CLI; returns .fa path.
    """
    fa_path = zst_path.with_suffix("")  # drop .zst
    if not _confirm_overwrite(fa_path, overwrite):
        raise RuntimeError("User declined overwrite.")
    LOGGER.info("Decompressing: %s -> %s", zst_path, fa_path)
    subprocess.run(["zstd", "-d", str(zst_path), "-f"], check=True)
    return fa_path
