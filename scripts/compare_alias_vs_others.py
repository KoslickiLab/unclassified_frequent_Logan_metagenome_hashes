#!/usr/bin/env python3
"""
Compare an arbitrary RIGHT-set database (by alias) against the union of all other RIGHT-set databases.

Counts reported (distinct hashes):
  1) |TARGET \ OTHERS|
  2) |TARGET ∩ OTHERS|
  3) |OTHERS \ TARGET|

Notes
- Assumes each *individual* source database already stores DISTINCT hashes.
  We do NOT add DISTINCT per-source; we only DISTINCT across the UNION that
  forms OTHERS.
- Completely ignores any databases with "set_id": "left".
- If a k-size column is present in the manifest entry (e.g., key "ksize"),
  we filter to k=31 when reading that source.
- Uses bucketing on (hash & (buckets-1)) for partition-local joins.
- Attaches all sources READ_ONLY; writes *only* to the provided --work-db
  to create ephemeral tables: work.target_bkt and work.others_bkt.

Example
  python compare_alias_vs_others.py \
    --manifest /path/DB_info.json \
    --target-alias logan_obelisks \
    --work-db /path/work.db \
    --tmp-dir /fast/tmp \
    --threads 128 --memory 3000GB --buckets 256
"""

import argparse, json, math, os, subprocess, sys, time
from pathlib import Path
from typing import Dict, Any, List

# -------- helpers --------

def is_power_of_two(n: int) -> bool:
    return n > 0 and (n & (n - 1)) == 0

def ident_ok(s: str) -> bool:
    import re
    return bool(re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', s))

def q_str(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"

def q_ident(s: str) -> str:
    return s if ident_ok(s) else '"' + s.replace('"', '""') + '"'

def qualify3(alias: str, table: str) -> str:
    parts = table.split('.')
    if len(parts) == 1:
        schema, tbl = 'main', parts[0]
    else:
        schema, tbl = parts[-2], parts[-1]
    return f"{alias}.{q_ident(schema)}.{q_ident(tbl)}"

def icol(name: str) -> str:
    return name if ident_ok(name) else '"' + name.replace('"','""') + '"'

def duckdb_cli_available() -> bool:
    try:
        subprocess.run(["duckdb", "-version"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, text=True)
        return True
    except Exception:
        return False

def run_duckdb_sql(dbfile: Path, sql: str, env: Dict[str, str], debug_sql_path: Path, phase: str):
    try:
        subprocess.run(["duckdb", str(dbfile)], input=sql, text=True, check=True, env=env)
    except subprocess.CalledProcessError:
        try:
            debug_sql_path.write_text(sql)
        except Exception:
            pass
        print(f"\nERROR during phase '{phase}'. SQL dumped to: {debug_sql_path}\n", file=sys.stderr)
        raise

def pretty_seconds(sec: float) -> str:
    if sec < 60: return f"{sec:.1f}s"
    m = sec / 60.0
    if m < 60: return f"{m:.1f}m"
    h = m / 60.0
    return f"{h:.2f}h"

def compute_sampling_bits(fraction: float) -> (int, int):
    if not (0 < fraction <= 1.0):
        raise ValueError("--sample-fraction must be in (0, 1].")
    if fraction >= 1.0:
        return (0, 0)
    for bits in range(1, 31):
        denom = 1 << bits
        thr = int(round(fraction * denom))
        if thr <= 0: thr = 1
        if thr < denom:
            return (bits, thr)
    return (30, (1 << 30) - 1)

REQUIRED_DB_KEYS = {"alias", "path", "table", "column", "set_id"}

def load_and_validate_manifest(path: Path) -> Dict[str, Any]:
    try:
        manifest = json.loads(path.read_text())
    except Exception as e:
        print(f"Failed to read/parse manifest JSON at {path}: {e}", file=sys.stderr)
        sys.exit(2)

    if not isinstance(manifest, dict) or "databases" not in manifest or not isinstance(manifest["databases"], list):
        print("Manifest must be an object with a 'databases' array.", file=sys.stderr)
        sys.exit(2)

    dbs = manifest["databases"]
    aliases = set()
    for i, db in enumerate(dbs):
        missing = REQUIRED_DB_KEYS - set(db.keys())
        if missing:
            print(f"Manifest error: database[{i}] missing keys: {sorted(missing)}", file=sys.stderr)
            sys.exit(2)
        for k in ("alias","path","table","column","set_id"):
            if not isinstance(db[k], str) or not db[k]:
                print(f"Manifest error: database[{i}].{k} must be a non-empty string.", file=sys.stderr)
                sys.exit(2)
        if db["set_id"] not in ("left","right"):
            print(f"Manifest error: database[{i}].set_id must be 'left' or 'right'.", file=sys.stderr)
            sys.exit(2)
        if db["alias"] in aliases:
            print(f"Manifest error: duplicate alias '{db['alias']}'", file=sys.stderr)
            sys.exit(2)
        aliases.add(db["alias"])
        if not Path(db["path"]).exists():
            print(f"Manifest error: database[{i}] path does not exist: {db['path']}", file=sys.stderr)
            sys.exit(2)

    return manifest

# -------- main --------

def main():
    ap = argparse.ArgumentParser(description="Counts: TARGET alias vs union of other RIGHT databases (k=31 when available). Distinct-at-source assumed.")
    ap.add_argument("--manifest", required=True, help="Path to DB_info.json")
    ap.add_argument("--target-alias", required=True, help="Alias of the RIGHT-set database to compare against all other RIGHT-set databases")
    ap.add_argument("--work-db", required=True, help="Path to working DuckDB database file")
    ap.add_argument("--tmp-dir", required=True, help="Temp spill directory (fast SSD)")
    ap.add_argument("--threads", type=int, default=128, help="DuckDB PRAGMA threads")
    ap.add_argument("--memory", default="3500GB", help="DuckDB memory_limit, e.g. 3500GB")
    ap.add_argument("--buckets", type=int, default=256, help="Power-of-two number of buckets (e.g., 256)")
    ap.add_argument("--sample-fraction", type=float, default=1.0, help="Deterministic fraction (0<f<=1). Uses higher bits to avoid correlation with bucket.")
    ap.add_argument("--log-dir", default=None, help="Directory for debug SQL logs (defaults to <work-db dir>/_logs)")
    args = ap.parse_args()

    manifest_path = Path(args.manifest)
    work_db = Path(args.work_db)
    tmp_dir = Path(args.tmp_dir)
    log_dir = Path(args.log_dir) if args.log_dir else (work_db.parent / "_logs")
    log_dir.mkdir(parents=True, exist_ok=True)

    if not duckdb_cli_available():
        print("DuckDB CLI ('duckdb') not found in PATH.", file=sys.stderr); sys.exit(2)
    if args.threads <= 0:
        print("--threads must be positive.", file=sys.stderr); sys.exit(2)
    if not is_power_of_two(args.buckets) or args.buckets > 65536:
        print("--buckets must be a power of two (<= 65536).", file=sys.stderr); sys.exit(2)
    if not (0 < args.sample_fraction <= 1.0):
        print("--sample-fraction must be in (0, 1].", file=sys.stderr); sys.exit(2)

    tmp_dir.mkdir(parents=True, exist_ok=True)

    manifest = load_and_validate_manifest(manifest_path)
    dbs: List[Dict[str,Any]] = manifest["databases"]

    # Identify TARGET and OTHERS (RIGHT-only); completely ignore LEFT
    target_db = None
    right_dbs = []
    for d in dbs:
        if d["set_id"] != "right":
            continue
        if d["alias"] == args.target_alias:
            target_db = d
        else:
            right_dbs.append(d)

    if target_db is None:
        print(f"Target alias '{args.target_alias}' not found in RIGHT-set databases.", file=sys.stderr)
        sys.exit(2)

    mask = args.buckets - 1
    bucket_bits = int(math.log2(args.buckets))

    def sampling_clause(col_ident: str) -> str:
        if args.sample_fraction >= 1.0: return ""
        sample_bits, sample_thr = compute_sampling_bits(args.sample_fraction)
        return f"(({col_ident} >> {bucket_bits}) & {(1<<sample_bits)-1}) < {sample_thr}"

    pragmas_sql = f"""
PRAGMA threads={args.threads};
PRAGMA memory_limit={q_str(args.memory)};
PRAGMA temp_directory={q_str(str(tmp_dir))};
PRAGMA enable_progress_bar=true;
PRAGMA progress_bar_time=1000;
""".strip()

    attach_sql = ";\n".join(f"ATTACH {q_str(d['path'])} AS {d['alias']} (READ_ONLY)" for d in dbs) + ";"

    init_sql = f"""
{pragmas_sql}
{attach_sql}

CREATE SCHEMA IF NOT EXISTS work;
DROP TABLE IF EXISTS work.target_bkt;
DROP TABLE IF EXISTS work.others_bkt;
CHECKPOINT;
"""
    run_duckdb_sql(work_db, init_sql, os.environ.copy(), log_dir / "init.sql", "init")

    # Phase 1: TARGET set (per-db is already distinct -> no DISTINCT here)
    t_tbl = qualify3(target_db["alias"], target_db["table"])
    t_col = icol(target_db["column"])
    t_ksz = target_db.get("ksize")
    t_where = []
    if isinstance(t_ksz, str) and t_ksz.strip():
        t_where.append(f"{icol(t_ksz)} = 31")
    t_samp = sampling_clause(t_col)
    if t_samp:
        t_where.append(t_samp)
    t_where_sql = (" WHERE " + " AND ".join(t_where)) if t_where else ""

    build_target_sql = f"""
{pragmas_sql}
{attach_sql}

CREATE OR REPLACE TABLE work.target_bkt AS
SELECT
  {t_col}::UBIGINT AS hash,
  ({t_col} & {mask}) AS bucket
FROM {t_tbl}{t_where_sql}
ORDER BY bucket, hash;

CHECKPOINT;
ANALYZE work.target_bkt;
"""
    t0 = time.time()
    print("Building TARGET set...")
    run_duckdb_sql(work_db, build_target_sql, os.environ.copy(), log_dir / "target_build.sql", "build_target")
    print(f"TARGET built in {pretty_seconds(time.time() - t0)}")

    # Phase 2: OTHERS union (each per-db already distinct; we still need DISTINCT across the UNION)
    union_parts = []
    for r in right_dbs:
        tbl = qualify3(r["alias"], r["table"])
        col = icol(r["column"])
        where_clauses = []
        kcol = r.get("ksize")
        if isinstance(kcol, str) and kcol.strip():
            where_clauses.append(f"{icol(kcol)} = 31")
        samp = sampling_clause(col)
        if samp:
            where_clauses.append(samp)
        where_clause = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
        union_parts.append(f"SELECT {col}::UBIGINT AS hash FROM {tbl}{where_clause}")

    union_sql = "\n  UNION ALL\n  ".join(union_parts) if union_parts else "SELECT NULL WHERE FALSE"  # empty guard

    build_others_sql = f"""
{pragmas_sql}
{attach_sql}

CREATE OR REPLACE TABLE work.others_bkt AS
SELECT DISTINCT
  hash::UBIGINT AS hash,
  (hash & {mask}) AS bucket
FROM (
  {union_sql}
)
ORDER BY bucket, hash;

CHECKPOINT;
ANALYZE work.others_bkt;
"""
    t1 = time.time()
    print("Building OTHERS set...")
    run_duckdb_sql(work_db, build_others_sql, os.environ.copy(), log_dir / "others_build.sql", "build_others")
    print(f"OTHERS built in {pretty_seconds(time.time() - t1)}")

    # Phase 3: counts
    counts_core = f"""
WITH
w AS (SELECT hash, bucket FROM work.target_bkt),
o AS (SELECT hash, bucket FROM work.others_bkt)
SELECT
  (SELECT COUNT(*) FROM w LEFT JOIN o ON w.bucket=o.bucket AND w.hash=o.hash WHERE o.hash IS NULL) AS target_only,
  (SELECT COUNT(*) FROM w INNER JOIN o ON w.bucket=o.bucket AND w.hash=o.hash) AS intersection,
  (SELECT COUNT(*) FROM o LEFT JOIN w ON w.bucket=o.bucket AND w.hash=o.hash WHERE w.hash IS NULL) AS others_only,
  (SELECT COUNT(*) FROM w) AS target_total,
  (SELECT COUNT(*) FROM o) AS others_total,
  (SELECT (SELECT COUNT(*) FROM w) = (SELECT COUNT(*) FROM w LEFT JOIN o ON w.bucket=o.bucket AND w.hash=o.hash WHERE o.hash IS NULL) + (SELECT COUNT(*) FROM w INNER JOIN o ON w.bucket=o.bucket AND w.hash=o.hash)) AS sanity_target,
  (SELECT (SELECT COUNT(*) FROM o) = (SELECT COUNT(*) FROM o LEFT JOIN w ON w.bucket=o.bucket AND w.hash=o.hash WHERE w.hash IS NULL) + (SELECT COUNT(*) FROM w INNER JOIN o ON w.bucket=o.bucket AND w.hash=o.hash)) AS sanity_others
"""

    env = os.environ.copy()
    out_path = log_dir / f"{args.target_alias}_vs_others_counts.csv"
    copy_sql = f"""
{pragmas_sql}
{attach_sql}
COPY (
{counts_core}
) TO {q_str(str(out_path))} WITH (HEADER, DELIMITER ',');
"""
    run_duckdb_sql(work_db, copy_sql, env, log_dir / "counts.sql", "counts")

    if not out_path.exists():
        print("No counts produced.", file=sys.stderr)
        sys.exit(2)

    import csv, json as _json
    row = None
    with out_path.open() as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            row = r
            break
    if row is None:
        print("No counts produced.", file=sys.stderr)
        sys.exit(2)

    def fmt(n): return f"{int(n):,}"

    print("\n================  {} vs Union(Other RIGHT DBs)  ================\n".format(args.target_alias))
    print("{:42} {:>20}".format("Relationship", "Count"))
    print("-" * 66)
    print("{:42} {:>20}".format("TARGET \\ OTHERS", fmt(row['target_only'])))
    print("{:42} {:>20}".format("TARGET ∩ OTHERS", fmt(row['intersection'])))
    print("{:42} {:>20}".format("OTHERS \\ TARGET", fmt(row['others_only'])))
    print("-" * 66)
    print("{:42} {:>20}   {}".format("|TARGET| (sanity check)", fmt(row['target_total']), "(ok)" if str(row.get('sanity_target','true')).lower()=="true" else "(mismatch!)"))
    print("{:42} {:>20}   {}".format("|OTHERS| (sanity check)", fmt(row['others_total']), "(ok)" if str(row.get('sanity_others','true')).lower()=="true" else "(mismatch!)"))

    print("\nJSON:\n" + _json.dumps({
        "target_alias": args.target_alias,
        "target_only": int(row["target_only"]),
        "intersection": int(row["intersection"]),
        "others_only": int(row["others_only"]),
        "target_total": int(row["target_total"]),
        "others_total": int(row["others_total"]),
        "sanity_target": str(row.get("sanity_target","true")).lower()=="true",
        "sanity_others": str(row.get("sanity_others","true")).lower()=="true",
        "sample_fraction": args.sample_fraction,
        "buckets": args.buckets,
        "threads": args.threads,
        "memory": args.memory
    }, indent=2))
    print()

if __name__ == "__main__":
    main()

