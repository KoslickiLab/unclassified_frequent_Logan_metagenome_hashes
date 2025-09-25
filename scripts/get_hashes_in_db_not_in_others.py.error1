#!/usr/bin/env python3
"""
logan_anti_from_manifest_v2.py

- Builds the RIGHT-side union ONCE (with k=31 when k column exists), storing it as a bucketed table.
- Performs a bucketed LEFT ANTI JOIN per bucket against that prebuilt table.
- Adds robust manifest/arg validation, path checks, and subprocess error handling.
- Shows progress: per-phase messages + DuckDB's native progress bar in the terminal.
"""

import argparse, json, os, shutil, subprocess, sys, time
from pathlib import Path
from typing import Dict, Any, List

# --------------------------- utility helpers ---------------------------

def is_power_of_two(n: int) -> bool:
    return n > 0 and (n & (n - 1)) == 0

def ident_ok(s: str) -> bool:
    # conservative check for bare identifiers
    return bool(__import__('re').match(r'^[A-Za-z_][A-Za-z0-9_]*$', s))

def q_str(s: str) -> str:
    # single-quote for SQL string literals
    return "'" + s.replace("'", "''") + "'"

def qualify(alias: str, table: str) -> str:
    # Produces 'alias.table' or 'alias.schema.table' if table is schema-qualified
    # Do not quote identifiers to preserve schema qualification; assume sane names.
    return f"{alias}.{table}"

def icol(name: str) -> str:
    # Column identifier; quote only if necessary
    if ident_ok(name):
        return name
    return '"' + name.replace('"','""') + '"'

def duckdb_cli_available() -> bool:
    try:
        subprocess.run(["duckdb", "-version"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, text=True)
        return True
    except Exception:
        return False

def run_duckdb_sql(dbfile: Path, sql: str, env: Dict[str, str], debug_sql_path: Path, phase: str):
    """
    Run a SQL text in DuckDB CLI so the native progress bar is visible.
    On failure, dumps SQL to debug_sql_path and raises.
    """
    try:
        subprocess.run(["duckdb", str(dbfile)], input=sql, text=True, check=True, env=env)
    except subprocess.CalledProcessError as e:
        try:
            debug_sql_path.write_text(sql)
        except Exception:
            pass
        print(f"\nERROR while executing phase '{phase}'. SQL dumped to: {debug_sql_path}\n", file=sys.stderr)
        raise

def pretty_seconds(sec: float) -> str:
    if sec < 60:
        return f"{sec:.1f}s"
    m = sec / 60.0
    if m < 60:
        return f"{m:.1f}m"
    h = m / 60.0
    return f"{h:.2f}h"

# --------------------------- validation ---------------------------

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
    left_count = 0
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
        if db["set_id"] == "left":
            left_count += 1
        # path exists?
        if not Path(db["path"]).exists():
            print(f"Manifest error: database[{i}] path does not exist: {db['path']}", file=sys.stderr)
            sys.exit(2)

    if left_count != 1:
        print(f"Manifest error: expected exactly one 'left' database, found {left_count}.", file=sys.stderr)
        sys.exit(2)

    return manifest

# --------------------------- core script ---------------------------

def main():
    ap = argparse.ArgumentParser(description="Bucketed anti-join using a manifest. Builds right-side union once (k=31 when available).")
    ap.add_argument("--manifest", required=True, help="Path to DB_info.json")
    ap.add_argument("--work-db", required=True, help="Path to working DuckDB database file")
    ap.add_argument("--tmp-dir", required=True, help="Temp spill directory (fast SSD)")
    ap.add_argument("--threads", type=int, default=128, help="DuckDB PRAGMA threads")
    ap.add_argument("--memory", default="3500GB", help="DuckDB memory_limit (e.g., 3500GB)")
    ap.add_argument("--buckets", type=int, default=256, help="Number of buckets (power of two, e.g., 64/128/256/512/1024)")
    ap.add_argument("--out-dir", default="out_parquet", help="Directory to write Parquet")
    ap.add_argument("--single-file", action="store_true", help="Write one Parquet file instead of partitioned by bucket")
    ap.add_argument("--distinct-left", action="store_true", help="Use DISTINCT on the left per-bucket (if left may have dupes)")
    ap.add_argument("--skip-build-neg", action="store_true", help="Skip rebuilding work.neg_bkt (must already exist)")
    args = ap.parse_args()

    manifest_path = Path(args.manifest)
    work_db = Path(args.work_db)
    tmp_dir = Path(args.tmp_dir)
    out_dir = Path(args.out_dir)

    # Basic environment checks
    if not duckdb_cli_available():
        print("DuckDB CLI ('duckdb') not found in PATH. Please install or add to PATH.", file=sys.stderr)
        sys.exit(2)

    if args.threads <= 0:
        print("--threads must be positive.", file=sys.stderr)
        sys.exit(2)
    if not is_power_of_two(args.buckets) or args.buckets > 65536:
        print("--buckets must be a power of two (<= 65536).", file=sys.stderr)
        sys.exit(2)

    tmp_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    manifest = load_and_validate_manifest(manifest_path)
    dbs: List[Dict[str,Any]] = manifest["databases"]
    left_db = next(d for d in dbs if d["set_id"] == "left")
    right_dbs = [d for d in dbs if d["set_id"] == "right"]

    # Build common strings
    mask = args.buckets - 1
    left_tbl = qualify(left_db["alias"], left_db["table"])
    left_col = icol(left_db["column"])

    # Output naming
    right_aliases = "-".join([d["alias"] for d in right_dbs]) or "none"
    stamp = time.strftime("%Y%m%d_%H%M%S")
    base_name = f"{left_db['alias']}_minus_rights_k31_{stamp}"
    single_path = out_dir / f"{base_name}.parquet"

    # Environment for DuckDB (can add env vars here if needed later)
    env = os.environ.copy()

    # ----------------- Phase 0: Attach & PRAGMAs -----------------
    init_sql = f"""
PRAGMA threads={args.threads};
PRAGMA memory_limit={q_str(args.memory)};
PRAGMA temp_directory={q_str(str(tmp_dir))};
PRAGMA enable_progress_bar=true;
PRAGMA progress_bar_time=1000;

{";".join(f"ATTACH {q_str(d['path'])} AS {d['alias']} (READ_ONLY)" for d in dbs)};

CREATE SCHEMA IF NOT EXISTS work;

-- Output table (results)
CREATE TABLE IF NOT EXISTS work.out (hash BIGINT, bucket INTEGER);

CHECKPOINT;
"""
    print("Initializing DuckDB and attaching databases...")
    run_duckdb_sql(work_db, init_sql, env, out_dir / "debug_init.sql", "init")

    # ----------------- Phase 1: Build RIGHT union ONCE -----------------
    if not args.skip_build_neg:
        print("Building right-side union (k=31 when available) into work.neg_bkt ...")
        t0 = time.time()

        union_parts = []
        for r in right_dbs:
            tbl = qualify(r["alias"], r["table"])
            col = icol(r["column"])
            where = []
            kcol = r.get("ksize")
            if isinstance(kcol, str) and kcol.strip():
                kcol_ident = icol(kcol)
                where.append(f"{kcol_ident} = 31")
            where_clause = (" WHERE " + " AND ".join(where)) if where else ""
            # DISTINCT here to trim per-source duplicates early
            union_parts.append(f"SELECT DISTINCT {col} AS hash FROM {tbl}{where_clause}")

        if not union_parts:
            print("No right-side databases found in manifest.", file=sys.stderr)
            sys.exit(2)

        union_sql = "\n  UNION ALL\n  ".join(union_parts)

        build_neg_sql = f"""
PRAGMA threads={args.threads};
PRAGMA memory_limit={q_str(args.memory)};
PRAGMA temp_directory={q_str(str(tmp_dir))};
PRAGMA enable_progress_bar=true;
PRAGMA progress_bar_time=1000;

-- Create a bucketed, deduplicated RIGHT set once.
-- We also order by (bucket, hash) to enable efficient per-bucket scans.
CREATE OR REPLACE TABLE work.neg_bkt AS
SELECT DISTINCT
  hash::BIGINT AS hash,
  (hash & {mask}) AS bucket
FROM (
  {union_sql}
)
ORDER BY bucket, hash;

CHECKPOINT;
ANALYZE work.neg_bkt;
"""
        run_duckdb_sql(work_db, build_neg_sql, env, out_dir / "debug_build_neg.sql", "build_neg")
        print(f"Right union built in {pretty_seconds(time.time() - t0)}")

    else:
        print("Skipping build of work.neg_bkt (as requested).")

    # ----------------- Phase 2: Bucketed anti-joins -----------------
    print("Running bucketed LEFT ANTI JOIN (left minus rights)...")
    t0 = time.time()
    for b in range(args.buckets):
        t_bucket = time.time()
        left_select = f"SELECT {'DISTINCT ' if args.distinct_left else ''}{left_col} AS hash FROM {left_tbl} WHERE (({left_col} & {mask}) = {b})"
        sql = f"""
PRAGMA threads={args.threads};
PRAGMA memory_limit={q_str(args.memory)};
PRAGMA temp_directory={q_str(str(tmp_dir))};
PRAGMA enable_progress_bar=true;
PRAGMA progress_bar_time=1000;

-- Bucket {b}/{args.buckets-1}
INSERT INTO work.out
SELECT a.hash, {b} AS bucket
FROM ({left_select}) a
LEFT ANTI JOIN (
  SELECT hash FROM work.neg_bkt WHERE bucket = {b}
) n USING (hash);

CHECKPOINT;
"""
        print(f"[{b:03d}/{args.buckets-1}] Processing bucket {b} ...", flush=True)
        run_duckdb_sql(work_db, sql, env, out_dir / f"debug_bucket_{b:03d}.sql", f"bucket_{b}")
        dt = time.time() - t_bucket
        # simple ETA
        done = b + 1
        eta = (time.time() - t0) / done * (args.buckets - done)
        print(f"[{b:03d}/{args.buckets-1}] Done in {pretty_seconds(dt)} | ETA {pretty_seconds(eta)}", flush=True)

    print(f"All buckets completed in {pretty_seconds(time.time() - t0)}")

    # ----------------- Phase 3: Export -----------------
    print("Exporting results to Parquet ...")
    if args.single_file:
        export_sql = f"""
PRAGMA threads={args.threads};
PRAGMA temp_directory={q_str(str(tmp_dir))};
PRAGMA enable_progress_bar=true;
PRAGMA progress_bar_time=1000;

COPY (SELECT hash FROM work.out ORDER BY hash)
TO {q_str(str(single_path))} (FORMAT PARQUET);

CHECKPOINT;
"""
        run_duckdb_sql(work_db, export_sql, env, out_dir / "debug_export.sql", "export_single")
        print(f"Parquet written: {single_path}")
    else:
        export_sql = f"""
PRAGMA threads={args.threads};
PRAGMA temp_directory={q_str(str(tmp_dir))};
PRAGMA enable_progress_bar=true;
PRAGMA progress_bar_time=1000;

COPY (SELECT hash, bucket FROM work.out)
TO {q_str(str(out_dir))} (FORMAT PARQUET, PARTITION_BY (bucket));

CHECKPOINT;
"""
        run_duckdb_sql(work_db, export_sql, env, out_dir / "debug_export.sql", "export_partitioned")
        print(f"Parquet partitioned by bucket in directory: {out_dir}")

    print("Done.")

if __name__ == "__main__":
    main()

