#!/usr/bin/env python3
"""
Comprehensive benchmark suite for Distributed SQL Database.
Generates SQL workloads, runs them across all configurations,
parses timing output, and stores results as CSV for graphing.

Compares three implementations:
  1. Single-node SQLite (in-memory, with transactions)
  2. Single-node SQLite (file-based, with transactions)
  3. Distributed MPI (with varying worker counts)

Usage:
  python tests/run_benchmarks.py [--build-dir BUILD] [--runs N] [--sizes S1,S2,...] [--workers W1,W2,...]

Output:
  benchmark_results/results.csv          — raw timing data
  benchmark_results/summary.csv          — aggregated (mean/min/max/stddev)
  benchmark_results/sql/                 — generated SQL files
"""

import argparse
import csv
import os
import re
import subprocess
import sys
import time
import statistics
from datetime import datetime
from pathlib import Path

# ── Defaults ──────────────────────────────────────────────────────────────────

DEFAULT_BUILD_DIR = "build"
DEFAULT_RUNS = 3
DEFAULT_SIZES = [100, 500, 1000, 5000, 10000]
DEFAULT_WORKERS = [2, 3, 4, 6, 8]

NAMES = [
    "Alice","Bob","Carol","Dave","Eve","Frank","Grace","Hank","Ivy","Jack",
    "Karen","Leo","Mia","Ned","Olivia","Paul","Quinn","Rose","Sam","Tina",
    "Uma","Victor","Wendy","Xander","Yara","Zoe","Aaron","Bella","Chris","Diana"
]
DEPTS = ["Engineering","Marketing","HR","Sales","Finance"]

# ── SQL Generation ────────────────────────────────────────────────────────────

def gen_insert_rows(n):
    """Generate N INSERT statements."""
    rows = []
    for i in range(1, n + 1):
        name = NAMES[i % len(NAMES)]
        dept = DEPTS[i % len(DEPTS)]
        salary = 50000 + (i * 137) % 100000
        rows.append(
            f"INSERT INTO employees VALUES ({i}, '{name}_{i}', '{dept}', {salary})"
        )
    return rows


def gen_sql_insert_only(n, distributed=True):
    """Benchmark: INSERT batch of N rows."""
    lines = []
    if distributed:
        lines += ["CREATE DATABASE bench;", "USE bench;"]
    lines.append("CREATE TABLE employees "
                 "(id INTEGER PRIMARY KEY, name TEXT, department TEXT, salary REAL);")
    lines.append("")
    lines.append(f"-- {n} inserts")
    lines += [r + ";" for r in gen_insert_rows(n)]
    lines.append("")
    if distributed:
        lines += ["DROP TABLE employees;", "DROP DATABASE bench;"]
    else:
        lines += ["DROP TABLE employees;"]
    return "\n".join(lines)


def gen_sql_select_targeted(n, num_queries=50, distributed=True):
    """Benchmark: targeted SELECT (WHERE id = X) on a table with N rows."""
    lines = []
    if distributed:
        lines += ["CREATE DATABASE bench;", "USE bench;"]
    lines.append("CREATE TABLE employees "
                 "(id INTEGER PRIMARY KEY, name TEXT, department TEXT, salary REAL);")
    lines.append("")
    lines += [r + ";" for r in gen_insert_rows(n)]
    lines.append("")
    lines.append(f"-- {num_queries} targeted selects")
    # Spread queries evenly across the ID space
    for i in range(num_queries):
        target_id = (i * n // num_queries) + 1
        lines.append(f"SELECT * FROM employees WHERE id = {target_id};")
    lines.append("")
    if distributed:
        lines += ["DROP TABLE employees;", "DROP DATABASE bench;"]
    else:
        lines += ["DROP TABLE employees;"]
    return "\n".join(lines)


def gen_sql_select_full_scan(n, num_scans=5, distributed=True):
    """Benchmark: full table scan on a table with N rows."""
    lines = []
    if distributed:
        lines += ["CREATE DATABASE bench;", "USE bench;"]
    lines.append("CREATE TABLE employees "
                 "(id INTEGER PRIMARY KEY, name TEXT, department TEXT, salary REAL);")
    lines.append("")
    lines += [r + ";" for r in gen_insert_rows(n)]
    lines.append("")
    lines.append(f"-- {num_scans} full scans")
    for _ in range(num_scans):
        lines.append("SELECT * FROM employees;")
    lines.append("")
    if distributed:
        lines += ["DROP TABLE employees;", "DROP DATABASE bench;"]
    else:
        lines += ["DROP TABLE employees;"]
    return "\n".join(lines)


def gen_sql_update_broadcast(n, num_updates=10, distributed=True):
    """Benchmark: broadcast UPDATE (no shard key) on a table with N rows."""
    lines = []
    if distributed:
        lines += ["CREATE DATABASE bench;", "USE bench;"]
    lines.append("CREATE TABLE employees "
                 "(id INTEGER PRIMARY KEY, name TEXT, department TEXT, salary REAL);")
    lines.append("")
    lines += [r + ";" for r in gen_insert_rows(n)]
    lines.append("")
    lines.append(f"-- {num_updates} broadcast updates")
    for i in range(num_updates):
        dept = DEPTS[i % len(DEPTS)]
        lines.append(
            f"UPDATE employees SET salary = salary * 1.01 WHERE department = '{dept}';"
        )
    lines.append("")
    if distributed:
        lines += ["DROP TABLE employees;", "DROP DATABASE bench;"]
    else:
        lines += ["DROP TABLE employees;"]
    return "\n".join(lines)


def gen_sql_delete_targeted(n, num_deletes=50, distributed=True):
    """Benchmark: targeted DELETE (WHERE id = X) on a table with N rows."""
    lines = []
    if distributed:
        lines += ["CREATE DATABASE bench;", "USE bench;"]
    lines.append("CREATE TABLE employees "
                 "(id INTEGER PRIMARY KEY, name TEXT, department TEXT, salary REAL);")
    lines.append("")
    lines += [r + ";" for r in gen_insert_rows(n)]
    lines.append("")
    lines.append(f"-- {num_deletes} targeted deletes")
    # Delete from end to avoid gaps affecting later deletes
    for i in range(num_deletes):
        target_id = n - i
        if target_id < 1:
            break
        lines.append(f"DELETE FROM employees WHERE id = {target_id};")
    lines.append("")
    if distributed:
        lines += ["DROP TABLE employees;", "DROP DATABASE bench;"]
    else:
        lines += ["DROP TABLE employees;"]
    return "\n".join(lines)


def gen_sql_mixed(n, distributed=True):
    """Benchmark: mixed workload (the classic benchmark from gen_benchmark)."""
    lines = []
    if distributed:
        lines += ["CREATE DATABASE bench;", "USE bench;"]
    lines.append("CREATE TABLE employees "
                 "(id INTEGER PRIMARY KEY, name TEXT, department TEXT, salary REAL);")
    lines.append("")
    lines.append(f"-- {n} inserts")
    lines += [r + ";" for r in gen_insert_rows(n)]
    lines.append("")

    mid = n // 2
    lines.append("-- Targeted selects")
    for tid in [1, mid, n]:
        lines.append(f"SELECT * FROM employees WHERE id = {tid};")
    lines.append("")

    lines.append("-- Full scan")
    lines.append("SELECT * FROM employees;")
    lines.append("")

    lines.append("-- Broadcast update")
    lines.append("UPDATE employees SET salary = salary * 1.10 WHERE department = 'Engineering';")
    lines.append("")

    lines.append("-- Targeted deletes")
    for tid in [1, mid, n]:
        lines.append(f"DELETE FROM employees WHERE id = {tid};")
    lines.append("")

    lines.append("-- Final scan")
    lines.append("SELECT * FROM employees;")
    lines.append("")

    if distributed:
        lines += ["DROP TABLE employees;", "DROP DATABASE bench;"]
    else:
        lines += ["DROP TABLE employees;"]
    return "\n".join(lines)


# Map of operation name -> (generator_func, is_per_op_count_variable)
OPERATIONS = {
    "insert_batch":      (gen_sql_insert_only,       False),
    "select_targeted":   (gen_sql_select_targeted,    False),
    "select_full_scan":  (gen_sql_select_full_scan,   False),
    "update_broadcast":  (gen_sql_update_broadcast,   False),
    "delete_targeted":   (gen_sql_delete_targeted,    False),
    "mixed":             (gen_sql_mixed,              False),
}

# ── Output Parsing ────────────────────────────────────────────────────────────

def parse_timings(output):
    """
    Parse benchmark output to extract per-statement timings.
    Returns list of (stmt_num, description, time_ms) tuples,
    plus total_ms from the summary line.
    """
    results = []
    total_ms = None

    # Match statement headers: [N] description
    stmt_pattern = re.compile(r'^\[(\d+)\]\s+(.+)$', re.MULTILINE)
    # Match timing: -- X ms
    time_pattern = re.compile(r'^--\s+(\d+)\s+ms$', re.MULTILINE)
    # Match total: === File complete: N statement(s), X ms total ===
    total_pattern = re.compile(r'===\s+File complete:.*?(\d+)\s+ms total\s+===')

    stmts = stmt_pattern.findall(output)
    times = time_pattern.findall(output)
    total_match = total_pattern.search(output)

    if total_match:
        total_ms = int(total_match.group(1))

    # Pair up statements with timings
    for i in range(min(len(stmts), len(times))):
        stmt_num = int(stmts[i][0])
        desc = stmts[i][1].strip()
        ms = int(times[i])
        results.append((stmt_num, desc, ms))

    return results, total_ms


def classify_operation(desc):
    """Classify a statement description into an operation category."""
    upper = desc.upper()
    if "INSERT BATCH" in upper or upper.startswith("INSERT"):
        return "insert"
    elif upper.startswith("SELECT") and "WHERE ID" in upper:
        return "select_targeted"
    elif upper.startswith("SELECT"):
        return "select_full_scan"
    elif upper.startswith("UPDATE"):
        return "update"
    elif upper.startswith("DELETE"):
        return "delete"
    elif upper.startswith("CREATE DATABASE") or upper.startswith("USE "):
        return "setup"
    elif upper.startswith("CREATE TABLE"):
        return "setup"
    elif upper.startswith("DROP"):
        return "teardown"
    else:
        return "other"


def aggregate_timings(timings):
    """
    Given parsed timings, aggregate by operation category.
    Returns dict of { category: { 'total_ms': X, 'count': N, 'times': [...] } }
    """
    agg = {}
    for stmt_num, desc, ms in timings:
        cat = classify_operation(desc)
        if cat not in agg:
            agg[cat] = {"total_ms": 0, "count": 0, "times": []}
        agg[cat]["total_ms"] += ms
        agg[cat]["count"] += 1
        agg[cat]["times"].append(ms)
    return agg


# ── Benchmark Execution ──────────────────────────────────────────────────────

def run_single_node(build_dir, sql_file, use_file_db=False):
    """Run bench_single and return stdout."""
    exe = os.path.join(build_dir, "bench_single")
    if sys.platform == "win32" and not exe.endswith(".exe"):
        exe += ".exe"

    cmd = [exe, sql_file, "--txn"]
    if use_file_db:
        cmd += ["--db", "bench_temp.db"]

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    if result.returncode != 0:
        print(f"  WARNING: bench_single returned {result.returncode}", file=sys.stderr)
        if result.stderr:
            print(f"  stderr: {result.stderr[:500]}", file=sys.stderr)
    return result.stdout


def run_distributed(build_dir, sql_file, num_workers):
    """Run dist_db with mpirun and return stdout."""
    exe = os.path.join(build_dir, "dist_db")
    if sys.platform == "win32" and not exe.endswith(".exe"):
        exe += ".exe"

    num_procs = num_workers + 1  # workers + coordinator
    cmd = ["mpiexec", "-n", str(num_procs), exe, "--file", sql_file]

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    if result.returncode != 0:
        print(f"  WARNING: dist_db returned {result.returncode}", file=sys.stderr)
        if result.stderr:
            print(f"  stderr: {result.stderr[:500]}", file=sys.stderr)
    return result.stdout


def cleanup_db_files():
    """Remove any leftover .db files from distributed runs."""
    for f in Path(".").glob("*_node*.db"):
        try:
            f.unlink()
        except OSError:
            pass
    for f in Path(".").glob("bench_temp.db"):
        try:
            f.unlink()
        except OSError:
            pass


# ── Main Driver ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Comprehensive Distributed SQL Benchmark Suite")
    parser.add_argument("--build-dir", default=DEFAULT_BUILD_DIR,
                        help=f"Build directory (default: {DEFAULT_BUILD_DIR})")
    parser.add_argument("--runs", type=int, default=DEFAULT_RUNS,
                        help=f"Number of runs per configuration (default: {DEFAULT_RUNS})")
    parser.add_argument("--sizes", default=",".join(map(str, DEFAULT_SIZES)),
                        help=f"Comma-separated row counts (default: {','.join(map(str, DEFAULT_SIZES))})")
    parser.add_argument("--workers", default=",".join(map(str, DEFAULT_WORKERS)),
                        help=f"Comma-separated worker counts (default: {','.join(map(str, DEFAULT_WORKERS))})")
    parser.add_argument("--operations", default=",".join(OPERATIONS.keys()),
                        help=f"Comma-separated operations to test (default: all)")
    parser.add_argument("--output-dir", default="benchmark_results",
                        help="Output directory for results (default: benchmark_results)")
    args = parser.parse_args()

    sizes = [int(s) for s in args.sizes.split(",")]
    workers = [int(w) for w in args.workers.split(",")]
    operations = [op.strip() for op in args.operations.split(",")]
    runs = args.runs
    build_dir = args.build_dir
    output_dir = args.output_dir

    # Create output directories
    sql_dir = os.path.join(output_dir, "sql")
    os.makedirs(sql_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = os.path.join(output_dir, "results.csv")
    summary_file = os.path.join(output_dir, "summary.csv")

    # Check executables exist
    for exe_name in ["bench_single", "dist_db"]:
        exe = os.path.join(build_dir, exe_name)
        if sys.platform == "win32":
            exe += ".exe"
        if not os.path.exists(exe):
            print(f"ERROR: {exe} not found. Run cmake --build {build_dir} first.")
            sys.exit(1)

    print("=" * 70)
    print("  Distributed SQL Database — Comprehensive Benchmark Suite")
    print("=" * 70)
    print(f"  Sizes:      {sizes}")
    print(f"  Workers:    {workers}")
    print(f"  Operations: {operations}")
    print(f"  Runs:       {runs}")
    print(f"  Output:     {output_dir}/")
    print("=" * 70)

    # ── Phase 1: Generate SQL files ───────────────────────────────────────────

    print("\n[Phase 1] Generating SQL workload files...")
    sql_files = {}  # (operation, size, distributed) -> path

    for op_name in operations:
        gen_func = OPERATIONS[op_name][0]
        for n in sizes:
            for distributed in [True, False]:
                suffix = "dist" if distributed else "single"
                fname = f"{op_name}_{n}_{suffix}.sql"
                fpath = os.path.join(sql_dir, fname)
                sql_content = gen_func(n, distributed=distributed)
                with open(fpath, "w") as f:
                    f.write(sql_content)
                sql_files[(op_name, n, distributed)] = fpath

    total_files = len(sql_files)
    print(f"  Generated {total_files} SQL files in {sql_dir}/")

    # ── Phase 2: Run benchmarks ──────────────────────────────────────────────

    print("\n[Phase 2] Running benchmarks...")

    raw_rows = []  # Each row: dict for CSV

    # Count total configurations for progress
    total_configs = 0
    for op_name in operations:
        for n in sizes:
            # Single-node memory + file = 2 configs
            total_configs += 2
            # Distributed with each worker count
            total_configs += len(workers)
    total_runs = total_configs * runs
    current_run = 0

    for op_name in operations:
        print(f"\n  === Operation: {op_name} ===")

        for n in sizes:
            # ── Single-node (in-memory) ──────────────────────────────
            sql_path = sql_files[(op_name, n, False)]
            for run_i in range(1, runs + 1):
                current_run += 1
                print(f"    [{current_run}/{total_runs}] single_memory  "
                      f"rows={n}  run={run_i}", end="", flush=True)

                cleanup_db_files()
                try:
                    output = run_single_node(build_dir, sql_path, use_file_db=False)
                    timings, total_ms = parse_timings(output)
                    agg = aggregate_timings(timings)

                    for cat, data in agg.items():
                        if cat in ("setup", "teardown", "other"):
                            continue
                        raw_rows.append({
                            "timestamp": timestamp,
                            "implementation": "single_memory",
                            "num_workers": 1,
                            "operation": op_name,
                            "sub_operation": cat,
                            "row_count": n,
                            "op_count": data["count"],
                            "total_ms": data["total_ms"],
                            "avg_ms": round(data["total_ms"] / max(data["count"], 1), 3),
                            "run": run_i,
                        })

                    # Also record the overall total
                    if total_ms is not None:
                        raw_rows.append({
                            "timestamp": timestamp,
                            "implementation": "single_memory",
                            "num_workers": 1,
                            "operation": op_name,
                            "sub_operation": "total",
                            "row_count": n,
                            "op_count": len(timings),
                            "total_ms": total_ms,
                            "avg_ms": round(total_ms / max(len(timings), 1), 3),
                            "run": run_i,
                        })
                    print(f"  -> {total_ms} ms")
                except Exception as e:
                    print(f"  -> ERROR: {e}")

            # ── Single-node (file-based) ─────────────────────────────
            for run_i in range(1, runs + 1):
                current_run += 1
                print(f"    [{current_run}/{total_runs}] single_file    "
                      f"rows={n}  run={run_i}", end="", flush=True)

                cleanup_db_files()
                try:
                    output = run_single_node(build_dir, sql_path, use_file_db=True)
                    timings, total_ms = parse_timings(output)
                    agg = aggregate_timings(timings)

                    for cat, data in agg.items():
                        if cat in ("setup", "teardown", "other"):
                            continue
                        raw_rows.append({
                            "timestamp": timestamp,
                            "implementation": "single_file",
                            "num_workers": 1,
                            "operation": op_name,
                            "sub_operation": cat,
                            "row_count": n,
                            "op_count": data["count"],
                            "total_ms": data["total_ms"],
                            "avg_ms": round(data["total_ms"] / max(data["count"], 1), 3),
                            "run": run_i,
                        })

                    if total_ms is not None:
                        raw_rows.append({
                            "timestamp": timestamp,
                            "implementation": "single_file",
                            "num_workers": 1,
                            "operation": op_name,
                            "sub_operation": "total",
                            "row_count": n,
                            "op_count": len(timings),
                            "total_ms": total_ms,
                            "avg_ms": round(total_ms / max(len(timings), 1), 3),
                            "run": run_i,
                        })
                    print(f"  -> {total_ms} ms")
                except Exception as e:
                    print(f"  -> ERROR: {e}")

            # ── Distributed (varying workers) ────────────────────────
            sql_path_dist = sql_files[(op_name, n, True)]
            for nw in workers:
                for run_i in range(1, runs + 1):
                    current_run += 1
                    print(f"    [{current_run}/{total_runs}] distributed    "
                          f"rows={n}  workers={nw}  run={run_i}", end="", flush=True)

                    cleanup_db_files()
                    try:
                        output = run_distributed(build_dir, sql_path_dist, nw)
                        timings, total_ms = parse_timings(output)
                        agg = aggregate_timings(timings)

                        for cat, data in agg.items():
                            if cat in ("setup", "teardown", "other"):
                                continue
                            raw_rows.append({
                                "timestamp": timestamp,
                                "implementation": "distributed",
                                "num_workers": nw,
                                "operation": op_name,
                                "sub_operation": cat,
                                "row_count": n,
                                "op_count": data["count"],
                                "total_ms": data["total_ms"],
                                "avg_ms": round(data["total_ms"] / max(data["count"], 1), 3),
                                "run": run_i,
                            })

                        if total_ms is not None:
                            raw_rows.append({
                                "timestamp": timestamp,
                                "implementation": "distributed",
                                "num_workers": nw,
                                "operation": op_name,
                                "sub_operation": "total",
                                "row_count": n,
                                "op_count": len(timings),
                                "total_ms": total_ms,
                                "avg_ms": round(total_ms / max(len(timings), 1), 3),
                                "run": run_i,
                            })
                        print(f"  -> {total_ms} ms")
                    except Exception as e:
                        print(f"  -> ERROR: {e}")

    cleanup_db_files()

    # ── Phase 3: Write raw results CSV ───────────────────────────────────────

    print(f"\n[Phase 3] Writing results to {results_file}...")

    fieldnames = ["timestamp", "implementation", "num_workers", "operation",
                  "sub_operation", "row_count", "op_count", "total_ms", "avg_ms", "run"]

    # Append to existing results file (accumulate across sessions)
    file_exists = os.path.exists(results_file)
    with open(results_file, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(raw_rows)

    print(f"  Wrote {len(raw_rows)} rows")

    # ── Phase 4: Compute summary statistics from ALL accumulated data ────────

    print(f"\n[Phase 4] Computing summary statistics -> {summary_file}...")

    # Read the full results.csv (all sessions) to build comprehensive summary
    all_rows = []
    with open(results_file, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            all_rows.append(row)

    # Group by (implementation, num_workers, operation, sub_operation, row_count)
    groups = {}
    for row in all_rows:
        key = (row["implementation"], int(row["num_workers"]), row["operation"],
               row["sub_operation"], int(row["row_count"]))
        if key not in groups:
            groups[key] = []
        groups[key].append(float(row["total_ms"]))

    summary_fields = ["implementation", "num_workers", "operation", "sub_operation",
                      "row_count", "runs", "mean_ms", "min_ms", "max_ms", "stddev_ms"]

    with open(summary_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=summary_fields)
        writer.writeheader()
        for key, times in sorted(groups.items()):
            impl, nw, op, sub_op, n = key
            mean = round(statistics.mean(times), 2)
            mn = min(times)
            mx = max(times)
            sd = round(statistics.stdev(times), 2) if len(times) > 1 else 0.0
            writer.writerow({
                "implementation": impl,
                "num_workers": nw,
                "operation": op,
                "sub_operation": sub_op,
                "row_count": n,
                "runs": len(times),
                "mean_ms": mean,
                "min_ms": mn,
                "max_ms": mx,
                "stddev_ms": sd,
            })

    print(f"  Wrote {len(groups)} summary rows")

    # ── Phase 5: Print quick summary table ───────────────────────────────────

    print("\n" + "=" * 70)
    print("  RESULTS SUMMARY (total time, mean across runs)")
    print("=" * 70)

    # Print per-operation, per-implementation summary
    for op_name in operations:
        print(f"\n  --- {op_name} ---")
        header = f"  {'Implementation':<20} {'Workers':>8} {'Rows':>8} {'Mean ms':>10} {'Stddev':>10}"
        print(header)
        print("  " + "-" * len(header.strip()))

        for key in sorted(groups.keys()):
            impl, nw, op, sub_op, n = key
            if op != op_name or sub_op != "total":
                continue
            times = groups[key]
            mean = round(statistics.mean(times), 1)
            sd = round(statistics.stdev(times), 1) if len(times) > 1 else 0.0
            print(f"  {impl:<20} {nw:>8} {n:>8} {mean:>10.1f} {sd:>10.1f}")

    print("\n" + "=" * 70)
    print(f"  Full results:  {results_file}")
    print(f"  Summary:       {summary_file}")
    print(f"  SQL files:     {sql_dir}/")
    print("=" * 70)


if __name__ == "__main__":
    main()
