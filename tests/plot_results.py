#!/usr/bin/env python3
"""
Generate graphs from benchmark results CSV.
Reads benchmark_results/summary.csv and produces PNG charts.

Usage:
  python tests/plot_results.py [--input benchmark_results/summary.csv] [--output benchmark_results/plots/]

Requires: matplotlib (pip install matplotlib)
"""

import argparse
import csv
import os
import sys
from collections import defaultdict

try:
    import matplotlib
    matplotlib.use("Agg")  # Non-interactive backend
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker
except ImportError:
    print("ERROR: matplotlib is required. Install with: pip install matplotlib")
    sys.exit(1)


def load_summary(path):
    """Load summary CSV into list of dicts with numeric conversion."""
    rows = []
    with open(path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row["num_workers"] = int(row["num_workers"])
            row["row_count"] = int(row["row_count"])
            row["runs"] = int(row["runs"])
            row["mean_ms"] = float(row["mean_ms"])
            row["min_ms"] = float(row["min_ms"])
            row["max_ms"] = float(row["max_ms"])
            row["stddev_ms"] = float(row["stddev_ms"])
            rows.append(row)
    return rows


COLORS = {
    "single_memory": "#2196F3",
    "single_file":   "#FF9800",
    "distributed":   "#4CAF50",
}

MARKERS = {
    "single_memory": "o",
    "single_file":   "s",
    "distributed":   "^",
}


def plot_scaling_by_row_count(data, output_dir):
    """
    For each operation: plot time vs row_count for each implementation.
    Distributed uses the median worker count available.
    """
    ops = sorted(set(r["operation"] for r in data))

    for op in ops:
        fig, ax = plt.subplots(figsize=(10, 6))
        op_data = [r for r in data if r["operation"] == op and r["sub_operation"] == "total"]

        # Group by (implementation, num_workers)
        groups = defaultdict(list)
        for r in op_data:
            if r["implementation"] == "distributed":
                key = f"distributed ({r['num_workers']}w)"
            else:
                key = r["implementation"]
            groups[key].append(r)

        for label, rows in sorted(groups.items()):
            rows.sort(key=lambda r: r["row_count"])
            xs = [r["row_count"] for r in rows]
            ys = [r["mean_ms"] for r in rows]
            errs = [r["stddev_ms"] for r in rows]

            base_impl = rows[0]["implementation"]
            color = COLORS.get(base_impl, "#666666")
            marker = MARKERS.get(base_impl, "D")

            # Vary shade for different worker counts
            if "distributed" in label:
                nw = rows[0]["num_workers"]
                alpha = 0.4 + 0.6 * (nw / 10.0)  # More workers = more opaque
            else:
                alpha = 1.0

            ax.errorbar(xs, ys, yerr=errs, label=label, marker=marker,
                        color=color, alpha=alpha, capsize=3, linewidth=2, markersize=6)

        ax.set_xlabel("Row Count", fontsize=12)
        ax.set_ylabel("Time (ms)", fontsize=12)
        ax.set_title(f"Scaling by Dataset Size — {op}", fontsize=14)
        ax.legend(fontsize=9, loc="upper left")
        ax.grid(True, alpha=0.3)
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.xaxis.set_major_formatter(ticker.ScalarFormatter())

        fname = os.path.join(output_dir, f"scaling_rows_{op}.png")
        fig.tight_layout()
        fig.savefig(fname, dpi=150)
        plt.close(fig)
        print(f"  Saved {fname}")


def plot_scaling_by_workers(data, output_dir):
    """
    For each operation and row_count: plot time vs num_workers (distributed only),
    with single-node as horizontal reference lines.
    """
    ops = sorted(set(r["operation"] for r in data))
    sizes = sorted(set(r["row_count"] for r in data))

    for op in ops:
        fig, ax = plt.subplots(figsize=(10, 6))

        dist_data = [r for r in data
                     if r["operation"] == op and r["sub_operation"] == "total"
                     and r["implementation"] == "distributed"]

        single_mem = [r for r in data
                      if r["operation"] == op and r["sub_operation"] == "total"
                      and r["implementation"] == "single_memory"]

        if not dist_data:
            plt.close(fig)
            continue

        # Plot distributed lines for each row_count
        by_size = defaultdict(list)
        for r in dist_data:
            by_size[r["row_count"]].append(r)

        cmap = plt.cm.viridis
        norm_sizes = sorted(by_size.keys())
        for idx, n in enumerate(norm_sizes):
            rows = sorted(by_size[n], key=lambda r: r["num_workers"])
            xs = [r["num_workers"] for r in rows]
            ys = [r["mean_ms"] for r in rows]
            errs = [r["stddev_ms"] for r in rows]
            color = cmap(idx / max(len(norm_sizes) - 1, 1))
            ax.errorbar(xs, ys, yerr=errs, label=f"{n} rows",
                        marker="^", color=color, capsize=3, linewidth=2, markersize=6)

        # Add single-node reference lines
        for r in single_mem:
            if r["row_count"] in by_size:
                ax.axhline(y=r["mean_ms"], linestyle="--", alpha=0.4, color="#2196F3")

        ax.set_xlabel("Number of Workers", fontsize=12)
        ax.set_ylabel("Time (ms)", fontsize=12)
        ax.set_title(f"Scaling by Worker Count — {op}", fontsize=14)
        ax.legend(fontsize=9, loc="upper right")
        ax.grid(True, alpha=0.3)

        fname = os.path.join(output_dir, f"scaling_workers_{op}.png")
        fig.tight_layout()
        fig.savefig(fname, dpi=150)
        plt.close(fig)
        print(f"  Saved {fname}")


def plot_implementation_comparison(data, output_dir):
    """
    Bar chart comparing all implementations for each operation at each size.
    """
    ops = sorted(set(r["operation"] for r in data))
    sizes = sorted(set(r["row_count"] for r in data))

    for n in sizes:
        fig, axes = plt.subplots(1, len(ops), figsize=(4 * len(ops), 6), sharey=False)
        if len(ops) == 1:
            axes = [axes]

        size_data = [r for r in data if r["row_count"] == n and r["sub_operation"] == "total"]

        for ax, op in zip(axes, ops):
            op_rows = [r for r in size_data if r["operation"] == op]
            if not op_rows:
                ax.set_title(f"{op}\n(no data)")
                continue

            labels = []
            means = []
            errs = []
            colors = []

            for r in sorted(op_rows, key=lambda x: (x["implementation"], x["num_workers"])):
                if r["implementation"] == "distributed":
                    labels.append(f"dist-{r['num_workers']}w")
                else:
                    labels.append(r["implementation"].replace("_", "\n"))
                means.append(r["mean_ms"])
                errs.append(r["stddev_ms"])
                colors.append(COLORS.get(r["implementation"], "#666666"))

            bars = ax.bar(range(len(labels)), means, yerr=errs,
                          color=colors, capsize=3, alpha=0.8, edgecolor="white")
            ax.set_xticks(range(len(labels)))
            ax.set_xticklabels(labels, fontsize=8, rotation=45, ha="right")
            ax.set_ylabel("Time (ms)" if ax == axes[0] else "")
            ax.set_title(op, fontsize=11)
            ax.grid(True, axis="y", alpha=0.3)

        fig.suptitle(f"Implementation Comparison — {n} rows", fontsize=14, y=1.02)
        fig.tight_layout()
        fname = os.path.join(output_dir, f"comparison_{n}_rows.png")
        fig.savefig(fname, dpi=150, bbox_inches="tight")
        plt.close(fig)
        print(f"  Saved {fname}")


def plot_operation_breakdown(data, output_dir):
    """
    Stacked bar chart showing time breakdown by sub-operation for mixed workload.
    """
    mixed = [r for r in data if r["operation"] == "mixed" and r["sub_operation"] != "total"]
    if not mixed:
        return

    # Group by (implementation, num_workers, row_count)
    groups = defaultdict(dict)
    for r in mixed:
        key = (r["implementation"], r["num_workers"], r["row_count"])
        groups[key][r["sub_operation"]] = r["mean_ms"]

    sizes = sorted(set(r["row_count"] for r in mixed))
    sub_ops = sorted(set(r["sub_operation"] for r in mixed))
    sub_colors = plt.cm.Set2(range(len(sub_ops)))

    for n in sizes:
        fig, ax = plt.subplots(figsize=(12, 6))
        n_groups = {k: v for k, v in groups.items() if k[2] == n}
        if not n_groups:
            plt.close(fig)
            continue

        sorted_keys = sorted(n_groups.keys())
        labels = []
        for impl, nw, _ in sorted_keys:
            if impl == "distributed":
                labels.append(f"dist-{nw}w")
            else:
                labels.append(impl.replace("_", "\n"))

        x = range(len(labels))
        bottoms = [0.0] * len(labels)

        for si, sub_op in enumerate(sub_ops):
            vals = [n_groups[k].get(sub_op, 0) for k in sorted_keys]
            ax.bar(x, vals, bottom=bottoms, label=sub_op,
                   color=sub_colors[si], edgecolor="white", alpha=0.85)
            bottoms = [b + v for b, v in zip(bottoms, vals)]

        ax.set_xticks(x)
        ax.set_xticklabels(labels, fontsize=9, rotation=45, ha="right")
        ax.set_ylabel("Time (ms)", fontsize=12)
        ax.set_title(f"Mixed Workload Breakdown — {n} rows", fontsize=14)
        ax.legend(fontsize=9, loc="upper left")
        ax.grid(True, axis="y", alpha=0.3)

        fname = os.path.join(output_dir, f"breakdown_mixed_{n}_rows.png")
        fig.tight_layout()
        fig.savefig(fname, dpi=150)
        plt.close(fig)
        print(f"  Saved {fname}")


def main():
    parser = argparse.ArgumentParser(description="Plot benchmark results")
    parser.add_argument("--input", default="benchmark_results/summary.csv",
                        help="Path to summary CSV")
    parser.add_argument("--output", default="benchmark_results/plots",
                        help="Output directory for plots")
    args = parser.parse_args()

    if not os.path.exists(args.input):
        print(f"ERROR: {args.input} not found. Run benchmarks first.")
        sys.exit(1)

    os.makedirs(args.output, exist_ok=True)
    data = load_summary(args.input)
    print(f"Loaded {len(data)} summary rows from {args.input}")

    print("\nGenerating plots...")
    plot_scaling_by_row_count(data, args.output)
    plot_scaling_by_workers(data, args.output)
    plot_implementation_comparison(data, args.output)
    plot_operation_breakdown(data, args.output)

    print(f"\nAll plots saved to {args.output}/")


if __name__ == "__main__":
    main()
