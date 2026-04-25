"""Read results/performance_results.csv and print a comparison pivot.

Stdlib only - no pandas required.
"""
import csv
import os
from collections import defaultdict
from pathlib import Path

RESULTS_DIR = Path(os.environ.get(
    "RESULTS_DIR", str(Path(__file__).resolve().parent / "results")))
PERF_FILE = RESULTS_DIR / "performance_results.csv"

if not PERF_FILE.exists():
    raise SystemExit(f"No performance file at {PERF_FILE} - run the queries first.")

rows = []
with PERF_FILE.open() as f:
    for r in csv.DictReader(f):
        rows.append(r)

print("=== Raw performance log ===")
print(f"{'query':30s}  {'api':10s}  {'seconds':>8s}  {'rows':>10s}")
for r in rows:
    print(f"{r['query']:30s}  {r['api']:10s}  {r['seconds']:>8s}  "
          f"{(r['rows'] or ''):>10s}")

# Pivot: query x {RDD, DataFrame, SQL} -> min seconds
apis = ("RDD", "DataFrame", "SQL")
pivot = defaultdict(lambda: {a: None for a in apis})
for r in rows:
    if r["api"] not in apis:
        continue
    sec = float(r["seconds"])
    cur = pivot[r["query"]][r["api"]]
    if cur is None or sec < cur:
        pivot[r["query"]][r["api"]] = sec

print("\n=== RDD vs DataFrame vs SQL (seconds, min over runs) ===")
header = f"{'query':30s}  " + "  ".join(f"{a:>10s}" for a in apis)
print(header)
for q in sorted(pivot):
    cells = []
    for a in apis:
        v = pivot[q][a]
        cells.append(f"{v:>10.3f}" if v is not None else f"{'-':>10s}")
    print(f"{q:30s}  " + "  ".join(cells))

pivot_path = RESULTS_DIR / "performance_pivot.csv"
with pivot_path.open("w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["query"] + list(apis))
    for q in sorted(pivot):
        w.writerow([q] + [pivot[q][a] if pivot[q][a] is not None else ""
                          for a in apis])
print(f"\nWrote pivot to {pivot_path}")
