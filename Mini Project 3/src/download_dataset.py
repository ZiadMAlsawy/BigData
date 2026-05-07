"""Download Kindle Reviews dataset via kagglehub and copy CSV into data/."""
from __future__ import annotations

import shutil
import sys
from pathlib import Path

import kagglehub

from common import CSV_PATH, DATA_DIR, ensure_dirs

DATASET = "bharadwaj6/kindle-reviews"


def main() -> int:
    ensure_dirs()
    print(f"[download] pulling {DATASET} via kagglehub")
    src_dir = Path(kagglehub.dataset_download(DATASET))
    print(f"[download] cached at {src_dir}")

    candidates = [p for p in src_dir.glob("*.csv") if "kindle" in p.name.lower()]
    if not candidates:
        candidates = list(src_dir.glob("*.csv"))
    if not candidates:
        print(f"[download] ERROR: no CSV found under {src_dir}", file=sys.stderr)
        return 1

    src = max(candidates, key=lambda p: p.stat().st_size)
    print(f"[download] copying {src.name} -> {CSV_PATH}")
    shutil.copy2(src, CSV_PATH)

    size_mb = CSV_PATH.stat().st_size / 1024 / 1024
    print(f"[download] CSV size: {size_mb:.1f} MB")
    print(f"[download] done -> {CSV_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
