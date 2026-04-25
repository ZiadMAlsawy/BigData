"""Fetch the Kaggle flight-delay CSV into the project root if missing."""
import sys
from pathlib import Path
import shutil

import kagglehub

ROOT = Path(__file__).resolve().parent.parent
TARGET = ROOT / "flights_sample_3m.csv"

if TARGET.exists():
    print(f"{TARGET.name} already present at {TARGET} - nothing to do.")
    sys.exit(0)

cache_path = Path(kagglehub.dataset_download(
    "patrickzel/flight-delay-and-cancellation-dataset-2019-2023"))
src = cache_path / "flights_sample_3m.csv"
if not src.exists():
    src = next(cache_path.rglob("flights_sample_3m.csv"))

shutil.copy(src, TARGET)
print(f"Copied dataset to {TARGET}")
