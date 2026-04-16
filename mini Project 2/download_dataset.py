"""Download the flight-delay dataset from Kaggle to the local project folder.

Usage:
    python download_dataset.py

Requires `kagglehub` and Kaggle credentials (~/.kaggle/kaggle.json or env vars).
"""
import shutil
from pathlib import Path

import kagglehub

TARGET = Path(__file__).parent / 'flights_sample_3m.csv'

if TARGET.exists():
    print(f'{TARGET.name} already present - skipping download.')
    raise SystemExit(0)

cache_path = Path(kagglehub.dataset_download(
    'patrickzel/flight-delay-and-cancellation-dataset-2019-2023'))
src = cache_path / 'flights_sample_3m.csv'
if not src.exists():
    src = next(cache_path.rglob('flights_sample_3m.csv'))

shutil.copy(src, TARGET)
print(f'Copied dataset to {TARGET}')
