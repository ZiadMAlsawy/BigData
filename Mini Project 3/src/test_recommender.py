"""Quick unit tests for recommender.hybrid_score / normalize.

Run: python -m pytest src/test_recommender.py
"""
from __future__ import annotations

import math

from recommender import hybrid_score, normalize


def test_normalize_uniform():
    out = normalize({1: 0.5, 2: 0.5, 3: 0.5})
    assert all(v == 0.0 for v in out.values())


def test_normalize_range():
    out = normalize({1: 0.0, 2: 5.0, 3: 10.0})
    assert math.isclose(out[1], 0.0, abs_tol=1e-6)
    assert math.isclose(out[2], 0.5, abs_tol=1e-6)
    assert math.isclose(out[3], 1.0, abs_tol=1e-6)


def test_hybrid_blend_weights():
    als = {1: 1.0, 2: 0.0}        # normalized -> {1:1, 2:0}
    trending = {1: 0.0, 2: 1.0}   # normalized -> {1:0, 2:1}
    blended = hybrid_score(als, trending, als_weight=0.7, trending_weight=0.3)
    assert math.isclose(blended[1], 0.7, abs_tol=1e-6)
    assert math.isclose(blended[2], 0.3, abs_tol=1e-6)


def test_hybrid_disjoint_keys():
    als = {1: 5.0, 2: 1.0}
    trending = {3: 10.0}
    blended = hybrid_score(als, trending)
    assert set(blended.keys()) == {1, 2, 3}
    # item 3 is missing from ALS so its ALS contribution = 0
    assert blended[3] > 0.0
