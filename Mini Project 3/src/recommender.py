"""Hybrid recommender: ALS factor dot-product blended with streaming trending score.

Loaded once at streaming-app start; broadcast user/item factor matrices to executors.
Designed to be cheap per call so the ML+streaming integration stays under 5 s.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from common import ITEM_FACTORS_PATH, USER_FACTORS_PATH

ALS_WEIGHT = 0.7
TRENDING_WEIGHT = 0.3


@dataclass
class FactorBundle:
    user_ids: np.ndarray         # shape (U,)
    user_factors: np.ndarray     # shape (U, rank)
    item_ids: np.ndarray         # shape (I,)
    item_factors: np.ndarray     # shape (I, rank)
    user_pos: dict[int, int]
    item_pos: dict[int, int]

    @property
    def rank(self) -> int:
        return self.user_factors.shape[1]


def load_factors(user_path=USER_FACTORS_PATH, item_path=ITEM_FACTORS_PATH) -> FactorBundle:
    """Read parquet factors into dense numpy arrays."""
    u = pd.read_parquet(user_path)
    i = pd.read_parquet(item_path)
    u_ids = u["id"].to_numpy(dtype=np.int32)
    i_ids = i["id"].to_numpy(dtype=np.int32)
    u_fac = np.stack(u["features"].to_numpy()).astype(np.float32)
    i_fac = np.stack(i["features"].to_numpy()).astype(np.float32)
    return FactorBundle(
        user_ids=u_ids, user_factors=u_fac,
        item_ids=i_ids, item_factors=i_fac,
        user_pos={int(uid): pos for pos, uid in enumerate(u_ids)},
        item_pos={int(iid): pos for pos, iid in enumerate(i_ids)},
    )


def als_top_k(bundle: FactorBundle, user_id: int, k: int = 5,
              exclude: set[int] | None = None) -> list[tuple[int, float]]:
    """Top-K ALS scores for a single user. Returns [(item_id, score), ...]."""
    pos = bundle.user_pos.get(int(user_id))
    if pos is None:
        return []
    scores = bundle.item_factors @ bundle.user_factors[pos]
    if exclude:
        for it in exclude:
            ip = bundle.item_pos.get(int(it))
            if ip is not None:
                scores[ip] = -np.inf
    # argpartition trick: O(n) for small k
    if k >= len(scores):
        idx = np.argsort(-scores)
    else:
        part = np.argpartition(-scores, k)[:k]
        idx = part[np.argsort(-scores[part])]
    return [(int(bundle.item_ids[j]), float(scores[j])) for j in idx]


def normalize(scores: dict[int, float]) -> dict[int, float]:
    if not scores:
        return scores
    arr = np.array(list(scores.values()), dtype=np.float32)
    lo, hi = arr.min(), arr.max()
    if hi - lo < 1e-9:
        return {k: 0.0 for k in scores}
    return {k: float((v - lo) / (hi - lo)) for k, v in scores.items()}


def hybrid_score(als_scores: dict[int, float],
                 trending: dict[int, float],
                 *,
                 als_weight: float = ALS_WEIGHT,
                 trending_weight: float = TRENDING_WEIGHT) -> dict[int, float]:
    """Blend ALS and trending into a single score per item.

    Both inputs are normalized to [0, 1] before blending so weights are meaningful.
    """
    a = normalize(als_scores)
    t = normalize(trending)
    keys = set(a) | set(t)
    return {
        k: als_weight * a.get(k, 0.0) + trending_weight * t.get(k, 0.0)
        for k in keys
    }


def hybrid_top_k(bundle: FactorBundle, user_id: int,
                 trending: dict[int, float], k: int = 5,
                 exclude: set[int] | None = None) -> list[tuple[int, float]]:
    """Top-K hybrid recs for a user. Cold-start fallback to trending if user unknown."""
    if int(user_id) not in bundle.user_pos:
        if not trending:
            return []
        sorted_t = sorted(trending.items(), key=lambda kv: kv[1], reverse=True)
        return [(int(it), float(s)) for it, s in sorted_t[:k]]

    als = dict(als_top_k(bundle, user_id, k=k * 4, exclude=exclude))
    blended = hybrid_score(als, trending)
    top = sorted(blended.items(), key=lambda kv: kv[1], reverse=True)[:k]
    return [(int(it), float(s)) for it, s in top]
