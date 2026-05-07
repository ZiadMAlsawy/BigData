"""Kafka producer for MP3.

Two modes:
  --mode replay    : iterate the indexed CSV, emit at controlled rate (default 50 ev/s)
  --mode synthetic : bursty generator that periodically spikes ratings on a chosen item

Each event is JSON:
  {"user_id": int, "item_id": int, "rating": float,
   "timestamp": "ISO-8601", "event_time_ms": int,
   "user_key": "<reviewerID>", "item_key": "<asin>"}

Partition key = str(user_id % 3) so events for the same user always land on the same partition.
"""
from __future__ import annotations

import argparse
import json
import random
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer

from common import (
    CSV_PATH, ITEM_INDEX_PATH, KAFKA_BROKER, TOPIC_INTERACTIONS,
    USER_INDEX_PATH,
)

NUM_PARTITIONS = 3


def make_producer(broker: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        linger_ms=5,
        acks=1,
    )


def load_lookup() -> tuple[pd.DataFrame, pd.DataFrame]:
    if not USER_INDEX_PATH.exists() or not ITEM_INDEX_PATH.exists():
        sys.exit(f"ERROR: run train_als.py first to produce {USER_INDEX_PATH} and {ITEM_INDEX_PATH}")
    users = pd.read_parquet(USER_INDEX_PATH)
    items = pd.read_parquet(ITEM_INDEX_PATH)
    return users, items


def _build_event(user_id: int, item_id: int, rating: float, *,
                 user_key: str, item_key: str) -> dict:
    now = datetime.now(timezone.utc)
    return {
        "user_id": int(user_id),
        "item_id": int(item_id),
        "rating": float(rating),
        "timestamp": now.isoformat(),
        "event_time_ms": int(now.timestamp() * 1000),
        "user_key": user_key,
        "item_key": item_key,
    }


def replay_mode(producer: KafkaProducer, csv_path: Path,
                rate: float, limit: int | None) -> None:
    users, items = load_lookup()
    user_map = dict(zip(users["reviewerID"], users["user_id"]))
    item_map = dict(zip(items["asin"], items["item_id"]))

    print(f"[producer:replay] reading {csv_path} (rate={rate}/s)")
    period = 1.0 / rate if rate > 0 else 0.0
    sent = 0
    skipped = 0

    chunks = pd.read_csv(csv_path, chunksize=20_000,
                         usecols=["reviewerID", "asin", "overall", "unixReviewTime"])
    for chunk in chunks:
        chunk = chunk.dropna(subset=["reviewerID", "asin", "overall"])
        for row in chunk.itertuples(index=False):
            uid = user_map.get(row.reviewerID)
            iid = item_map.get(row.asin)
            if uid is None or iid is None:
                skipped += 1
                continue
            ev = _build_event(uid, iid, row.overall,
                              user_key=row.reviewerID, item_key=row.asin)
            producer.send(TOPIC_INTERACTIONS,
                          key=uid % NUM_PARTITIONS, value=ev)
            sent += 1
            if sent % 1000 == 0:
                print(f"[producer:replay] sent={sent:,}  skipped={skipped:,}")
            if limit and sent >= limit:
                producer.flush()
                print(f"[producer:replay] reached limit {limit}, stopping")
                return
            if period:
                time.sleep(period)
    producer.flush()
    print(f"[producer:replay] done, sent={sent:,}  skipped={skipped:,}")


def synthetic_mode(producer: KafkaProducer, rate: float,
                   spike_every: float, spike_seconds: float) -> None:
    users, items = load_lookup()
    user_ids = users["user_id"].tolist()
    item_pool = items["item_id"].tolist()
    item_keys = dict(zip(items["item_id"], items["asin"]))
    user_keys = dict(zip(users["user_id"], users["reviewerID"]))

    print(f"[producer:synthetic] rate={rate}/s spike_every={spike_every}s "
          f"spike_dur={spike_seconds}s users={len(user_ids):,} items={len(item_pool):,}")
    period = 1.0 / rate if rate > 0 else 0.0
    next_spike = time.time() + spike_every
    spike_until = 0.0
    spike_item = None
    sent = 0

    while True:
        now = time.time()
        if now >= next_spike and now >= spike_until:
            spike_item = random.choice(item_pool)
            spike_until = now + spike_seconds
            next_spike = now + spike_every
            print(f"[producer:synthetic] SPIKING item_id={spike_item} for {spike_seconds}s")

        if now < spike_until and spike_item is not None and random.random() < 0.6:
            iid = spike_item
            rating = random.choice([4.5, 5.0, 5.0, 5.0])
        else:
            iid = random.choice(item_pool)
            rating = round(random.uniform(1.0, 5.0) * 2) / 2

        uid = random.choice(user_ids)
        ev = _build_event(uid, iid, rating,
                          user_key=user_keys.get(uid, str(uid)),
                          item_key=item_keys.get(iid, str(iid)))
        producer.send(TOPIC_INTERACTIONS, key=uid % NUM_PARTITIONS, value=ev)
        sent += 1
        if sent % 500 == 0:
            print(f"[producer:synthetic] sent={sent:,}")
        if period:
            time.sleep(period)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["replay", "synthetic"], default="replay")
    ap.add_argument("--rate", type=float, default=50.0,
                    help="events per second (0 = as fast as possible)")
    ap.add_argument("--limit", type=int, default=None,
                    help="replay-mode max events to send")
    ap.add_argument("--spike-every", type=float, default=60.0,
                    help="synthetic mode: seconds between spikes")
    ap.add_argument("--spike-seconds", type=float, default=15.0,
                    help="synthetic mode: how long each spike lasts")
    ap.add_argument("--broker", default=KAFKA_BROKER)
    ap.add_argument("--csv", default=str(CSV_PATH))
    args = ap.parse_args()

    producer = make_producer(args.broker)
    print(f"[producer] connected to {args.broker}, topic={TOPIC_INTERACTIONS}")

    def _shutdown(*_):
        print("\n[producer] flushing & closing")
        producer.flush(timeout=5)
        producer.close(timeout=5)
        sys.exit(0)
    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    if args.mode == "replay":
        replay_mode(producer, Path(args.csv), args.rate, args.limit)
    else:
        synthetic_mode(producer, args.rate, args.spike_every, args.spike_seconds)


if __name__ == "__main__":
    main()
