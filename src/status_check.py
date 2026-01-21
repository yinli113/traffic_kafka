import argparse
import json
import os
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from confluent_kafka import Consumer, TopicPartition

from .kafka_config import kafka_client_config_from_env


def _parse_ts(s: str) -> Optional[datetime]:
    # Examples:
    #  - 2026-01-15T06:54:32.534307+00:00
    #  - 2026-01-15T14:30:00+11:00
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _floor_time(dt: datetime, window_minutes: int) -> datetime:
    # Floor to nearest window in UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt_utc = dt.astimezone(timezone.utc)
    minutes = (dt_utc.minute // window_minutes) * window_minutes
    return dt_utc.replace(minute=minutes, second=0, microsecond=0)


def _iter_jsonl_records(out_dir: Path) -> Iterable[dict]:
    if not out_dir.exists():
        return
    for p in sorted(out_dir.glob("traffic_raw_*.jsonl")):
        try:
            with p.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        yield json.loads(line)
                    except Exception:
                        continue
        except Exception:
            continue


def summarize_jsonl(
    *,
    out_dir: Path,
    window_minutes: int,
    since_minutes: int,
) -> Dict[str, object]:
    """
    Uses your local dump files as an end-to-end confirmation:
    producer -> kafka -> consumer -> jsonl.
    """
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=since_minutes)

    window_counts: Counter = Counter()
    keys: Counter = Counter()
    partitions_max_offset: Dict[int, int] = defaultdict(lambda: -1)
    total = 0

    for rec in _iter_jsonl_records(out_dir):
        dumped_at = _parse_ts(str(rec.get("dumped_at", "")))
        if dumped_at is None:
            continue
        dumped_at_utc = dumped_at.astimezone(timezone.utc)
        if dumped_at_utc < cutoff:
            continue

        total += 1
        win = _floor_time(dumped_at_utc, window_minutes)
        window_counts[win.isoformat()] += 1
        if rec.get("key") is not None:
            keys[str(rec.get("key"))] += 1

        try:
            part = int(rec.get("partition"))
            off = int(rec.get("offset"))
            partitions_max_offset[part] = max(partitions_max_offset[part], off)
        except Exception:
            pass

    return {
        "jsonl_out_dir": str(out_dir),
        "since_minutes": since_minutes,
        "window_minutes": window_minutes,
        "messages_seen": total,
        "messages_per_window": dict(sorted(window_counts.items())),
        "top_keys": keys.most_common(10),
        "max_offset_by_partition_seen_in_jsonl": dict(sorted(partitions_max_offset.items())),
    }


def kafka_offsets(
    *,
    topic: str,
    group_id: str,
    timeout_seconds: float = 10.0,
) -> Dict[str, object]:
    """
    Kafka-side confirmation:
    - end offsets: what's stored
    - committed offsets (consumer group): what's been processed/committed
    - lag = end - committed
    """
    cfg = kafka_client_config_from_env()
    cfg.update(
        {
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    c = Consumer(cfg)
    try:
        md = c.list_topics(topic=topic, timeout=timeout_seconds)
        t = md.topics.get(topic)
        if t is None or t.error is not None:
            raise RuntimeError(f"Topic not found or error: {topic} {t.error if t else ''}".strip())

        partitions = sorted(t.partitions.keys())
        tps = [TopicPartition(topic, p) for p in partitions]

        committed = c.committed(tps, timeout=timeout_seconds)
        committed_map = {tp.partition: tp.offset for tp in committed}

        end_map: Dict[int, int] = {}
        begin_map: Dict[int, int] = {}
        for p in partitions:
            lo, hi = c.get_watermark_offsets(TopicPartition(topic, p), timeout=timeout_seconds)
            begin_map[p] = int(lo)
            end_map[p] = int(hi)

        lag_map: Dict[int, int] = {}
        for p in partitions:
            end = end_map[p]
            com = committed_map.get(p, -1001)
            if com is None or com < 0:
                # If never committed, treat lag as all available messages
                lag_map[p] = end - begin_map[p]
            else:
                lag_map[p] = max(0, end - com)

        return {
            "topic": topic,
            "group_id": group_id,
            "partitions": partitions,
            "begin_offset": begin_map,
            "end_offset": end_map,
            "committed_offset": committed_map,
            "lag": lag_map,
        }
    finally:
        c.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="One-command status check: Kafka offsets/lag + JSONL output counts.",
    )
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "traffic_raw"))
    parser.add_argument("--group-id", default=os.getenv("KAFKA_GROUP_ID", "traffic_dump"))
    parser.add_argument("--out-dir", default=os.getenv("OUT_DIR", "./data/raw"))
    parser.add_argument("--window-minutes", type=int, default=int(os.getenv("WINDOW_MINUTES", "5")))
    parser.add_argument("--since-minutes", type=int, default=int(os.getenv("SINCE_MINUTES", "120")))
    args = parser.parse_args()

    out_dir = Path(args.out_dir)

    print("=== Kafka (stored vs committed) ===")
    ko = kafka_offsets(topic=args.topic, group_id=args.group_id)
    for p in ko["partitions"]:
        b = ko["begin_offset"].get(p)
        e = ko["end_offset"].get(p)
        c = ko["committed_offset"].get(p)
        l = ko["lag"].get(p)
        print(f"partition={p} begin={b} end={e} committed={c} lag={l}")

    total_end = sum(ko["end_offset"].values())
    total_begin = sum(ko["begin_offset"].values())
    total_avail = max(0, total_end - total_begin)
    total_lag = sum(ko["lag"].values())
    print(f"total_available_messages~={total_avail} total_lag={total_lag}")

    print("\n=== Local JSONL dump (end-to-end confirmation) ===")
    js = summarize_jsonl(out_dir=out_dir, window_minutes=args.window_minutes, since_minutes=args.since_minutes)
    print(f"out_dir={js['jsonl_out_dir']} since_minutes={js['since_minutes']} window_minutes={js['window_minutes']}")
    print(f"messages_seen_in_jsonl={js['messages_seen']}")

    mpw = js["messages_per_window"]
    if mpw:
        print("messages_per_window:")
        for k, v in mpw.items():
            print(f"  {k}  {v}")
    else:
        print("messages_per_window: (none in time range)")

    print(f"top_keys={js['top_keys']}")
    print(f"max_offset_by_partition_seen_in_jsonl={js['max_offset_by_partition_seen_in_jsonl']}")


if __name__ == "__main__":
    main()

