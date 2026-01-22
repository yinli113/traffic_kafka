# Kafka Status Check Guide (Beginner-Friendly)

This doc explains how to use `src/status_check.py` to understand what’s happening in your Kafka pipeline and how to debug common issues.

## What problem this solves

When you run this project, you often want to answer:

- Did the **producer** actually get data into Kafka?
- Did the **consumer** actually read data from Kafka?
- Is the consumer **caught up** or **behind** (lagging)?
- How many messages arrived in the last 5 minutes / 15 minutes?

`src/status_check.py` answers those questions by combining:

- Kafka-side truth (**offsets**) and
- end-to-end confirmation (**your local JSONL dump**).

## Quick usage

Run from your laptop:

```bash
cd /Users/yinli/Desktop/traffic_kafka
source .venv/bin/activate
python -m src.status_check --topic traffic_raw --group-id traffic_dump --out-dir ./data/raw --window-minutes 5 --since-minutes 120
```

## What the script prints (and what it means)

### Section A: `=== Kafka (stored vs committed) ===`

Example line:

```
partition=0 begin=0 end=72 committed=27 lag=45
```

- **partition=0**: Kafka topics are split into partitions (parallel logs).
- **begin=0**: the earliest offset still available in this partition (retention can move this forward).
- **end=72**: the “next offset” Kafka will write; meaning offsets `0..71` exist (72 total messages since begin).
- **committed=27**: the consumer group’s last committed offset for this partition.
- **lag=45**: how many messages the consumer group is behind in that partition.

#### Why offsets matter

Kafka stores messages as an ordered log per partition:

- Every message gets an **offset** (0,1,2,3,...).
- Consumers track progress via **committed offsets** in a consumer group.

### Section B: `total_available_messages` and `total_lag`

This summarizes all partitions:

- **total_available_messages**: approx how many messages are currently in the topic (across partitions).
- **total_lag**: approx how many messages your consumer group hasn’t committed yet.

If the producer is stopped, a healthy consumer should make `total_lag` trend toward **0**.

### Section C: `=== Local JSONL dump (end-to-end confirmation) ===`

This part reads the files written by `src/consumer_dump.py` and answers:

- Are we actually writing messages to disk?
- How many messages per time window?
- Which keys (link IDs) appeared recently?

Key fields:

- **messages_seen_in_jsonl**: how many messages were written to JSONL in the last `--since-minutes`.
- **messages_per_window**: counts grouped into `--window-minutes` buckets (default 5 minutes).
- **top_keys**: most common Kafka keys in that time range (for this project: link IDs).
- **max_offset_by_partition_seen_in_jsonl**: biggest offsets the JSONL dump has observed per partition (useful to compare with Kafka end offsets).

## How `src/status_check.py` works (step by step)

### 1) It reads Kafka metadata (offsets)

The function `kafka_offsets()`:

```101:159:src/status_check.py
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
        # ...
        committed = c.committed(tps, timeout=timeout_seconds)
        # ...
        lo, hi = c.get_watermark_offsets(TopicPartition(topic, p), timeout=timeout_seconds)
        # ...
```

What it does:

- Asks Kafka for the topic partitions.
- For each partition, reads **watermarks** (begin/end offsets).
- Reads the consumer group’s **committed** offsets.
- Computes `lag = end - committed` (or “all available” if never committed).

### 2) It reads your local JSONL files

The function `summarize_jsonl()`:

```51:98:src/status_check.py
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
    # ...
    for rec in _iter_jsonl_records(out_dir):
        dumped_at = _parse_ts(str(rec.get("dumped_at", "")))
        # ...
        win = _floor_time(dumped_at_utc, window_minutes)
        window_counts[win.isoformat()] += 1
        if rec.get("key") is not None:
            keys[str(rec.get("key"))] += 1
```

What it does:

- Scans `./data/raw/traffic_raw_*.jsonl`
- Uses `dumped_at` (when the consumer wrote the file) as the time axis
- Counts messages per 5-minute window
- Counts keys (link IDs) so you can confirm you’re polling many links

### 3) It prints both views together

The `main()` function prints the Kafka offsets/lag first, then the JSONL summary:

```164:210:src/status_check.py
def main() -> None:
    # ...
    print("=== Kafka (stored vs committed) ===")
    ko = kafka_offsets(topic=args.topic, group_id=args.group_id)
    # ...
    print("\n=== Local JSONL dump (end-to-end confirmation) ===")
    js = summarize_jsonl(out_dir=out_dir, window_minutes=args.window_minutes, since_minutes=args.since_minutes)
    # ...
```

## Troubleshooting playbook

### Symptom: Kafka has messages but JSONL shows 0

You might see:

- Kafka section shows non-zero `end` offsets
- JSONL section shows `messages_seen_in_jsonl=0`

Likely causes:

- `src.consumer_dump.py` is not running
- consumer is writing to a different `--out-dir`
- you ran status_check from the wrong directory (so `./data/raw` doesn’t exist)

Fix:

```bash
ps aux | grep src.consumer_dump | grep -v grep
python -m src.consumer_dump --topic traffic_raw --out-dir ./data/raw
```

### Symptom: Lag is high and not decreasing

Likely causes:

- consumer stopped
- consumer is slow (disk I/O)
- producer is producing faster than consumer can consume

Fix:

- Restart consumer
- Reduce polling frequency or number of link IDs
- (Optional) run a second consumer in the same group to parallelize (only helps if you have multiple partitions and enough spread)

### Symptom: Some partitions show `committed=-1001`

That means the consumer group has **never committed** an offset for that partition.

Common reasons:

- consumer was not running while those partitions received messages
- keys are uneven and producer wrote mostly to a subset of partitions

Fix:

- ensure consumer is running; it will eventually commit as it reads
- expand your polled IDs (more keys) to spread load across partitions

## What “no missing messages” really means in practice

In real streaming systems, you aim for:

- **no silent drops** (you know when things fail)
- **recoverability** (replay from Kafka)
- **dedupe downstream** (because at-least-once is normal)

This project follows that pattern:

- Kafka offsets tell you what was stored
- Consumer committed offsets tell you what was processed
- Local JSONL confirms end-to-end writing

