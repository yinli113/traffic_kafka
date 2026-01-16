import argparse
import json
import os
import signal
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from confluent_kafka import Consumer, KafkaException

from .kafka_config import kafka_client_config_from_env


def _utc_now_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _maybe_load_dotenv() -> None:
    # Optional convenience: load .env in local dev without exporting variables each time.
    try:
        from dotenv import load_dotenv  # type: ignore

        load_dotenv()
    except Exception:
        pass


def _safe_json_loads(s: str) -> Optional[Dict[str, Any]]:
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


class DumpWriter:
    def __init__(self, out_dir: Path, topic: str) -> None:
        self.out_dir = out_dir
        self.topic = topic
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.path = self.out_dir / f"{topic}_{_utc_now_compact()}.jsonl"
        self.fp = self.path.open("a", encoding="utf-8")
        self.top_level_keys = Counter()
        self.payload_keys = Counter()
        self.latest_stats_keys = Counter()
        self.seen = 0

    def write(self, record: Dict[str, Any]) -> None:
        self.fp.write(json.dumps(record, ensure_ascii=False) + "\n")
        self.seen += 1

        payload_obj = record.get("value_json")
        if isinstance(payload_obj, dict):
            self.top_level_keys.update(payload_obj.keys())
            payload = payload_obj.get("payload")
            if isinstance(payload, dict):
                self.payload_keys.update(payload.keys())
                latest_stats = payload.get("latest_stats")
                if isinstance(latest_stats, dict):
                    self.latest_stats_keys.update(latest_stats.keys())

    def flush_schema_hints(self) -> None:
        hints = {
            "seen_messages": self.seen,
            "top_level_keys": dict(self.top_level_keys.most_common()),
            "payload_keys": dict(self.payload_keys.most_common()),
            "latest_stats_keys": dict(self.latest_stats_keys.most_common()),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        (self.out_dir / "schema_hints.json").write_text(json.dumps(hints, indent=2), encoding="utf-8")

    def close(self) -> None:
        try:
            self.fp.flush()
        finally:
            self.fp.close()


def run(*, topic: str, out_dir: Path, group_id: str, poll_timeout: float) -> None:
    cfg = kafka_client_config_from_env()
    cfg.update(
        {
            "group.id": group_id,
            "auto.offset.reset": os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
            "enable.auto.commit": True,
        }
    )

    consumer = Consumer(cfg)
    writer = DumpWriter(out_dir=out_dir, topic=topic)
    keep_running = True

    def _handle_stop(_signum, _frame) -> None:
        nonlocal keep_running
        keep_running = False

    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    try:
        consumer.subscribe([topic])
        last_hints = time.time()

        while keep_running:
            msg = consumer.poll(poll_timeout)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            key = msg.key().decode("utf-8", errors="replace") if msg.key() else None
            value = msg.value().decode("utf-8", errors="replace") if msg.value() else ""
            value_json = _safe_json_loads(value)

            out = {
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "timestamp": msg.timestamp(),
                "key": key,
                "value": value,
                "value_json": value_json,
                "dumped_at": datetime.now(timezone.utc).isoformat(),
            }
            writer.write(out)

            now = time.time()
            if now - last_hints >= 5:
                writer.flush_schema_hints()
                last_hints = now

    finally:
        try:
            writer.flush_schema_hints()
            writer.close()
        finally:
            consumer.close()


def main() -> None:
    _maybe_load_dotenv()
    parser = argparse.ArgumentParser(description="Consume Kafka topic and dump messages to local JSONL files.")
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "traffic_raw"))
    parser.add_argument("--out-dir", default=os.getenv("OUT_DIR", "./data/raw"))
    parser.add_argument("--group-id", default=os.getenv("KAFKA_GROUP_ID", "traffic_dump"))
    parser.add_argument("--poll-timeout", type=float, default=float(os.getenv("POLL_TIMEOUT_SECONDS", "1.0")))
    args = parser.parse_args()

    run(
        topic=args.topic,
        out_dir=Path(args.out_dir),
        group_id=args.group_id,
        poll_timeout=args.poll_timeout,
    )


if __name__ == "__main__":
    main()

