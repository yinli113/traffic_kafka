import argparse
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional


def _maybe_load_dotenv() -> None:
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


def _bool_to_int(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return 1 if value else 0
    return None


def _iter_jsonl_files(jsonl_dir: Path, topic: Optional[str]) -> Iterable[Path]:
    pattern = f"{topic}_*.jsonl" if topic else "*.jsonl"
    return sorted(jsonl_dir.glob(pattern))


def _extract_payload(value_json: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(value_json, dict):
        return None
    payload = value_json.get("payload")
    if isinstance(payload, dict):
        return payload
    return value_json


def _row_from_record(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    value_json = record.get("value_json")
    if value_json is None:
        value_json = _safe_json_loads(record.get("value", "") or "")
    payload = _extract_payload(value_json)
    if not isinstance(payload, dict):
        return None

    latest_stats = payload.get("latest_stats")
    if not isinstance(latest_stats, dict):
        latest_stats = {}

    return {
        "topic": record.get("topic"),
        "partition": record.get("partition"),
        "offset": record.get("offset"),
        "key": record.get("key"),
        "dumped_at": record.get("dumped_at"),
        "ingested_at": value_json.get("ingested_at") if isinstance(value_json, dict) else None,
        "interval_start": latest_stats.get("interval_start"),
        "link_id": payload.get("id"),
        "road_name": payload.get("name"),
        "travel_time_seconds": latest_stats.get("travel_time"),
        "delay_seconds": latest_stats.get("delay"),
        "speed_kmh": latest_stats.get("speed"),
        "length_m": payload.get("length"),
        "minimum_tt_seconds": payload.get("minimum_tt"),
        "enough_data": _bool_to_int(latest_stats.get("enough_data")),
        "ignored": _bool_to_int(latest_stats.get("ignored")),
        "closed": _bool_to_int(latest_stats.get("closed")),
        "expected_missing": _bool_to_int(latest_stats.get("expected_missing")),
        "payload_json": json.dumps(payload, ensure_ascii=False),
    }


def _init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS traffic_observations (
            topic TEXT,
            partition INTEGER,
            offset INTEGER,
            key TEXT,
            dumped_at TEXT,
            ingested_at TEXT,
            interval_start TEXT,
            link_id INTEGER,
            road_name TEXT,
            travel_time_seconds REAL,
            delay_seconds REAL,
            speed_kmh REAL,
            length_m REAL,
            minimum_tt_seconds REAL,
            enough_data INTEGER,
            ignored INTEGER,
            closed INTEGER,
            expected_missing INTEGER,
            payload_json TEXT,
            PRIMARY KEY (topic, partition, offset)
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_interval_start ON traffic_observations(interval_start);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_link_id ON traffic_observations(link_id);")


def _load_jsonl_file(conn: sqlite3.Connection, path: Path) -> int:
    inserted = 0
    with path.open("r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except Exception:
                continue
            if not isinstance(record, dict):
                continue
            row = _row_from_record(record)
            if row is None:
                continue
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO traffic_observations (
                    topic, partition, offset, key, dumped_at, ingested_at, interval_start,
                    link_id, road_name, travel_time_seconds, delay_seconds, speed_kmh,
                    length_m, minimum_tt_seconds, enough_data, ignored, closed,
                    expected_missing, payload_json
                ) VALUES (
                    :topic, :partition, :offset, :key, :dumped_at, :ingested_at, :interval_start,
                    :link_id, :road_name, :travel_time_seconds, :delay_seconds, :speed_kmh,
                    :length_m, :minimum_tt_seconds, :enough_data, :ignored, :closed,
                    :expected_missing, :payload_json
                );
                """,
                row,
            )
            inserted += cur.rowcount
    return inserted


def run(*, jsonl_dir: Path, db_path: Path, topic: Optional[str]) -> None:
    jsonl_dir.mkdir(parents=True, exist_ok=True)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        _init_db(conn)
        total_inserted = 0
        files = list(_iter_jsonl_files(jsonl_dir, topic))
        for path in files:
            total_inserted += _load_jsonl_file(conn, path)
        conn.commit()
    finally:
        conn.close()

    now = datetime.now(timezone.utc).isoformat()
    print(f"[sqlite] loaded files={len(files)} inserted_rows={total_inserted} at={now}")


def main() -> None:
    _maybe_load_dotenv()
    parser = argparse.ArgumentParser(description="Load JSONL dumps into a local SQLite database.")
    parser.add_argument("--jsonl-dir", default=os.getenv("JSONL_DIR", "./data/raw"))
    parser.add_argument("--db-path", default=os.getenv("SQLITE_DB_PATH", "./data/traffic.db"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC"))
    args = parser.parse_args()

    run(
        jsonl_dir=Path(args.jsonl_dir),
        db_path=Path(args.db_path),
        topic=args.topic,
    )


if __name__ == "__main__":
    main()
