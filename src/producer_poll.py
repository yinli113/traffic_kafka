import argparse
import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import requests
from confluent_kafka import Producer

from .kafka_config import kafka_client_config_from_env


JsonType = Union[Dict[str, Any], List[Any]]


def _maybe_load_dotenv() -> None:
    # Optional convenience: load .env in local dev without exporting variables each time.
    try:
        from dotenv import load_dotenv  # type: ignore

        load_dotenv()
    except Exception:
        pass


def _vic_headers_from_env() -> Dict[str, str]:
    """
    Flexible auth header config because the portal style varies.

    Examples:
      - x-api-key: <KEY>
        VIC_API_KEY_HEADER=x-api-key
        VIC_API_KEY_PREFIX=

      - Authorization: Bearer <KEY>
        VIC_API_KEY_HEADER=Authorization
        VIC_API_KEY_PREFIX=Bearer␠
    """
    api_key = os.getenv("VIC_API_KEY", "")
    if not api_key:
        return {}

    header = os.getenv("VIC_API_KEY_HEADER", "x-api-key")
    prefix = os.getenv("VIC_API_KEY_PREFIX", "")
    return {header: f"{prefix}{api_key}"}


def _extract_records(payload: JsonType) -> Iterable[Dict[str, Any]]:
    """
    Try to iterate "records" from common API response shapes.
    If we can't find an array of records, yield the payload as a single record.
    """
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                yield item
        return

    # dict shapes
    for k in ("data", "results", "items", "features"):
        v = payload.get(k)
        if isinstance(v, list):
            for item in v:
                if isinstance(item, dict):
                    yield item
            return

    yield payload


def _guess_message_key(record: Dict[str, Any]) -> Optional[str]:
    for k in ("link_id", "linkId", "id", "ID"):
        v = record.get(k)
        if v is None:
            continue
        if isinstance(v, (str, int)):
            return str(v)
    return None


def _delivery_report(err, msg) -> None:
    if err is not None:
        print(f"[kafka] delivery failed: {err}")


def fetch_json(url: str, timeout_seconds: int = 30) -> JsonType:
    resp = requests.get(url, headers=_vic_headers_from_env(), timeout=timeout_seconds)
    resp.raise_for_status()
    return resp.json()


def build_envelope(record: Dict[str, Any], source_url: str) -> Dict[str, Any]:
    return {
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "source": {"type": "vic_bluetooth_api", "url": source_url},
        "payload": record,
    }


def _build_url_from_base(base_url: str, link_id: int) -> str:
    return f"{base_url.rstrip('/')}/links/{link_id}"


def run(
    *,
    topic: str,
    url: str,
    base_url: Optional[str],
    link_ids: Optional[List[int]],
    interval_seconds: int,
    duration_seconds: int,
    timeout_seconds: int,
) -> None:
    producer_cfg = kafka_client_config_from_env()
    producer = Producer(producer_cfg)

    start = time.time()
    polls = 0

    while True:
        elapsed = time.time() - start
        if elapsed >= duration_seconds:
            break

        try:
            urls: List[str]
            if base_url and link_ids:
                urls = [_build_url_from_base(base_url, i) for i in link_ids]
            else:
                urls = [url]

            produced = 0
            for u in urls:
                payload = fetch_json(u, timeout_seconds=timeout_seconds)
                for record in _extract_records(payload):
                    key = _guess_message_key(record)
                    envelope = build_envelope(record, source_url=u)
                    producer.produce(
                        topic=topic,
                        key=key.encode("utf-8") if key is not None else None,
                        value=json.dumps(envelope, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
                        on_delivery=_delivery_report,
                    )
                    produced += 1

            producer.poll(0)
            polls += 1
            print(f"[producer] poll={polls} produced={produced} urls={len(urls)} elapsed={int(elapsed)}s")

        except Exception as e:
            print(f"[producer] fetch/produce error: {e}")

        sleep_for = min(interval_seconds, max(0, duration_seconds - int(time.time() - start)))
        if sleep_for <= 0:
            break
        time.sleep(sleep_for)

    producer.flush(30)


def main() -> None:
    _maybe_load_dotenv()
    parser = argparse.ArgumentParser(description="Poll Victoria Bluetooth API and publish raw records to Kafka.")
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "traffic_raw"))
    parser.add_argument("--url", default=os.getenv("VIC_API_URL", ""))
    parser.add_argument(
        "--base-url",
        default=os.getenv("VIC_API_BASE_URL", ""),
        help="API base URL like https://api.opendata.transport.vic.gov.au/opendata/roads/bluetooth-travel-time/v1",
    )
    parser.add_argument(
        "--link-ids",
        default=os.getenv("VIC_LINK_IDS", ""),
        help="Comma-separated link ids to poll, e.g. 3,9,10. Used only with --base-url (or VIC_API_BASE_URL).",
    )
    parser.add_argument("--interval-seconds", type=int, default=int(os.getenv("POLL_INTERVAL_SECONDS", "300")))
    parser.add_argument("--duration-seconds", type=int, default=int(os.getenv("RUN_DURATION_SECONDS", "600")))
    parser.add_argument("--timeout-seconds", type=int, default=int(os.getenv("HTTP_TIMEOUT_SECONDS", "30")))
    args = parser.parse_args()

    base_url = args.base_url.strip() or None
    link_ids: Optional[List[int]] = None
    if args.link_ids.strip():
        try:
            link_ids = [int(x.strip()) for x in args.link_ids.split(",") if x.strip()]
        except ValueError:
            raise SystemExit("Invalid --link-ids. Provide comma-separated integers, e.g. 3,9,10.")

    if base_url and link_ids:
        url = _build_url_from_base(base_url, link_ids[0])
    else:
        url = args.url.strip()

    if not url:
        raise SystemExit("Missing API URL. Set VIC_API_URL or pass --url, or use --base-url + --link-ids.")

    run(
        topic=args.topic,
        url=url,
        base_url=base_url,
        link_ids=link_ids,
        interval_seconds=args.interval_seconds,
        duration_seconds=args.duration_seconds,
        timeout_seconds=args.timeout_seconds,
    )


if __name__ == "__main__":
    main()

