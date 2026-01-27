import os
import time
from pathlib import Path

try:
    from scripts.load_to_sqlite import _maybe_load_dotenv, run as load_run
except ModuleNotFoundError:  # pragma: no cover - allow direct script execution
    from load_to_sqlite import _maybe_load_dotenv, run as load_run


def main() -> None:
    _maybe_load_dotenv()
    jsonl_dir = Path(os.getenv("JSONL_DIR", "./data/raw"))
    db_path = Path(os.getenv("SQLITE_DB_PATH", "./data/traffic.db"))
    topic = os.getenv("KAFKA_TOPIC")
    interval_seconds = int(os.getenv("ETL_INTERVAL_SECONDS", "300"))

    print(
        f"[etl] starting jsonl_dir={jsonl_dir} db_path={db_path} "
        f"topic={topic or '*'} interval_seconds={interval_seconds}"
    )

    while True:
        load_run(jsonl_dir=jsonl_dir, db_path=db_path, topic=topic)
        time.sleep(interval_seconds)


if __name__ == "__main__":
    main()
