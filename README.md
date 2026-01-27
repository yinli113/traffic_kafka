# Victoria Real-Time Traffic Pipeline

Learn Kafka by building a practical streaming pipeline on real traffic data from Victoria Transport Open Data.

- **Source**: Bluetooth Travel Time API
- **Buffer**: Kafka-compatible broker (local Redpanda → Confluent Cloud later)
- **Processing**: Databricks Streaming Tables (SQL) + Delta
- **Outputs**: Kafka monitoring KPIs + traffic performance dashboard

## Architecture

![Pipeline overview](https://github.com/yinli113/traffic_kafka/blob/main/image/pipeline-graph.png?raw=1)

## API access

1. Open the Transport Victoria Open Data portal for **Bluetooth Travel Time**.
2. Generate an API key.
3. Copy the endpoint URL you want to poll.

Auth header formats vary by portal:

- `x-api-key: <KEY>` → set `VIC_API_KEY_HEADER=x-api-key`, `VIC_API_KEY_PREFIX=` (empty)
- `Authorization: Bearer <KEY>` → set `VIC_API_KEY_HEADER=Authorization`, `VIC_API_KEY_PREFIX=Bearer `

## Quickstart (local Kafka)

Prereqs: Docker Desktop installed and running.

```bash
docker compose up -d
docker exec -it traffic_redpanda rpk topic create traffic_raw --partitions 3 --replicas 1 --brokers localhost:9092
```

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 1) Run the consumer (dump messages to local JSONL)

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
python -m src.consumer_dump --topic traffic_raw --out-dir ./data/raw
```

### 2) Run the producer (poll API and publish raw JSON)

```bash
export VIC_API_URL="PASTE_THE_BLUETOOTH_TRAVEL_TIME_ENDPOINT_HERE"
export VIC_API_KEY="PASTE_YOUR_KEY_HERE"
export VIC_API_KEY_HEADER="x-api-key"          # or Authorization
export VIC_API_KEY_PREFIX=""                   # or "Bearer "
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
python -m src.producer_poll --topic traffic_raw --interval-seconds 300 --duration-seconds 600
```

Afterward, inspect the `.jsonl` files in `./data/raw` to understand the schema.

## Kafka reliability check (one command)

```bash
source .venv/bin/activate
python -m src.status_check --topic traffic_raw --group-id traffic_dump --out-dir ./data/raw --window-minutes 5 --since-minutes 120
```

See: `docs/kafka_status_check.md` for offsets, lag, and troubleshooting.

## Databricks Free Edition (file-based streaming)

Use this when you want to practice streaming tables without Kafka connectivity.

1. Upload `data/raw/traffic_raw_*.jsonl` to Databricks and create a table.
2. Recommended table name: `workspace.default.traffic_raw_file_bronze`.
3. Create a Databricks Pipeline and add the SQL files:
   - `databricks/pipeline/transformations/traffic_bronze.sql`
   - `databricks/pipeline/transformations/traffic_silver.sql`
   - `databricks/pipeline/transformations/traffic_silver_dedup.sql`
   - `databricks/pipeline/transformations/dim_link.sql`
   - `databricks/pipeline/transformations/fact_link_interval.sql`
   - `databricks/pipeline/transformations/fact_kafka_monitor_5m.sql`
   - `databricks/pipeline/transformations/traffic_delay_agg.sql`
4. Run the pipeline (use **Reset/Full refresh** to reprocess uploads).

## Local SQLite + Streamlit demo

Use this to explore a “real-time” dashboard locally before moving to Databricks.

1. Load JSONL dumps into SQLite (one-time or on demand):

```bash
python scripts/load_to_sqlite.py --jsonl-dir ./data/raw --db-path ./data/traffic.db
```

2. Optional: run a tiny ETL loop that updates every 5 minutes:

```bash
python -m scripts.etl_sqlite_loop
```

3. Start the Streamlit app and choose dashboards from the sidebar:

```bash
streamlit run app/streamlit_app.py
```

The app reads from `SQLITE_DB_PATH` (default: `./data/traffic.db`).

## Streamlit Community Cloud (public demo)

GitHub Pages can’t host Streamlit (it needs a Python server). Use Streamlit Community Cloud:

1. Push this repo to GitHub.
2. Go to Streamlit Community Cloud → **New app**.
3. Select this repo, branch `main`, and set **Main file path** to `app/streamlit_app.py`.
4. Deploy.

Notes:
- The app will auto-load a tiny sample dataset from `data/sample/traffic_sample.jsonl` if no DB exists.
- You can override with env vars in Streamlit Cloud:
  - `SQLITE_DB_PATH=./data/traffic.db`
  - `SAMPLE_JSONL_PATH=./data/sample/traffic_sample.jsonl`

## Example dashboards

Traffic performance:

![Traffic performance dashboard](https://github.com/yinli113/traffic_kafka/blob/main/image/traffic-performance.png?raw=1)

Pipeline monitoring:

![Kafka monitoring dashboard](https://github.com/yinli113/traffic_kafka/blob/main/image/kafka-monitor.png?raw=1)

## Confluent Cloud (later)

Add these env vars in addition to `KAFKA_BOOTSTRAP_SERVERS`:

```bash
export KAFKA_SECURITY_PROTOCOL=SASL_SSL
export KAFKA_SASL_MECHANISM=PLAIN
export KAFKA_SASL_USERNAME="YOUR_CONFLUENT_API_KEY"
export KAFKA_SASL_PASSWORD="YOUR_CONFLUENT_API_SECRET"
```

## Repo structure

- `src/`: producer, consumer, and status tools
- `databricks/`: DLT SQL pipeline and notes
- `docs/`: Kafka status check guide
- `image/`: dashboard screenshots


