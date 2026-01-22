-- Technical pipeline monitoring (Kafka/Docker metrics via Bronze)
--
-- This table is designed for dashboards:
-- - ingestion rate (records per 5 minutes)
-- - partition distribution (counts per partition)
-- - latency proxies (ingested_at -> dumped_at, interval_start -> dumped_at)
--
-- Notes:
-- - We cannot read "Kafka consumer lag" directly from Databricks SQL in Free Edition.
--   But we can measure end-to-end pipeline latency and throughput from the data we do have.

CREATE OR REFRESH STREAMING TABLE fact_kafka_monitor_5m
AS
WITH base AS (
  SELECT
    -- Use dumped_at for "when this record landed in our lake" (pipeline time)
    CAST(dumped_at AS TIMESTAMP) AS dumped_at,

    -- Producer ingestion time (when your producer published)
    CAST(value_json.ingested_at AS TIMESTAMP) AS ingested_at,

    -- Event time (stats interval time) - may be null for some records
    CAST(value_json.payload.latest_stats.interval_start AS TIMESTAMP) AS interval_start,

    CAST(partition AS INT) AS partition,
    CAST(offset AS BIGINT) AS offset
  FROM STREAM(LIVE.traffic_bronze)
)
SELECT
  window(dumped_at, "5 minutes") AS w,

  COUNT(*) AS records_processed,

  -- Partition distribution
  SUM(CASE WHEN partition = 0 THEN 1 ELSE 0 END) AS partition_0_count,
  SUM(CASE WHEN partition = 1 THEN 1 ELSE 0 END) AS partition_1_count,
  SUM(CASE WHEN partition = 2 THEN 1 ELSE 0 END) AS partition_2_count,

  -- Offsets give a rough sense of progress per partition
  MAX(CASE WHEN partition = 0 THEN offset ELSE NULL END) AS max_offset_p0,
  MAX(CASE WHEN partition = 1 THEN offset ELSE NULL END) AS max_offset_p1,
  MAX(CASE WHEN partition = 2 THEN offset ELSE NULL END) AS max_offset_p2,

  -- Latency: producer -> dump consumer (seconds)
  AVG(unix_timestamp(dumped_at) - unix_timestamp(ingested_at)) AS avg_ingest_to_dump_latency_seconds,
  MAX(unix_timestamp(dumped_at) - unix_timestamp(ingested_at)) AS max_ingest_to_dump_latency_seconds,

  -- Latency: event time -> dump consumer (seconds)
  AVG(unix_timestamp(dumped_at) - unix_timestamp(interval_start)) AS avg_event_to_dump_latency_seconds,
  MAX(unix_timestamp(dumped_at) - unix_timestamp(interval_start)) AS max_event_to_dump_latency_seconds

FROM base
GROUP BY window(dumped_at, "5 minutes");

