-- Bronze streaming table
--
-- Source is the table created from your uploaded JSONL dump.
-- Adjust the source table name if your upload created a different one.

CREATE OR REFRESH STREAMING TABLE traffic_bronze
AS
SELECT
  topic,
  CAST(partition AS INT)   AS partition,
  CAST(offset AS BIGINT)   AS offset,
  CAST(key AS STRING)      AS kafka_key,
  CAST(value AS STRING)    AS value_str,
  value_json,
  dumped_at
FROM STREAM(workspace.default.traffic_raw_file_bronze);

