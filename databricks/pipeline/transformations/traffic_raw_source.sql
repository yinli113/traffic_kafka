-- DLT source unification (merge multiple uploaded tables)
--
-- DLT pipelines do NOT allow CREATE OR REPLACE TABLE/VIEW.
-- Use a DLT statement instead: CREATE OR REFRESH STREAMING TABLE.
--
-- Edit the two source table names below to match your Databricks uploads.

CREATE OR REFRESH STREAMING TABLE traffic_raw_source
AS
-- IMPORTANT: don't UNION the parsed `value_json` struct because uploaded tables can infer
-- slightly different nested types (e.g., incidents array). Instead, UNION stable columns
-- and keep the raw JSON string `value` for downstream parsing.

SELECT
  CAST(topic AS STRING) AS topic,
  CAST(partition AS INT) AS partition,
  CAST(offset AS BIGINT) AS offset,
  CAST(timestamp AS STRING) AS kafka_timestamp_raw,
  CAST(key AS STRING) AS kafka_key,
  CAST(value AS STRING) AS value_str,
  CAST(dumped_at AS TIMESTAMP) AS dumped_at
FROM STREAM(workspace.default.traffic_raw_file1)
UNION ALL
SELECT
  CAST(topic AS STRING) AS topic,
  CAST(partition AS INT) AS partition,
  CAST(offset AS BIGINT) AS offset,
  CAST(timestamp AS STRING) AS kafka_timestamp_raw,
  CAST(key AS STRING) AS kafka_key,
  CAST(value AS STRING) AS value_str,
  CAST(dumped_at AS TIMESTAMP) AS dumped_at
FROM STREAM(workspace.default.traffic_raw_file2);

