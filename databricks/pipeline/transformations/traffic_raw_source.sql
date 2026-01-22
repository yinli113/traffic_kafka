-- DLT source unification (merge multiple uploaded tables)
--
-- DLT pipelines do NOT allow CREATE OR REPLACE TABLE/VIEW.
-- Use a DLT statement instead: CREATE OR REFRESH STREAMING TABLE.
--
-- Edit the two source table names below to match your Databricks uploads.

CREATE OR REFRESH STREAMING TABLE traffic_raw_source
AS
SELECT * FROM STREAM(workspace.default.traffic_raw_file1)
UNION ALL
SELECT * FROM STREAM(workspace.default.traffic_raw_file2);

