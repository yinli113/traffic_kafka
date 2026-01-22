-- Gold streaming table: average delay per road name over sliding window

CREATE OR REFRESH STREAMING TABLE traffic_delay_agg
AS
SELECT
  window(interval_start, "10 minutes", "5 minutes") AS w,
  road_name,
  AVG(delay_seconds) AS avg_delay_seconds
FROM STREAM(LIVE.traffic_silver_dedup)
GROUP BY window(interval_start, "10 minutes", "5 minutes"), road_name;

