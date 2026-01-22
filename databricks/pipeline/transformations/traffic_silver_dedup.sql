-- Silver (deduplicated)
--
-- Deduplicate by natural key: (link_id, interval_start)
-- Keep the latest record by dumped_at.

CREATE OR REFRESH STREAMING TABLE traffic_silver_dedup
AS
SELECT * EXCEPT (rn)
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY link_id, interval_start
      ORDER BY dumped_at DESC
    ) AS rn
  FROM STREAM(LIVE.traffic_silver)
)
WHERE rn = 1;

