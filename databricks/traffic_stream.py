"""
Databricks Structured Streaming consumer for the traffic_raw topic.

This script is designed to work *before* you fully know the JSON schema:
- It stores a Bronze Delta table with raw Kafka value strings (replayable).
- It attempts to extract common fields using JSON paths (best-effort).
  Once you’ve inspected `data/raw/*.jsonl`, you can tighten this to a real schema
  using from_json() with an explicit StructType.
"""

from pyspark.sql import functions as F

# =========================
# 1) Source + output tables
# =========================

# If you uploaded your local JSONL dump and created a table from it, set this:
SOURCE_TABLE = "workspace.default.traffic_raw_file_bronze"

# Canonical Delta tables we’ll build
BRONZE_TABLE = "workspace.default.traffic_bronze"
SILVER_TABLE = "workspace.default.traffic_silver"
AGG_TABLE = "workspace.default.traffic_delay_agg"

BRONZE_CHECKPOINT = "/tmp/checkpoints/traffic_bronze"
SILVER_CHECKPOINT = "/tmp/checkpoints/traffic_silver"
AGG_CHECKPOINT = "/tmp/checkpoints/traffic_delay_agg"

# =========================
# 2) Bronze: stream from uploaded table
# =========================

# The uploaded table already has `value_json` (parsed) and `value` (raw string).
bronze_src = spark.readStream.table(SOURCE_TABLE)

bronze = (
    bronze_src.select(
        F.col("topic").cast("string").alias("topic"),
        F.col("partition").cast("int").alias("partition"),
        F.col("offset").cast("long").alias("offset"),
        F.col("timestamp").alias("kafka_timestamp_raw"),
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("value_str"),
        F.col("value_json").alias("value_json"),
        F.col("dumped_at").alias("dumped_at"),
    )
    .withColumn("ingested_at", F.to_timestamp(F.get_json_object("value_str", "$.ingested_at")))
)

bronze_query = (
    bronze.writeStream.format("delta")
    .option("checkpointLocation", BRONZE_CHECKPOINT)
    .outputMode("append")
    .toTable(BRONZE_TABLE)
)

# ======================================
# 3) Best-effort field extraction (Silver)
# ======================================

def j(path: str):
    return F.get_json_object(F.col("value_str"), path)


silver = (
    bronze.withColumn("link_id", F.col("value_json.payload.id").cast("int"))
    .withColumn("road_name", F.coalesce(F.col("value_json.payload.name"), F.col("value_json.payload.public_name")))
    .withColumn("enabled", F.col("value_json.payload.enabled").cast("boolean"))
    .withColumn("draft", F.col("value_json.payload.draft").cast("boolean"))
    .withColumn("interval_start", F.to_timestamp(F.col("value_json.payload.latest_stats.interval_start")))
    .withColumn("delay_seconds", F.col("value_json.payload.latest_stats.delay").cast("double"))
    .withColumn("travel_time_seconds", F.col("value_json.payload.latest_stats.travel_time").cast("double"))
    .withColumn("speed", F.col("value_json.payload.latest_stats.speed").cast("double"))
    .withColumn("enough_data", F.col("value_json.payload.latest_stats.enough_data").cast("boolean"))
    .withColumn("ignored", F.col("value_json.payload.latest_stats.ignored").cast("boolean"))
    .withColumn("closed", F.col("value_json.payload.latest_stats.closed").cast("boolean"))
    .withColumn("expected_missing", F.col("value_json.payload.latest_stats.expected_missing").cast("boolean"))
    .withColumn("travel_time_minutes", (F.col("travel_time_seconds") / F.lit(60.0)))
    .withColumn(
        "congestion_flag",
        (F.col("speed").isNotNull())
        & (F.col("free_flow_speed").isNotNull())
        & (F.col("speed") < F.lit(0.3) * F.col("free_flow_speed")),
    )
)

# Filter: live + good-quality + delay > 60s (per your brief)
silver_filtered = (
    silver.filter(
        (F.col("enabled") == F.lit(True))
        & (F.coalesce(F.col("draft"), F.lit(False)) == F.lit(False))
        & (F.coalesce(F.col("ignored"), F.lit(False)) == F.lit(False))
        & (F.coalesce(F.col("closed"), F.lit(False)) == F.lit(False))
        & (F.coalesce(F.col("expected_missing"), F.lit(False)) == F.lit(False))
        & (F.col("enough_data") == F.lit(True))
        & (F.col("delay_seconds") > F.lit(60.0))
    )
    .withColumn("event_time", F.coalesce(F.col("interval_start"), F.col("ingested_at")))
    .dropDuplicates(["link_id", "interval_start"])
)

silver_query = (
    silver_filtered.writeStream.format("delta")
    .option("checkpointLocation", SILVER_CHECKPOINT)
    .outputMode("append")
    .toTable(SILVER_TABLE)
)

# ==================================
# 4) Sliding window aggregation (Agg)
# ==================================

agg = (
    silver_filtered.withWatermark("event_time", "30 minutes")
    .groupBy(
        F.window(F.col("event_time"), "10 minutes", "5 minutes").alias("w"),
        F.col("road_name"),
    )
    .agg(F.avg("delay_seconds").alias("avg_delay_seconds"))
    .select(
        F.col("w.start").alias("window_start"),
        F.col("w.end").alias("window_end"),
        F.col("road_name"),
        F.col("avg_delay_seconds"),
    )
)

agg_query = (
    agg.writeStream.format("delta")
    .option("checkpointLocation", AGG_CHECKPOINT)
    .outputMode("append")
    .toTable(AGG_TABLE)
)

# In a Databricks notebook, the three queries will run concurrently.
# To stop: bronze_query.stop(); silver_query.stop(); agg_query.stop()

