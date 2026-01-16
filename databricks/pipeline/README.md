# New Pipeline (Databricks Free Edition) — Traffic Streaming SQL

This folder mirrors the Databricks Free Edition Pipeline layout:

- `explorations/`: optional ad-hoc queries / notebooks (not required)
- `transformations/`: **all** streaming table definitions (SQL)

## What this pipeline does

Source: a table you create in Databricks from uploading your local dump file, e.g.:

- `workspace.default.traffic_raw_file_bronze`

Pipeline outputs (created by SQL in `transformations/`):

- Bronze: `workspace.default.traffic_bronze`
- Silver: `workspace.default.traffic_silver`
- Gold: `workspace.default.traffic_delay_agg`

## How to use in Databricks

1. In Databricks, create a Pipeline and point it at a workspace folder (Databricks generates one for you).
2. Under that pipeline folder, create a `transformations/` folder.
3. Copy the SQL files from this repo’s `databricks/pipeline/transformations/` into your Pipeline’s `transformations/` folder.
4. Run the pipeline (Triggered mode is fine).

## Important: first run / reset behavior

Streaming tables are incremental. If you change definitions or want to reprocess the same uploaded data:
- Use **Reset / Full refresh / Recompute** in the Pipeline UI, then run again.

