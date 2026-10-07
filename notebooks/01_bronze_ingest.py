# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Bronze: raw flights
# MAGIC Auto Loader picks up only the CSV files it has not seen before and appends them, untouched, to a Delta table.
# MAGIC Every column stays a string. Bronze is a faithful copy of the source plus lineage columns.

# COMMAND ----------

import os
import sys

# Make the shared package in ../src importable from this notebook.
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from flightlake.config import LakeConfig

dbutils.widgets.text("catalog", "flightlake")
cfg = LakeConfig(catalog=dbutils.widgets.get("catalog"))

# COMMAND ----------

from flightlake.transforms import add_ingest_metadata, sanitize_columns

source_path = f"{cfg.landing_path}/flights/"
checkpoint = f"{cfg.checkpoint_path}/bronze_flights"

raw = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    # Columns are inferred as strings on the first run and then pinned here.
    .option("cloudFiles.schemaLocation", f"{checkpoint}/_schema")
    # New or unexpected columns go into _rescued_data instead of breaking the stream.
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    # The BTS zip also contains a readme.html. Ignore everything that is not a CSV.
    .option("pathGlobFilter", "*.csv")
    .load(source_path)
)

bronze = sanitize_columns(add_ingest_metadata(raw))

# COMMAND ----------

query = (
    bronze.writeStream.option("checkpointLocation", checkpoint)
    # availableNow = process everything that is waiting, then stop. Streaming semantics, batch cost.
    .trigger(availableNow=True)
    .toTable(cfg.bronze_flights)
)
query.awaitTermination()

# COMMAND ----------

display(
    spark.sql(
        f"SELECT _source_file, count(*) AS rows, max(_ingest_ts) AS ingested_at FROM {cfg.bronze_flights} GROUP BY 1 ORDER BY 1"
    )
)
