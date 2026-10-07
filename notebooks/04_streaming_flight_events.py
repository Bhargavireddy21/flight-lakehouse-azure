# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Streaming: live flight status events
# MAGIC Three hops, each its own stream with its own checkpoint:
# MAGIC 1. **bronze**: raw event body from Azure Event Hubs (Kafka endpoint), stored as-is
# MAGIC 2. **silver**: parsed JSON, invalid events dropped, duplicates removed
# MAGIC 3. **gold**: departures and delays per airport in 5-minute windows, upserted
# MAGIC
# MAGIC | Widget | Meaning |
# MAGIC |---|---|
# MAGIC | source | `eventhub` on Azure. `files` reads JSON-lines files from the landing volume (`.../events/`) so the logic can be tested without Event Hubs |
# MAGIC | eventhub_namespace | namespace name only, without .servicebus.windows.net |
# MAGIC | trigger | `available_now` drains what is waiting and stops (cheap). `continuous` keeps running until cancelled |

# COMMAND ----------

import os
import sys

# Make the shared package in ../src importable from this notebook.
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from flightlake.config import LakeConfig

dbutils.widgets.text("catalog", "flightlake")
cfg = LakeConfig(catalog=dbutils.widgets.get("catalog"))

dbutils.widgets.dropdown("source", "eventhub", ["eventhub", "files"])
dbutils.widgets.text("eventhub_namespace", "")
dbutils.widgets.text("eventhub_name", "flight-events")
dbutils.widgets.dropdown("trigger", "available_now", ["available_now", "continuous"])

source = dbutils.widgets.get("source")
trigger = {"availableNow": True} if dbutils.widgets.get("trigger") == "available_now" else {"processingTime": "30 seconds"}
ckpt = f"{cfg.checkpoint_path}/events"

# COMMAND ----------

from pyspark.sql import functions as F

from flightlake import streaming as S

# COMMAND ----------

# MAGIC %md ### Hop 1: Event Hubs -> bronze

# COMMAND ----------

if source == "eventhub":
    # The connection string never appears in code or in Git. It is read from a Databricks secret scope.
    connection = dbutils.secrets.get("flightlake", "eventhub-connection")
    options = S.kafka_options(dbutils.widgets.get("eventhub_namespace"), dbutils.widgets.get("eventhub_name"), connection)
    raw = spark.readStream.format("kafka").options(**options).load().select(
        F.col("value").cast("string").alias("body"),
        F.col("timestamp").alias("enqueued_ts"),
        "partition",
        "offset",
    )
else:
    raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("cloudFiles.schemaLocation", f"{ckpt}/bronze_schema")
        .load(f"{cfg.landing_path}/events/")
        .select(
            F.col("value").alias("body"),
            F.current_timestamp().alias("enqueued_ts"),
            F.lit(0).alias("partition"),
            F.lit(0).cast("long").alias("offset"),
        )
    )

q1 = (
    raw.withColumn("_ingest_ts", F.current_timestamp())
    .writeStream.option("checkpointLocation", f"{ckpt}/bronze")
    .trigger(**trigger)
    .toTable(cfg.bronze_events)
)

# COMMAND ----------

# MAGIC %md ### Hop 2: bronze -> silver (parse, validate, de-duplicate)

# COMMAND ----------

if trigger.get("availableNow"):
    q1.awaitTermination()  # in drain mode run the hops one after another

q2 = (
    S.dedupe_events(S.parse_events(spark.readStream.table(cfg.bronze_events)))
    .writeStream.option("checkpointLocation", f"{ckpt}/silver")
    .trigger(**trigger)
    .toTable(cfg.silver_events)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hop 3: silver -> gold (windowed aggregate)
# MAGIC Update mode emits a window every time it changes, so the sink must upsert. That is the MERGE in `foreachBatch`.

# COMMAND ----------

if trigger.get("availableNow"):
    q2.awaitTermination()

gold_table = cfg.gold("live_airport_delays")


def upsert_windows(batch_df, batch_id):
    from delta.tables import DeltaTable

    session = batch_df.sparkSession
    if not session.catalog.tableExists(gold_table):
        batch_df.limit(0).write.format("delta").saveAsTable(gold_table)
    (
        DeltaTable.forName(session, gold_table)
        .alias("t")
        .merge(batch_df.alias("s"), "t.window_start = s.window_start AND t.origin_airport = s.origin_airport")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


q3 = (
    S.aggregate_airport_delays(spark.readStream.table(cfg.silver_events))
    .writeStream.outputMode("update")
    .foreachBatch(upsert_windows)
    .option("checkpointLocation", f"{ckpt}/gold")
    .trigger(**trigger)
    .start()
)

# COMMAND ----------

if trigger.get("availableNow"):
    q3.awaitTermination()
    print("bronze events:", spark.table(cfg.bronze_events).count())
    print("silver events:", spark.table(cfg.silver_events).count())
    display(spark.sql(f"SELECT * FROM {gold_table} ORDER BY window_start DESC, departures DESC LIMIT 50"))
