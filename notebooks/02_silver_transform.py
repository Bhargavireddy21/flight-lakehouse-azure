# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Silver: clean, validated, de-duplicated flights
# MAGIC Reads only the bronze rows added since the last run (the checkpoint remembers), applies types and quality rules,
# MAGIC then MERGEs into silver so re-running or re-ingesting a file never creates duplicates.
# MAGIC Rows that fail a rule go to a quarantine table with the reason.

# COMMAND ----------

import os
import sys

# Make the shared package in ../src importable from this notebook.
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from flightlake.config import LakeConfig

dbutils.widgets.text("catalog", "flightlake")
cfg = LakeConfig(catalog=dbutils.widgets.get("catalog"))

# COMMAND ----------

from flightlake.transforms import FLIGHT_KEY

checkpoint = f"{cfg.checkpoint_path}/silver_flights"
merge_condition = " AND ".join(f"t.{c} = s.{c}" for c in FLIGHT_KEY)

# Plain strings only below this line. foreachBatch can run in a separate Python process
# (serverless / shared compute), so the function re-adds ../src to the path and imports inside.
SRC_PATH = os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
SILVER_TABLE = cfg.silver_flights
QUARANTINE_TABLE = cfg.silver_quarantine


def upsert_to_silver(batch_df, batch_id):
    import sys

    if SRC_PATH not in sys.path:
        sys.path.append(SRC_PATH)
    from delta.tables import DeltaTable

    from flightlake.transforms import clean_flights

    valid, rejected = clean_flights(batch_df)
    session = batch_df.sparkSession

    if not session.catalog.tableExists(SILVER_TABLE):
        valid.limit(0).write.format("delta").saveAsTable(SILVER_TABLE)

    (
        DeltaTable.forName(session, SILVER_TABLE)
        .alias("t")
        .merge(valid.alias("s"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    rejected.write.format("delta").mode("append").saveAsTable(QUARANTINE_TABLE)


# COMMAND ----------

query = (
    spark.readStream.table(cfg.bronze_flights)
    .writeStream.foreachBatch(upsert_to_silver)
    .option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()

# COMMAND ----------

# MAGIC %md ### Reconciliation: what came in, what was kept, what was rejected and why

# COMMAND ----------

bronze_rows = spark.table(cfg.bronze_flights).count()
silver_rows = spark.table(cfg.silver_flights).count()
rejected_rows = spark.table(cfg.silver_quarantine).count() if spark.catalog.tableExists(cfg.silver_quarantine) else 0
print(f"bronze={bronze_rows:,}  silver={silver_rows:,}  quarantined={rejected_rows:,}  duplicates removed={bronze_rows - silver_rows - rejected_rows:,}")

if rejected_rows:
    display(spark.sql(f"SELECT _reject_reason, count(*) AS rows FROM {cfg.silver_quarantine} GROUP BY 1 ORDER BY 2 DESC"))
