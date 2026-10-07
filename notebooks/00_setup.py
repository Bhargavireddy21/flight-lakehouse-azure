# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Setup
# MAGIC Creates the catalog, schemas and volumes. Run once per workspace. Safe to re-run.
# MAGIC
# MAGIC | Widget | Azure | Databricks Free Edition |
# MAGIC |---|---|---|
# MAGIC | catalog | flightlake | workspace |
# MAGIC | managed_location | abfss://lake@STORAGE.dfs.core.windows.net/ | leave empty |
# MAGIC | landing_location | abfss://landing@STORAGE.dfs.core.windows.net/raw | leave empty |

# COMMAND ----------

dbutils.widgets.text("catalog", "flightlake")
dbutils.widgets.text("managed_location", "")
dbutils.widgets.text("landing_location", "")

catalog = dbutils.widgets.get("catalog")
managed_location = dbutils.widgets.get("managed_location").strip()
landing_location = dbutils.widgets.get("landing_location").strip()

# COMMAND ----------

# On Azure the catalog stores its managed tables in OUR ADLS account (needs an external location first, see the guide).
location_clause = f" MANAGED LOCATION '{managed_location}'" if managed_location else ""
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}{location_clause}")

for schema in ["landing", "bronze", "silver", "gold", "meta"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

# Landing zone = files, so it is a volume, not a table.
# On Azure it is an EXTERNAL volume pointing at the container Data Factory writes into.
if landing_location:
    spark.sql(f"CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog}.landing.raw LOCATION '{landing_location}'")
else:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.landing.raw")

# Streaming checkpoints and Auto Loader schema tracking live here.
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.meta.checkpoints")

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {catalog}"))
display(spark.sql(f"SHOW VOLUMES IN {catalog}.landing"))
