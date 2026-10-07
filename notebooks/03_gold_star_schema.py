# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Gold: star schema for BI
# MAGIC `fact_flights` surrounded by `dim_date`, `dim_carrier`, `dim_airport` (used twice: origin and destination),
# MAGIC plus a pre-aggregated `agg_daily_route` table for fast dashboards. Gold is fully rebuilt from silver each run.

# COMMAND ----------

import os
import sys

# Make the shared package in ../src importable from this notebook.
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from flightlake.config import LakeConfig

dbutils.widgets.text("catalog", "flightlake")
cfg = LakeConfig(catalog=dbutils.widgets.get("catalog"))

# COMMAND ----------

from flightlake import transforms as T

silver = spark.table(cfg.silver_flights)
fact = T.build_fact_flights(silver)

tables = {
    "dim_date": T.build_dim_date(silver),
    "dim_carrier": T.build_dim_carrier(silver),
    "dim_airport": T.build_dim_airport(silver),
    "fact_flights": fact,
    "agg_daily_route": T.build_agg_daily_route(fact),
}

for name, df in tables.items():
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(cfg.gold(name))
    print(f"{cfg.gold(name)}: {spark.table(cfg.gold(name)).count():,} rows")

# COMMAND ----------

# Fail the run (and therefore the Data Factory pipeline) if the model is broken.
fact_t = spark.table(cfg.gold("fact_flights"))
assert fact_t.count() == silver.count(), "fact row count must equal silver row count"
for dim, fk, pk in [
    ("dim_date", "date_key", "date_key"),
    ("dim_carrier", "carrier_code", "carrier_code"),
    ("dim_airport", "origin_airport_code", "airport_code"),
    ("dim_airport", "dest_airport_code", "airport_code"),
]:
    d = spark.table(cfg.gold(dim))
    orphans = fact_t.join(d, fact_t[fk] == d[pk], "left_anti").count()
    assert orphans == 0, f"{orphans} fact rows have no match in {dim} on {fk}"
print("gold checks passed")

# COMMAND ----------

# Co-locate the columns dashboards filter on, so queries skip files they do not need.
spark.sql(f"OPTIMIZE {cfg.gold('fact_flights')} ZORDER BY (date_key, carrier_code)")

# COMMAND ----------

display(
    spark.sql(f"""
    SELECT c.carrier_name,
           sum(a.flights)                                              AS flights,
           round(100 * sum(a.on_time_flights) / sum(a.arrived_flights), 1) AS on_time_pct,
           round(100 * sum(a.cancelled_flights) / sum(a.flights), 2)   AS cancel_pct
    FROM {cfg.gold('agg_daily_route')} a
    JOIN {cfg.gold('dim_carrier')} c USING (carrier_code)
    GROUP BY 1 ORDER BY on_time_pct DESC
    """)
)
