"""Batch transformations: bronze -> silver -> gold.

Every function takes DataFrames and returns DataFrames. No reads, no writes, no SparkSession lookups
(except where a tiny reference DataFrame has to be created). That is what makes them testable locally.
"""
import re

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from flightlake.reference import CANCELLATION_REASONS, CARRIERS

# Natural key of one scheduled flight. Used for de-duplication and for the silver MERGE.
FLIGHT_KEY = ["flight_date", "carrier_code", "flight_number", "origin_airport", "dest_airport", "crs_dep_time"]

# silver column -> (bronze column, target type)
_SILVER_COLUMNS = {
    "flight_date": ("FlightDate", "date"),
    "carrier_code": ("Reporting_Airline", "string"),
    "tail_number": ("Tail_Number", "string"),
    "flight_number": ("Flight_Number_Reporting_Airline", "string"),
    "origin_airport": ("Origin", "string"),
    "origin_city": ("OriginCityName", "string"),
    "origin_state": ("OriginState", "string"),
    "dest_airport": ("Dest", "string"),
    "dest_city": ("DestCityName", "string"),
    "dest_state": ("DestState", "string"),
    "crs_dep_time": ("CRSDepTime", "string"),
    "dep_time": ("DepTime", "string"),
    "dep_delay_min": ("DepDelay", "double"),
    "crs_arr_time": ("CRSArrTime", "string"),
    "arr_time": ("ArrTime", "string"),
    "arr_delay_min": ("ArrDelay", "double"),
    "cancelled_flag": ("Cancelled", "double"),
    "cancellation_code": ("CancellationCode", "string"),
    "diverted_flag": ("Diverted", "double"),
    "air_time_min": ("AirTime", "double"),
    "distance_miles": ("Distance", "double"),
    "carrier_delay_min": ("CarrierDelay", "double"),
    "weather_delay_min": ("WeatherDelay", "double"),
    "nas_delay_min": ("NASDelay", "double"),
    "security_delay_min": ("SecurityDelay", "double"),
    "late_aircraft_delay_min": ("LateAircraftDelay", "double"),
}

DELAY_CAUSE_COLUMNS = [
    "carrier_delay_min",
    "weather_delay_min",
    "nas_delay_min",
    "security_delay_min",
    "late_aircraft_delay_min",
]


def sanitize_columns(df: DataFrame) -> DataFrame:
    """Delta rejects column names with spaces or punctuation. Replace anything odd with '_'."""
    return df.toDF(*[re.sub(r"[^0-9a-zA-Z_]", "_", c) for c in df.columns])


def add_ingest_metadata(df: DataFrame, source_file_col: str = "_metadata.file_path") -> DataFrame:
    """Lineage columns added in bronze: when the row arrived and which file it came from."""
    return df.withColumn("_ingest_ts", F.current_timestamp()).withColumn("_source_file", F.col(source_file_col))


def _hhmm(col_name: str):
    """BTS times are 'hhmm' with leading zeros dropped or quoted ('5', '0905', '1430'). Normalise to 4 chars."""
    digits = F.regexp_replace(F.trim(F.col(col_name)), r"[^0-9]", "")
    return F.when(F.length(digits) > 0, F.lpad(digits, 4, "0"))


def clean_flights(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Type, validate and de-duplicate raw flights.

    Returns (valid, rejected). Rejected rows keep the original values plus a `_reject_reason`
    so bad data is quarantined and visible instead of silently dropped.
    """
    missing = [src for src, _ in _SILVER_COLUMNS.values() if src not in bronze.columns]
    if missing:
        raise ValueError(f"Bronze is missing expected columns: {missing}")

    has_ingest_ts = "_ingest_ts" in bronze.columns
    selected = []
    for target, (source, dtype) in _SILVER_COLUMNS.items():
        if target in ("crs_dep_time", "dep_time", "crs_arr_time", "arr_time"):
            selected.append(_hhmm(source).alias(target))
        elif dtype == "string":
            trimmed = F.trim(F.col(source))
            selected.append(F.when(F.length(trimmed) > 0, trimmed).alias(target))
        else:
            # try_cast returns NULL on garbage instead of failing the whole job
            selected.append(F.expr(f"try_cast(`{source}` as {dtype})").alias(target))
    selected.append((F.col("_ingest_ts") if has_ingest_ts else F.current_timestamp()).alias("_ingest_ts"))

    typed = bronze.select(*selected)

    rules = {
        "missing_flight_date": F.col("flight_date").isNull(),
        "missing_carrier": F.col("carrier_code").isNull(),
        "missing_flight_number": F.col("flight_number").isNull(),
        "missing_crs_dep_time": F.col("crs_dep_time").isNull(),
        "bad_origin_code": ~F.coalesce(F.col("origin_airport").rlike("^[A-Z]{3}$"), F.lit(False)),
        "bad_dest_code": ~F.coalesce(F.col("dest_airport").rlike("^[A-Z]{3}$"), F.lit(False)),
        "bad_distance": ~F.coalesce(F.col("distance_miles") > 0, F.lit(False)),
    }
    reasons = F.concat_ws(",", *[F.when(cond, F.lit(name)) for name, cond in rules.items()])
    checked = typed.withColumn("_reject_reason", reasons)

    rejected = checked.filter(F.col("_reject_reason") != "")

    # Keep the most recently ingested copy of each flight.
    latest_first = Window.partitionBy(*FLIGHT_KEY).orderBy(F.col("_ingest_ts").desc())
    valid = (
        checked.filter(F.col("_reject_reason") == "")
        .drop("_reject_reason")
        .withColumn("_rn", F.row_number().over(latest_first))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    valid = (
        valid.withColumn("is_cancelled", F.coalesce(F.col("cancelled_flag"), F.lit(0.0)) == 1.0)
        .withColumn("is_diverted", F.coalesce(F.col("diverted_flag"), F.lit(0.0)) == 1.0)
        .drop("cancelled_flag", "diverted_flag")
        .withColumn("dep_delay_min", F.col("dep_delay_min").cast("int"))
        .withColumn("arr_delay_min", F.col("arr_delay_min").cast("int"))
        # Industry definition: a flight is "delayed" when it arrives 15+ minutes late.
        # NULL for flights that never arrived (cancelled / diverted without an arrival time).
        .withColumn("is_arr_delayed_15", F.when(F.col("arr_delay_min").isNotNull(), F.col("arr_delay_min") >= 15))
        .withColumn("crs_dep_hour", F.substring("crs_dep_time", 1, 2).cast("int"))
        .withColumn("route", F.concat_ws("-", "origin_airport", "dest_airport"))
    )
    for c in DELAY_CAUSE_COLUMNS:
        valid = valid.withColumn(c, F.coalesce(F.col(c), F.lit(0.0)).cast("int"))
    return valid, rejected


# ----------------------------------------------------------------------------- gold: star schema


def build_dim_date(silver: DataFrame) -> DataFrame:
    d = silver.select("flight_date").distinct()
    return d.select(
        F.date_format("flight_date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("flight_date").alias("date"),
        F.year("flight_date").alias("year"),
        F.quarter("flight_date").alias("quarter"),
        F.month("flight_date").alias("month"),
        F.date_format("flight_date", "MMMM").alias("month_name"),
        F.dayofmonth("flight_date").alias("day_of_month"),
        # dayofweek: 1 = Sunday ... 7 = Saturday
        F.dayofweek("flight_date").alias("day_of_week"),
        F.date_format("flight_date", "EEEE").alias("day_name"),
        F.dayofweek("flight_date").isin(1, 7).alias("is_weekend"),
    )


def build_dim_airport(silver: DataFrame) -> DataFrame:
    origins = silver.select(
        F.col("origin_airport").alias("airport_code"), F.col("origin_city").alias("city"), F.col("origin_state").alias("state")
    )
    dests = silver.select(
        F.col("dest_airport").alias("airport_code"), F.col("dest_city").alias("city"), F.col("dest_state").alias("state")
    )
    # One row per airport. max() ignores NULLs, so a missing city on one row does not win.
    return origins.unionByName(dests).groupBy("airport_code").agg(F.max("city").alias("city"), F.max("state").alias("state"))


def build_dim_carrier(silver: DataFrame) -> DataFrame:
    spark = silver.sparkSession
    names = spark.createDataFrame(list(CARRIERS.items()), "carrier_code string, carrier_name string")
    codes = silver.select("carrier_code").distinct()
    # Unknown carriers still get a row, so the fact table never has an orphan key.
    return codes.join(F.broadcast(names), "carrier_code", "left").withColumn(
        "carrier_name", F.coalesce("carrier_name", "carrier_code")
    )


def build_fact_flights(silver: DataFrame) -> DataFrame:
    spark = silver.sparkSession
    reasons = spark.createDataFrame(list(CANCELLATION_REASONS.items()), "cancellation_code string, cancellation_reason string")
    return silver.join(F.broadcast(reasons), "cancellation_code", "left").select(
        F.date_format("flight_date", "yyyyMMdd").cast("int").alias("date_key"),
        "carrier_code",
        F.col("origin_airport").alias("origin_airport_code"),
        F.col("dest_airport").alias("dest_airport_code"),
        "flight_number",
        "tail_number",
        "route",
        "crs_dep_time",
        "crs_dep_hour",
        "dep_delay_min",
        "arr_delay_min",
        "is_arr_delayed_15",
        "is_cancelled",
        "cancellation_reason",
        "is_diverted",
        "air_time_min",
        "distance_miles",
        *DELAY_CAUSE_COLUMNS,
    )


def build_agg_daily_route(fact: DataFrame) -> DataFrame:
    """Pre-aggregated table for dashboards: one row per day, carrier and route."""
    arrived = F.col("arr_delay_min").isNotNull()
    return fact.groupBy("date_key", "carrier_code", "origin_airport_code", "dest_airport_code").agg(
        F.count("*").alias("flights"),
        F.sum(F.col("is_cancelled").cast("int")).alias("cancelled_flights"),
        F.sum(F.col("is_diverted").cast("int")).alias("diverted_flights"),
        F.sum(arrived.cast("int")).alias("arrived_flights"),
        F.sum(F.when(arrived & (F.col("arr_delay_min") < 15), 1).otherwise(0)).alias("on_time_flights"),
        F.round(F.avg("arr_delay_min"), 2).alias("avg_arr_delay_min"),
        F.round(F.avg("dep_delay_min"), 2).alias("avg_dep_delay_min"),
        *[F.sum(c).alias(f"total_{c}") for c in DELAY_CAUSE_COLUMNS],
    )
