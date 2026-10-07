"""Streaming transformations for live flight status events (Event Hubs -> bronze -> silver -> gold)."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

EVENT_SCHEMA = """
    event_id string,
    event_ts timestamp,
    carrier_code string,
    flight_number string,
    origin_airport string,
    dest_airport string,
    status string,
    delay_min int
"""

VALID_STATUSES = ["DEPARTED", "ARRIVED", "CANCELLED"]


def kafka_options(namespace: str, event_hub: str, connection_string: str) -> dict:
    """Options for Spark's built-in Kafka source pointed at the Event Hubs Kafka endpoint.

    Event Hubs speaks the Kafka protocol on port 9093 (Standard tier and above), so no extra
    library is needed on the cluster. 'kafkashaded.' is the Databricks-shaded class prefix.
    """
    jaas = (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="$ConnectionString" password="{connection_string}";'
    )
    return {
        "kafka.bootstrap.servers": f"{namespace}.servicebus.windows.net:9093",
        "subscribe": event_hub,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": jaas,
        "startingOffsets": "earliest",
        "failOnDataLoss": "false",
    }


def parse_events(raw: DataFrame, body_col: str = "body") -> DataFrame:
    """JSON string -> typed columns. Malformed or incomplete events are dropped here."""
    parsed = raw.withColumn("e", F.from_json(F.col(body_col), EVENT_SCHEMA)).select("e.*")
    return parsed.filter(
        F.col("event_id").isNotNull()
        & F.col("event_ts").isNotNull()
        & F.col("origin_airport").isNotNull()
        & F.col("status").isin(VALID_STATUSES)
    )


def dedupe_events(events: DataFrame, watermark: str = "10 minutes") -> DataFrame:
    """Event Hubs is at-least-once, so the same event can arrive twice. Drop repeats by event_id.

    The watermark bounds how long Spark remembers ids, otherwise state grows forever.
    """
    return events.withWatermark("event_ts", watermark).dropDuplicates(["event_id", "event_ts"])


def aggregate_airport_delays(events: DataFrame, window: str = "5 minutes", watermark: str = "10 minutes") -> DataFrame:
    """Departures per origin airport in tumbling windows."""
    return (
        events.withWatermark("event_ts", watermark)
        .groupBy(F.window("event_ts", window).alias("w"), F.col("origin_airport"))
        .agg(
            F.sum(F.when(F.col("status") == "DEPARTED", 1).otherwise(0)).alias("departures"),
            F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancellations"),
            F.round(F.avg(F.when(F.col("status") == "DEPARTED", F.col("delay_min"))), 2).alias("avg_dep_delay_min"),
            F.max(F.when(F.col("status") == "DEPARTED", F.col("delay_min"))).alias("max_dep_delay_min"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "origin_airport",
            "departures",
            "cancellations",
            "avg_dep_delay_min",
            "max_dep_delay_min",
        )
    )
