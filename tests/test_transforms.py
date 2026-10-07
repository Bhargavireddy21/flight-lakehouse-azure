import pytest
from pyspark.sql import functions as F

from flightlake import transforms as T


def row(**overrides):
    base = {
        "FlightDate": "2024-01-15", "Reporting_Airline": "AS", "Tail_Number": "N123AS",
        "Flight_Number_Reporting_Airline": "301", "Origin": "SEA", "OriginCityName": "Seattle, WA",
        "OriginState": "WA", "Dest": "SFO", "DestCityName": "San Francisco, CA", "DestState": "CA",
        "CRSDepTime": "905", "DepTime": "0925", "DepDelay": "20.00", "CRSArrTime": "1110", "ArrTime": "1135",
        "ArrDelay": "25.00", "Cancelled": "0.00", "CancellationCode": None, "Diverted": "0.00",
        "AirTime": "100.00", "Distance": "679.00", "CarrierDelay": "25.00", "WeatherDelay": None,
        "NASDelay": None, "SecurityDelay": None, "LateAircraftDelay": None,
    }
    base.update(overrides)
    return base


def make(spark, rows):
    schema = ", ".join(f"{c} string" for c in rows[0])
    return spark.createDataFrame([tuple(r.values()) for r in rows], schema)


def test_sanitize_columns(spark):
    df = spark.createDataFrame([(1, 2)], ["Flight Date", "a.b"])
    assert T.sanitize_columns(df).columns == ["Flight_Date", "a_b"]


def test_clean_types_and_derived_columns(spark):
    valid, rejected = T.clean_flights(make(spark, [row()]))
    r = valid.collect()[0]
    assert rejected.count() == 0
    assert str(r.flight_date) == "2024-01-15"
    assert r.crs_dep_time == "0905" and r.crs_dep_hour == 9   # leading zero restored
    assert r.arr_delay_min == 25 and r.is_arr_delayed_15 is True
    assert r.is_cancelled is False and r.route == "SEA-SFO"
    assert r.weather_delay_min == 0                            # NULL cause becomes 0


def test_cancelled_flight_has_null_delay_flag(spark):
    cancelled = row(Cancelled="1.00", CancellationCode="B", DepTime=None, DepDelay=None, ArrTime=None, ArrDelay=None)
    r = T.clean_flights(make(spark, [cancelled]))[0].collect()[0]
    assert r.is_cancelled is True and r.is_arr_delayed_15 is None


@pytest.mark.parametrize(
    "override, reason",
    [
        ({"FlightDate": "not-a-date"}, "missing_flight_date"),
        ({"Origin": "??"}, "bad_origin_code"),
        ({"Dest": None}, "bad_dest_code"),
        ({"Distance": "-1"}, "bad_distance"),
        ({"Reporting_Airline": " "}, "missing_carrier"),
    ],
)
def test_bad_rows_are_quarantined_with_a_reason(spark, override, reason):
    valid, rejected = T.clean_flights(make(spark, [row(**override)]))
    assert valid.count() == 0
    assert reason in rejected.collect()[0]["_reject_reason"]


def test_duplicates_keep_latest_ingested(spark):
    df = make(spark, [row(ArrDelay="5.00"), row(ArrDelay="40.00")])
    df = df.withColumn("_ingest_ts", F.when(F.col("ArrDelay") == "40.00", F.lit("2024-02-02")).otherwise(F.lit("2024-02-01")).cast("timestamp"))
    valid, _ = T.clean_flights(df)
    assert [r.arr_delay_min for r in valid.collect()] == [40]


def test_missing_bronze_column_fails_loudly(spark):
    with pytest.raises(ValueError, match="Distance"):
        T.clean_flights(make(spark, [row()]).drop("Distance"))


# ---- whole-file checks on the sample data

def test_sample_file_reconciles(bronze):
    valid, rejected = T.clean_flights(bronze)
    v, r = valid.count(), rejected.count()
    assert r == 15                                   # 5 bad origin + 5 bad date + 5 bad distance
    assert v + r <= bronze.count()                   # difference = duplicates removed
    assert valid.groupBy(*T.FLIGHT_KEY).count().filter("count > 1").count() == 0


def test_star_schema_has_no_orphans(bronze):
    silver, _ = T.clean_flights(bronze)
    fact = T.build_fact_flights(silver)
    assert fact.count() == silver.count()            # the cancellation-reason join must not fan out
    dim_date, dim_airport, dim_carrier = T.build_dim_date(silver), T.build_dim_airport(silver), T.build_dim_carrier(silver)
    assert fact.join(dim_date, "date_key", "left_anti").count() == 0
    assert fact.join(dim_carrier, "carrier_code", "left_anti").count() == 0
    for fk in ("origin_airport_code", "dest_airport_code"):
        assert fact.join(dim_airport, fact[fk] == dim_airport.airport_code, "left_anti").count() == 0
    for dim, key in ((dim_date, "date_key"), (dim_airport, "airport_code"), (dim_carrier, "carrier_code")):
        assert dim.count() == dim.select(key).distinct().count()


def test_unknown_carrier_falls_back_to_code(spark):
    silver, _ = T.clean_flights(make(spark, [row(Reporting_Airline="ZZ")]))
    assert T.build_dim_carrier(silver).collect()[0].carrier_name == "ZZ"


def test_daily_route_aggregate(spark):
    rows = [
        row(Flight_Number_Reporting_Airline="1", ArrDelay="25.00"),
        row(Flight_Number_Reporting_Airline="2", ArrDelay="5.00", CarrierDelay=None),
        row(Flight_Number_Reporting_Airline="3", Cancelled="1.00", CancellationCode="A", ArrDelay=None, DepDelay=None, CarrierDelay=None),
    ]
    silver, _ = T.clean_flights(make(spark, rows))
    a = T.build_agg_daily_route(T.build_fact_flights(silver)).collect()
    assert len(a) == 1
    a = a[0]
    assert (a.flights, a.cancelled_flights, a.arrived_flights, a.on_time_flights) == (3, 1, 2, 1)
    assert a.avg_arr_delay_min == 15.0 and a.total_carrier_delay_min == 25
