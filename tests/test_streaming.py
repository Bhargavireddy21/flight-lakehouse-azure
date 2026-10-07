import json

from flightlake import streaming as S


def event(**o):
    base = {"event_id": "e1", "event_ts": "2024-01-15T09:01:00Z", "carrier_code": "AS", "flight_number": "301",
            "origin_airport": "SEA", "dest_airport": "SFO", "status": "DEPARTED", "delay_min": 10}
    base.update(o)
    return json.dumps(base)


def bodies(spark, values):
    return spark.createDataFrame([(v,) for v in values], "body string")


def test_parse_drops_malformed_and_invalid(spark):
    raw = bodies(spark, [event(), "{not json", event(event_id="e2", status="BOARDING"), event(event_id=None)])
    out = S.parse_events(raw).collect()
    assert [r.event_id for r in out] == ["e1"]
    assert out[0].delay_min == 10 and out[0].event_ts.minute == 1


def test_dedupe(spark):
    raw = bodies(spark, [event(), event(), event(event_id="e2")])
    assert S.dedupe_events(S.parse_events(raw)).count() == 2


def test_window_aggregation(spark):
    raw = bodies(spark, [
        event(event_id="a", event_ts="2024-01-15T09:01:00Z", delay_min=10),
        event(event_id="b", event_ts="2024-01-15T09:04:00Z", delay_min=30),
        event(event_id="c", event_ts="2024-01-15T09:04:30Z", status="CANCELLED", delay_min=None),
        event(event_id="d", event_ts="2024-01-15T09:06:00Z", delay_min=0),          # next window
        event(event_id="e", event_ts="2024-01-15T09:02:00Z", origin_airport="DEN"),  # other airport
    ])
    out = {(r.origin_airport, r.window_start.minute): r for r in S.aggregate_airport_delays(S.parse_events(raw)).collect()}
    assert len(out) == 3
    sea = out[("SEA", 0)]
    assert (sea.departures, sea.cancellations, sea.avg_dep_delay_min, sea.max_dep_delay_min) == (2, 1, 20.0, 30)
    assert out[("SEA", 5)].departures == 1


def test_kafka_options_point_at_event_hubs():
    o = S.kafka_options("myns", "flight-events", "Endpoint=sb://x/;SharedAccessKey=abc")
    assert o["kafka.bootstrap.servers"] == "myns.servicebus.windows.net:9093"
    assert o["subscribe"] == "flight-events" and 'username="$ConnectionString"' in o["kafka.sasl.jaas.config"]


def test_runs_as_a_real_structured_stream(spark, tmp_path):
    """Same functions, but through readStream/writeStream, to prove watermark + update mode are valid."""
    src = tmp_path / "in"
    src.mkdir()
    (src / "events.jsonl").write_text("\n".join([event(), event(), event(event_id="e2", delay_min=30)]))

    def stream():
        return spark.readStream.text(str(src)).withColumnRenamed("value", "body")

    q1 = (
        S.dedupe_events(S.parse_events(stream()))
        .writeStream.format("memory").queryName("silver_events").trigger(availableNow=True).start()
    )
    q2 = (
        S.aggregate_airport_delays(S.parse_events(stream()))
        .writeStream.format("memory").queryName("gold_windows").outputMode("update").trigger(availableNow=True).start()
    )
    q1.awaitTermination()
    q2.awaitTermination()
    assert spark.table("silver_events").count() == 2
    # The aggregate has no dedupe in front of it here, so the repeated event is counted: 3 departures.
    assert spark.table("gold_windows").collect()[0].departures == 3
