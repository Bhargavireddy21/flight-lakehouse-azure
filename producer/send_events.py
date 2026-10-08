"""Replay sample flights as live status events.

Send to Azure Event Hubs (needs EVENTHUB_CONNECTION_STRING and EVENTHUB_NAME in the environment):
    python producer/send_events.py --count 500 --rate 20

Or write JSON lines to a file, to test the streaming notebook without Event Hubs:
    python producer/send_events.py --count 500 --out events.jsonl

~2% of events are sent twice on purpose so the de-duplication step has work to do.
"""
import argparse
import csv
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

SAMPLE = os.path.join(os.path.dirname(__file__), "..", "sample_data", "flights_sample.csv")


def build_events(count: int, seed: int = 7):
    rng = random.Random(seed)
    with open(SAMPLE, newline="") as f:
        flights = [r for r in csv.DictReader(f) if len(r["Origin"]) == 3]
    for _ in range(count):
        fl = rng.choice(flights)
        status = rng.choices(["DEPARTED", "ARRIVED", "CANCELLED"], weights=[60, 35, 5])[0]
        delay = None if status == "CANCELLED" else max(-10, int(rng.expovariate(1 / 18)) - 8)
        yield {
            "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
            "event_ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "carrier_code": fl["Reporting_Airline"],
            "flight_number": fl["Flight_Number_Reporting_Airline"],
            "origin_airport": fl["Origin"],
            "dest_airport": fl["Dest"],
            "status": status,
            "delay_min": delay,
        }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--count", type=int, default=500)
    p.add_argument("--rate", type=float, default=20, help="events per second (Event Hubs mode)")
    p.add_argument("--out", help="write JSON lines to this file instead of sending to Event Hubs")
    args = p.parse_args()
    rng = random.Random(1)

    if args.out:
        with open(args.out, "w") as f:
            for e in build_events(args.count):
                for _ in range(2 if rng.random() < 0.02 else 1):
                    f.write(json.dumps(e) + "\n")
        print(f"wrote events to {args.out}")
        return

    from azure.eventhub import EventData, EventHubProducerClient  # pip install azure-eventhub

    client = EventHubProducerClient.from_connection_string(
        os.environ["EVENTHUB_CONNECTION_STRING"], eventhub_name=os.environ.get("EVENTHUB_NAME", "flight-events")
    )
    sent = 0
    with client:
        for e in build_events(args.count):
            batch = client.create_batch(partition_key=e["origin_airport"])
            for _ in range(2 if rng.random() < 0.02 else 1):
                batch.add(EventData(json.dumps(e)))
                sent += 1
            client.send_batch(batch)
            time.sleep(1 / args.rate)
    print(f"sent {sent} events")


if __name__ == "__main__":
    main()
