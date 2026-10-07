"""Generate a small SYNTHETIC flights CSV with the same column names as the BTS on-time file.

Used for unit tests, local development and the Databricks Free Edition dry run.
It is NOT real data. The Azure pipeline ingests the real BTS files through Data Factory.

    python scripts/generate_sample_data.py --rows 2000 --out sample_data/flights_sample.csv
"""
import argparse
import csv
import random
from datetime import date, timedelta

AIRPORTS = [
    ("SEA", "Seattle, WA", "WA"), ("SFO", "San Francisco, CA", "CA"), ("LAX", "Los Angeles, CA", "CA"),
    ("DEN", "Denver, CO", "CO"), ("ORD", "Chicago, IL", "IL"), ("DFW", "Dallas/Fort Worth, TX", "TX"),
    ("ATL", "Atlanta, GA", "GA"), ("JFK", "New York, NY", "NY"), ("BOS", "Boston, MA", "MA"),
    ("MIA", "Miami, FL", "FL"), ("PHX", "Phoenix, AZ", "AZ"), ("IAH", "Houston, TX", "TX"),
]
CARRIERS = ["AA", "AS", "B6", "DL", "UA", "WN", "NK", "F9"]
COLUMNS = [
    "Year", "Quarter", "Month", "DayofMonth", "DayOfWeek", "FlightDate", "Reporting_Airline", "Tail_Number",
    "Flight_Number_Reporting_Airline", "Origin", "OriginCityName", "OriginState", "Dest", "DestCityName",
    "DestState", "CRSDepTime", "DepTime", "DepDelay", "CRSArrTime", "ArrTime", "ArrDelay", "Cancelled",
    "CancellationCode", "Diverted", "AirTime", "Distance", "CarrierDelay", "WeatherDelay", "NASDelay",
    "SecurityDelay", "LateAircraftDelay",
]


def hhmm(minutes: int) -> str:
    minutes %= 1440
    return f"{minutes // 60:02d}{minutes % 60:02d}"


def make_row(rng: random.Random, start: date, days: int) -> dict:
    d = start + timedelta(days=rng.randrange(days))
    origin, dest = rng.sample(AIRPORTS, 2)
    dep = rng.randrange(5 * 60, 23 * 60)
    air = rng.randrange(60, 330)
    row = {
        "Year": d.year, "Quarter": (d.month - 1) // 3 + 1, "Month": d.month, "DayofMonth": d.day,
        "DayOfWeek": d.isoweekday(), "FlightDate": d.isoformat(), "Reporting_Airline": rng.choice(CARRIERS),
        "Tail_Number": f"N{rng.randrange(100, 999)}{rng.choice('ABCDEFG')}{rng.choice('ABCDEFG')}",
        "Flight_Number_Reporting_Airline": rng.randrange(1, 2999),
        "Origin": origin[0], "OriginCityName": origin[1], "OriginState": origin[2],
        "Dest": dest[0], "DestCityName": dest[1], "DestState": dest[2],
        "CRSDepTime": hhmm(dep), "CRSArrTime": hhmm(dep + air + 30),
        "Cancelled": "0.00", "CancellationCode": "", "Diverted": "0.00",
        "Distance": f"{air * 8.0:.2f}",
    }
    delay_cols = ["CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay"]
    for c in ["DepTime", "DepDelay", "ArrTime", "ArrDelay", "AirTime", *delay_cols]:
        row[c] = ""

    roll = rng.random()
    if roll < 0.03:  # cancelled
        row["Cancelled"] = "1.00"
        row["CancellationCode"] = rng.choice("ABCD")
        return row

    dep_delay = int(rng.gauss(-3, 6)) if rng.random() < 0.78 else rng.randrange(15, 240)
    row["DepTime"] = hhmm(dep + dep_delay)
    row["DepDelay"] = f"{dep_delay:.2f}"
    if roll < 0.035:  # diverted, never arrived at scheduled destination
        row["Diverted"] = "1.00"
        return row

    arr_delay = dep_delay + rng.randrange(-12, 13)
    row["ArrTime"] = hhmm(dep + air + 30 + arr_delay)
    row["ArrDelay"] = f"{arr_delay:.2f}"
    row["AirTime"] = f"{air:.2f}"
    if arr_delay >= 15:  # BTS only fills delay causes for flights delayed 15+ minutes
        remaining = arr_delay
        for c in rng.sample(delay_cols, len(delay_cols)):
            part = rng.randrange(0, remaining + 1)
            row[c] = f"{part:.2f}"
            remaining -= part
        row["CarrierDelay"] = f"{float(row['CarrierDelay']) + remaining:.2f}"
    return row


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--rows", type=int, default=2000)
    p.add_argument("--out", default="sample_data/flights_sample.csv")
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()

    rng = random.Random(args.seed)
    rows = [make_row(rng, date(2024, 1, 1), 31) for _ in range(args.rows)]

    # Deliberately dirty rows so the quality rules have something to catch.
    rows += [dict(r) for r in rows[:20]]                       # exact duplicates
    for r in rows[20:25]:
        bad = dict(r); bad["Origin"] = "??"; rows.append(bad)  # invalid airport code
    for r in rows[25:30]:
        bad = dict(r); bad["FlightDate"] = "not-a-date"; rows.append(bad)
    for r in rows[30:35]:
        bad = dict(r); bad["Distance"] = "-1"; rows.append(bad)
    rng.shuffle(rows)

    with open(args.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()
        w.writerows(rows)
    print(f"wrote {len(rows)} rows to {args.out}")


if __name__ == "__main__":
    main()
