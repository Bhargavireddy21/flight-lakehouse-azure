-- Example questions the gold layer answers. Run in the Databricks SQL editor.
-- Replace `flightlake` with `workspace` on Databricks Free Edition.

-- 1. On-time performance by carrier
SELECT c.carrier_name,
       sum(a.flights)                                                   AS flights,
       round(100 * sum(a.on_time_flights) / sum(a.arrived_flights), 1)  AS on_time_pct,
       round(100 * sum(a.cancelled_flights) / sum(a.flights), 2)        AS cancel_pct
FROM flightlake.gold.agg_daily_route a
JOIN flightlake.gold.dim_carrier c USING (carrier_code)
GROUP BY c.carrier_name
ORDER BY on_time_pct DESC;

-- 2. Ten worst routes by average arrival delay (minimum 50 flights)
SELECT concat(origin_airport_code, '-', dest_airport_code) AS route,
       sum(flights)                                         AS flights,
       round(sum(avg_arr_delay_min * arrived_flights) / sum(arrived_flights), 1) AS avg_arr_delay_min
FROM flightlake.gold.agg_daily_route
GROUP BY 1
HAVING sum(flights) >= 50
ORDER BY avg_arr_delay_min DESC
LIMIT 10;

-- 3. What causes the delay minutes, per carrier
SELECT carrier_code,
       sum(total_carrier_delay_min)        AS carrier,
       sum(total_weather_delay_min)        AS weather,
       sum(total_nas_delay_min)            AS air_system,
       sum(total_security_delay_min)       AS security,
       sum(total_late_aircraft_delay_min)  AS late_aircraft
FROM flightlake.gold.agg_daily_route
GROUP BY carrier_code
ORDER BY carrier_code;

-- 4. Delay rate by scheduled departure hour and weekend flag
SELECT f.crs_dep_hour,
       d.is_weekend,
       count(*)                                                AS flights,
       round(100 * avg(cast(f.is_arr_delayed_15 AS int)), 1)   AS delayed_pct
FROM flightlake.gold.fact_flights f
JOIN flightlake.gold.dim_date d USING (date_key)
GROUP BY f.crs_dep_hour, d.is_weekend
ORDER BY f.crs_dep_hour, d.is_weekend;

-- 5. Day-over-day change in flights per carrier (window function)
WITH daily AS (
  SELECT date_key, carrier_code, sum(flights) AS flights
  FROM flightlake.gold.agg_daily_route
  GROUP BY date_key, carrier_code
)
SELECT date_key, carrier_code, flights,
       flights - lag(flights) OVER (PARTITION BY carrier_code ORDER BY date_key) AS change_vs_previous_day
FROM daily
ORDER BY carrier_code, date_key;
