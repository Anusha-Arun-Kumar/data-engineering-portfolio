"""
NYC Taxi — Window Function Queries with DuckDB
-----------------------------------------------
Runs 5 window function queries directly against the clean parquet file.
No database server needed — DuckDB reads parquet natively.

Run:
    python3 queries.py
"""

import duckdb
import numpy as np

PARQUET_PATH = "data/nyc_taxi_clean.parquet"

con = duckdb.connect()

print("\n" + "═" * 60)
print("  NYC TAXI — WINDOW FUNCTION QUERIES")
print("═" * 60)


# ── QUERY 1: RANK drivers by trip count ──────────────────────────
# ROW_NUMBER / RANK — most common window function in interviews
# Business question: which pickup zones are busiest?

print("\n── Q1: Top 10 busiest pickup zones (RANK) ───────────────────")

q1 = f"""
SELECT
    pulocationid                                        AS zone,
    COUNT(*)                                            AS total_trips,
    RANK() OVER (ORDER BY COUNT(*) DESC)                AS rank
FROM read_parquet('{PARQUET_PATH}')
GROUP BY pulocationid
QUALIFY rank <= 10
ORDER BY rank
"""

print(con.execute(q1).df().to_string(index=False))


# ── QUERY 2: Running total revenue by day ────────────────────────
# SUM OVER — classic running total pattern
# Business question: how does cumulative revenue grow through January?

print("\n── Q2: Running total revenue by day (SUM OVER) ─────────────")

q2 = f"""
SELECT
    pickup_date,
    ROUND(SUM(total_amount), 2)                         AS daily_revenue,
    ROUND(SUM(SUM(total_amount)) OVER (
        ORDER BY pickup_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 2)                                               AS running_total
FROM read_parquet('{PARQUET_PATH}')
GROUP BY pickup_date
ORDER BY pickup_date
LIMIT 10
"""

print(con.execute(q2).df().to_string(index=False))


# ── QUERY 3: % of daily trips each hour represents ───────────────
# SUM OVER PARTITION — partitioned window, very common interview Q
# Business question: what share of each day's trips happen per hour?

print("\n── Q3: Hourly share of daily trips (SUM OVER PARTITION) ─────")

q3 = f"""
SELECT
    pickup_date,
    pickup_hour,
    COUNT(*)                                            AS hourly_trips,
    SUM(COUNT(*)) OVER (PARTITION BY pickup_date)       AS daily_trips,
    ROUND(
        COUNT(*) * 100.0 /
        SUM(COUNT(*)) OVER (PARTITION BY pickup_date), 2
    )                                                   AS pct_of_day
FROM read_parquet('{PARQUET_PATH}')
GROUP BY pickup_date, pickup_hour
ORDER BY pickup_date, pickup_hour
LIMIT 12
"""

print(con.execute(q3).df().to_string(index=False))


# ── QUERY 4: LAG to find previous trip duration ──────────────────
# LAG / LEAD — interview favourite for time-series questions
# Business question: how does each trip's duration compare to the previous one?

print("\n── Q4: Trip duration vs previous trip (LAG) ─────────────────")

q4 = f"""
SELECT
    tpep_pickup_datetime                                AS pickup_time,
    pulocationid                                        AS zone,
    ROUND(trip_duration_mins, 1)                        AS duration_mins,
    ROUND(LAG(trip_duration_mins) OVER (
        PARTITION BY pulocationid
        ORDER BY tpep_pickup_datetime
    ), 1)                                               AS prev_duration_mins,
    ROUND(trip_duration_mins - LAG(trip_duration_mins) OVER (
        PARTITION BY pulocationid
        ORDER BY tpep_pickup_datetime
    ), 1)                                               AS diff_mins
FROM read_parquet('{PARQUET_PATH}')
WHERE pulocationid = 161          -- zone 161 = Midtown, busiest zone
ORDER BY tpep_pickup_datetime
LIMIT 10
"""

print(con.execute(q4).df().to_string(index=False))


# ── QUERY 5: Top 3 pickup zones per borough-equivalent ───────────
# ROW_NUMBER OVER PARTITION — the classic "top N per group" pattern
# This is one of the most common SQL interview questions ever asked

print("\n── Q5: Top 3 zones by trips per hour-bucket (ROW_NUMBER) ────")

q5 = f"""
WITH hourly_zone_trips AS (
    SELECT
        pickup_hour,
        pulocationid                                    AS zone,
        COUNT(*)                                        AS trips
    FROM read_parquet('{PARQUET_PATH}')
    GROUP BY pickup_hour, pulocationid
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY pickup_hour
            ORDER BY trips DESC
        )                                               AS rn
    FROM hourly_zone_trips
)
SELECT
    pickup_hour,
    zone,
    trips,
    rn                                                  AS rank_within_hour
FROM ranked
WHERE rn <= 3
ORDER BY pickup_hour, rn
LIMIT 15
"""

print(con.execute(q5).df().to_string(index=False))


print("\n" + "═" * 60)
print("  All 5 queries complete.")
print("═" * 60 + "\n")

con.close()
