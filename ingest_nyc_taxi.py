"""
NYC Taxi Data — Ingestion & Cleaning Pipeline
----------------------------------------------
Pulls Yellow Taxi trip data from the NYC Open Data API,
cleans and validates it, and saves a clean parquet file
ready for SQL analysis with DuckDB.

Run:
    python ingest_nyc_taxi.py

Output:
    data/nyc_taxi_clean.parquet
"""

import os
import pandas as pd


# ── CONFIG ────────────────────────────────────────────────────────────────────

# NYC Open Data — Yellow Taxi trips (2023, publicly available, no API key needed)
DATA_URL = "https://data.cityofnewyork.us/resource/gkne-dk5s.csv"

OUTPUT_DIR = "data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "nyc_taxi_clean.parquet")


# ── INGEST ────────────────────────────────────────────────────────────────────

DATA_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"

def ingest(url: str) -> pd.DataFrame:
    """Download NYC Yellow Taxi trip data from the TLC public dataset."""
    import requests, io
    print("[ingest] Fetching NYC TLC Yellow Taxi data (Jan 2024)...")
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    df = pd.read_parquet(io.BytesIO(response.content))
    print(f"[ingest] Raw shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
    return df


# ── CLEAN ─────────────────────────────────────────────────────────────────────

def clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and validate the raw taxi dataframe.

    Steps:
        1. Rename columns to snake_case
        2. Parse datetime columns
        3. Drop rows with nulls in critical columns
        4. Remove physically impossible values
        5. Add derived columns useful for analysis
    """
    print("[clean] Starting cleaning pipeline...")

    # 1. Rename columns — lowercase and strip spaces
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
    )

    # 2. Parse datetimes
    for col in ["tpep_pickup_datetime", "tpep_dropoff_datetime"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # 3. Drop rows with nulls in columns we always need
    critical_cols = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "total_amount",
    ]
    existing_critical = [c for c in critical_cols if c in df.columns]
    before = len(df)
    df = df.dropna(subset=existing_critical)
    print(f"[clean] Dropped {before - len(df):,} rows with nulls in critical columns")

    # 4. Remove physically impossible values
    #    - trips must go somewhere (distance > 0)
    #    - fares must be positive
    #    - passenger count must be 1–6 (NYC taxi max)
    #    - trip duration must be positive and under 5 hours
    if "tpep_pickup_datetime" in df.columns and "tpep_dropoff_datetime" in df.columns:
        df["trip_duration_mins"] = (
            (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"])
            .dt.total_seconds() / 60
        )

    rules = {
        "trip_distance":      (df["trip_distance"] > 0),
        "total_amount":       (df["total_amount"] > 0),
    }
    if "passenger_count" in df.columns:
        rules["passenger_count"] = df["passenger_count"].between(1, 6)
    if "trip_duration_mins" in df.columns:
        rules["trip_duration_mins"] = df["trip_duration_mins"].between(1, 300)

    before = len(df)
    combined_mask = pd.Series(True, index=df.index)
    for rule_name, mask in rules.items():
        failed = (~mask).sum()
        if failed:
            print(f"[clean]   Removing {failed:,} rows failing rule: {rule_name}")
        combined_mask &= mask
    df = df[combined_mask]
    print(f"[clean] Removed {before - len(df):,} rows with impossible values")

    # 5. Add derived columns
    if "tpep_pickup_datetime" in df.columns:
        df["pickup_hour"]    = df["tpep_pickup_datetime"].dt.hour
        df["pickup_day"]     = df["tpep_pickup_datetime"].dt.day_name()
        df["pickup_date"]    = df["tpep_pickup_datetime"].dt.date

    if "trip_distance" in df.columns and "trip_duration_mins" in df.columns:
        # speed in mph — useful for filtering bad GPS data later
        df["avg_speed_mph"] = (
            df["trip_distance"] / (df["trip_duration_mins"] / 60)
        ).round(2)
        # remove physically impossible speeds (> 100mph in a NYC taxi...)
        before = len(df)
        df = df[df["avg_speed_mph"] <= 100]
        print(f"[clean] Removed {before - len(df):,} rows with speed > 100mph")

    if "total_amount" in df.columns and "trip_distance" in df.columns:
        df["fare_per_mile"] = (
            df["total_amount"] / df["trip_distance"]
        ).round(2)

    print(f"[clean] Clean shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
    return df


# ── VALIDATE ──────────────────────────────────────────────────────────────────

def validate(df: pd.DataFrame) -> None:
    """
    Print a quick data quality summary so we know what we're working with.
    In a real pipeline this would write to a data quality log.
    """
    print("\n[validate] ── Data Quality Report ──────────────────────────────")
    print(f"  Total rows:       {len(df):,}")
    print(f"  Total columns:    {df.shape[1]}")

    null_counts = df.isnull().sum()
    null_cols = null_counts[null_counts > 0]
    if null_cols.empty:
        print("  Nulls remaining:  none ✓")
    else:
        print(f"  Nulls remaining:  {len(null_cols)} columns have nulls")
        for col, count in null_cols.items():
            print(f"    {col}: {count:,}")

    if "trip_distance" in df.columns:
        print(f"  Avg trip distance: {df['trip_distance'].mean():.2f} miles")
    if "total_amount" in df.columns:
        print(f"  Avg fare:          ${df['total_amount'].mean():.2f}")
    if "trip_duration_mins" in df.columns:
        print(f"  Avg trip duration: {df['trip_duration_mins'].mean():.1f} mins")
    if "pickup_hour" in df.columns:
        peak_hour = df["pickup_hour"].value_counts().idxmax()
        print(f"  Busiest hour:      {peak_hour}:00")
    print("[validate] ─────────────────────────────────────────────────────\n")


# ── SAVE ──────────────────────────────────────────────────────────────────────

def save(df: pd.DataFrame, path: str) -> None:
    """Save clean dataframe as parquet — efficient, typed, and DuckDB-ready."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(path, index=False)
    size_mb = os.path.getsize(path) / (1024 * 1024)
    print(f"[save] Saved to {path} ({size_mb:.1f} MB)")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    raw_df   = ingest(DATA_URL)
    clean_df = clean(raw_df)
    validate(clean_df)
    save(clean_df, OUTPUT_FILE)
    print("[done] Pipeline complete. Run queries.sql next with DuckDB.")


if __name__ == "__main__":
    main()
