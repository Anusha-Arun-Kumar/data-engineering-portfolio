# Data Engineering Portfolio

Personal projects built while transitioning into Data / AI Engineering.

## Projects

### NYC Taxi Pipeline
End-to-end ingestion and cleaning pipeline on the NYC TLC Yellow Taxi dataset.

- **Data:** 2.9 million rows, January 2024 (official NYC TLC source)
- **Stack:** Python, pandas, pyarrow, DuckDB
- **What it does:**
  - Fetches raw parquet data from the NYC TLC public dataset
  - Validates and removes ~250k bad rows across 4 quality rules
  - Adds derived columns (trip duration, speed, fare per mile)
  - Outputs a clean parquet file ready for SQL analysis