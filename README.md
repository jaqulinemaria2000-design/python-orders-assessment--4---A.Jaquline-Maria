Data Pipeline

This repository contains the solution for **Assignment Week 4**. The chosen option is **Option B: SQLite + Airflow**, which avoids Docker overhead and allows the pipeline to run locally on Windows while still demonstrating all ETL, Star Schema, and Orchestration concepts.

## Project Structure

```text
assignment/
├── dags/
│   └── dag.py        # Airflow DAG to automate the pipeline
├── data/                          # Automatically created on run
│   ├── source.db                  # Mock source database with SCD2 customer profiles & FX Rates
│   ├── oltp.db                    # Target OLTP normalized database
│   ├── dwh.db                     # Target Data Warehouse Star Schema & Marts
│   ├── airflow_job_logs.db        # Logs pipeline execution statuses
│   ├── raw/                       # Staging area for ingested files
│   ├── cleaned/                   # Staging area for cleaned files
│   └── quarantine/                # Quarantined data anomalies (FX Spikes, Overlaps, etc)
├── scripts/
│   ├── setup_source_db.py         # 1. Generates source.db mock data
│   ├── ingestion.py               # 2. Ingests CSV, JSON, Excel and SQLite into raw staging
│   ├── cleaning.py                # 3. Standardizes data, flags anomalies, resolves SCD2 overlaps
│   ├── oltp_loader.py             # 4. Loads cleaned data into normalized OLTP schema
│   └── dwh_loader.py              # 5. Transforms OLTP to Star Schema with simulated partitioning
└── requirements.txt               # Required Python packages
```

## Setup & Running Locally

1. **Install Dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Run Pipeline Manually (Without Airflow):**
   You can execute the entire pipeline by running the scripts in sequence:
   ```bash
   python scripts/setup_source_db.py
   python scripts/ingestion.py
   python scripts/cleaning.py
   python scripts/oltp_loader.py
   python scripts/dwh_loader.py
   ```
   *Note: Ensure to run the scripts from the `assignment` directory.*

3. **Run using Airflow:**
   - Copy the `dags/assignment_4_dag.py` to your Airflow `dags` folder.
   - The DAG `assignment_4_pipeline` will appear in the Airflow UI, which you can trigger manually to execute the end-to-end pipeline.
   - The DAG executes the scripts sequentially and creates `airflow_job_logs.db` in the `data/` folder to log job success/failure statuses for auditing.

## Data Lineage & Anomalies Handled

1. **Ingestion Layer:** Consolidates data from 4 formats (`.csv`, `.json`, `.xlsx`, `.db`).
2. **Cleaning Layer:**
   - Detects Outlier FX Spikes (using rolling median) and logs to `quarantine/fx_rate_anomalies.csv`. Missing dates are forward-filled.
   - Resolves SCD2 Overlapping Timeframes and logs offending records to `quarantine/customer_profile_overlaps.csv`.
3. **OLTP Layer:** Safely deduplicates and filters records referencing invalid Foreign Keys before inserting into a properly designed normalized schema.
4. **Data Warehouse Layer:**
   - **Simulated Partitioning:** `fact_orders` is split into physical monthly tables (e.g. `fact_orders_2024_01`) and wrapped in a unified view `curated_orders_enriched`.
   - **Metrics:** `mart_daily_metrics` aggregates revenue, exposure, and high-risk flags per day natively using Pandas grouping.

## Verification

After running the pipeline, check the `data/` directory. You will find:
- **`data/quarantine/`**: Contains CSVs representing rejected anomalies.
- **`data/oltp.db`**: Contains 5 normalized tables (`customers`, `orders`, `payments`, `customer_profile`, `fx_rates_daily`).
- **`data/dwh.db`**: Contains Star Schema (`dim_customers`, `fact_orders_YYYY_MM` partitions, `curated_payments_aggregated`, and `mart_daily_metrics`).

