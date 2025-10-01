# FX Pipeline (GCP) — Daily ECB Rates
Production-like demo: ingest daily ECB FX rates → GCS (raw) → PySpark transform → GCS (processed, Parquet) → BigQuery → Looker Studio