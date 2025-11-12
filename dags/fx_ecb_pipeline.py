from datetime import datetime, time, timedelta, timezone
from airflow import DAG
from airflow.sensors.time_sensor import TimeSensorAsync
from airflow.providers.google.cloud.transfers.http_to_gcs import HttpToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python import PythonOperator
from lib.unzip_to_csv import unzip_raw_zip_to_csv

PROJECT_ID = "de-port"
RAW_BUCKET = "de-port-raw-yk"
SCHEDULE = "0 0 * * *"
START_DS = "{{ data_interval_start | ds }}"

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


with DAG(
    dag_id="fx_ecb_pipeline",
    description="ECB FX",
    default_args=default_args,
    schedule=SCHEDULE,
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
) as dag:

    wait_until_03_utc = TimeSensorAsync(
        task_id="wait_until_03_00_utc",
        target_time=time(3, 0),
    )    

    download_zip = HttpToGCSOperator(
        task_id="http_zip_to_gcs_raw",
        http_conn_id="ecb_http",  # Admin → Connections: Host=https://www.ecb.europa.eu
        endpoint="/stats/eurofxref/eurofxref-hist.zip",
        method="GET",
        bucket_name=RAW_BUCKET,
        object_name=f"ecb_fx/{START_DS}/eurofxref-hist.zip",
        mime_type="application/zip",
    )

    unzip_to_csv = PythonOperator(
        task_id="unzip_to_csv",
        python_callable=unzip_raw_zip_to_csv,
        op_kwargs={
            "raw_bucket": RAW_BUCKET,
            "object_shortname": f"ecb_fx/{START_DS}/eurofxref-hist",
        },
    )

    load_raw = GCSToBigQueryOperator(
        task_id="load_csv_to_bq_raw",
        bucket=RAW_BUCKET,
        source_objects=[f"ecb_fx/{START_DS}/eurofxref-hist.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}.fx.fx_raw",
        source_format="CSV",
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )

    wait_until_03_utc >> download_zip >> unzip_to_csv >> load_raw
