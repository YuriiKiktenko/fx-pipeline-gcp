import io, zipfile
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.gcs import GCSHook

def unzip_raw_zip_to_csv(raw_bucket, object_shortname):
    print(f"[unzip] bucket={raw_bucket} shortname={object_shortname}")
    gcs = GCSHook()
    
    zip_obj = f"{object_shortname}.zip"
    csv_obj = f"{object_shortname}.csv"

    if not gcs.exists(bucket_name=raw_bucket, object_name=zip_obj):
        raise AirflowException(f"ZIP not found: gs://{raw_bucket}/{zip_obj}")

    zip_bytes = gcs.download(bucket_name=raw_bucket, object_name=zip_obj)
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        if "eurofxref-hist.csv" not in zf.namelist():
            raise AirflowException(f"'eurofxref-hist.csv' not found in ZIP. Found: {zf.namelist()}")
        csv_bytes = zf.read("eurofxref-hist.csv")

    gcs.upload(bucket_name=raw_bucket, object_name=csv_obj, data=csv_bytes, mime_type="text/csv")
    return f"gs://{raw_bucket}/{csv_obj}"
