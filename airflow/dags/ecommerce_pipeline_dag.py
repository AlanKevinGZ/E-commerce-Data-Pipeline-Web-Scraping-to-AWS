import os
import sys
import subprocess
from datetime import datetime, timedelta
import requests

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/usr/local/airflow/include")

default_args = {
    "owner": "alan",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ecommerce_scraping_pipeline",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["ecommerce", "scraping", "etl"],
) as dag:

    # --- Scraping (en paralelo) ---
    run_scraper_cars = BashOperator(
        task_id="run_scraper_cars",
        bash_command="cd /usr/local/airflow/include && scrapy crawl webscraper_cars",
    )

    run_scraper_ecommerce = BashOperator(
        task_id="run_scraper_ecommerce",
        bash_command="cd /usr/local/airflow/include && scrapy crawl webscraper_ecommerce",
    )

    # --- Verificación de archivos RAW ---
    def _verify_raw_files(site_name, **context):
        import boto3
        from datetime import timezone

        s3 = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION"),
        )
        bucket = os.getenv("AWS_S3_BUCKET")
        date_path = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        prefix = f"raw/text/{site_name}/{date_path}/"

        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if response.get("KeyCount", 0) == 0:
            raise FileNotFoundError(
                f"No se encontraron archivos RAW en s3://{bucket}/{prefix}"
            )
        print(f"Verificación OK ({site_name}): {response['KeyCount']} archivo(s) en RAW.")

    verify_raw_files_cars = PythonOperator(
        task_id="verify_raw_files_cars",
        python_callable=_verify_raw_files,
        op_kwargs={"site_name": "webscraper_cars"},
    )

    verify_raw_files_ecommerce = PythonOperator(
        task_id="verify_raw_files_ecommerce",
        python_callable=_verify_raw_files,
        op_kwargs={"site_name": "webscraper_ecommerce"},
    )

    # --- ETL (en paralelo), cada uno publica su resultado en XCom ---
    def _run_etl(script_path, **context):
        result = subprocess.run(
            ["python", script_path],
            cwd="/usr/local/airflow/include",
            capture_output=True,
            text=True,
        )
        print(result.stdout)
        if result.returncode != 0:
            print(result.stderr)
            context["ti"].xcom_push(key="status", value="failed")
            raise RuntimeError(f"{script_path} falló con código {result.returncode}")
        context["ti"].xcom_push(key="status", value="success")

    run_etl_cars = PythonOperator(
        task_id="run_etl_cars",
        python_callable=_run_etl,
        op_kwargs={"script_path": "etl/raw_to_curated.py"},
    )

    run_etl_ecommerce = PythonOperator(
        task_id="run_etl_ecommerce",
        python_callable=_run_etl,
        op_kwargs={"script_path": "etl/raw_to_curated_ecommerce.py"},
    )

    # --- Notificaciones ---
    def _send_slack(message):
        webhook_url = os.getenv("SLACK_WEBHOOK_URL")
        if webhook_url:
            response = requests.post(webhook_url, json={"text": message})
            response.raise_for_status()
        else:
            print("SLACK_WEBHOOK_URL no configurada, solo imprimiendo:")
            print(message)

    def _notify_success(**context):
        ti = context["ti"]
        dag_run = context["dag_run"]

        cars_state = ti.xcom_pull(task_ids="run_etl_cars", key="status") or "unknown"
        ecommerce_state = ti.xcom_pull(task_ids="run_etl_ecommerce", key="status") or "unknown"

        message = (
            f"✅ *Ecommerce Scraping Pipeline*\n"
            f"Fecha: {dag_run.logical_date}\n"
            f"Cars ETL: {cars_state}\n"
            f"Ecommerce ETL: {ecommerce_state}\n"
            f"Run ID: {dag_run.run_id}"
        )
        _send_slack(message)

    def _notify_failure(**context):
        ti = context["ti"]
        dag_run = context["dag_run"]

        cars_state = ti.xcom_pull(task_ids="run_etl_cars", key="status") or "no ejecutado"
        ecommerce_state = ti.xcom_pull(task_ids="run_etl_ecommerce", key="status") or "no ejecutado"

        message = (
            f"❌ *Ecommerce Scraping Pipeline*\n"
            f"Fecha: {dag_run.logical_date}\n"
            f"Cars ETL: {cars_state}\n"
            f"Ecommerce ETL: {ecommerce_state}\n"
            f"Run ID: {dag_run.run_id}"
        )
        _send_slack(message)

    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=_notify_success,
        trigger_rule="all_success",
    )

    notify_failure = PythonOperator(
        task_id="notify_failure",
        python_callable=_notify_failure,
        trigger_rule="one_failed",
    )

    # --- Dependencias ---
    run_scraper_cars >> verify_raw_files_cars >> run_etl_cars
    run_scraper_ecommerce >> verify_raw_files_ecommerce >> run_etl_ecommerce

    [run_etl_cars, run_etl_ecommerce] >> notify_success
    [run_etl_cars, run_etl_ecommerce] >> notify_failure