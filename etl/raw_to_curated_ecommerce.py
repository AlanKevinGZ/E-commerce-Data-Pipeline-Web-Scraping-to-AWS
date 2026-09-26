import os
import shutil
from datetime import datetime, timezone

import boto3
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

load_dotenv()

BUCKET = os.getenv("AWS_S3_BUCKET")
SITE_NAME = "webscraper_ecommerce"
DATE_PATH = datetime.now(timezone.utc).strftime("%Y/%m/%d")
MAX_REJECTION_RATE = 0.15

LOCAL_RAW_DIR = "etl/_tmp/raw_ecommerce"
LOCAL_CURATED_DIR = "etl/_tmp/curated_ecommerce"


def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION"),
    )


def download_raw_json(s3):
    prefix = f"raw/text/{SITE_NAME}/{DATE_PATH}/"
    os.makedirs(LOCAL_RAW_DIR, exist_ok=True)

    paginator = s3.get_paginator("list_objects_v2")
    downloaded = 0
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json"):
                local_path = os.path.join(LOCAL_RAW_DIR, os.path.basename(key))
                s3.download_file(BUCKET, key, local_path)
                downloaded += 1

    if downloaded == 0:
        raise FileNotFoundError(f"No se encontraron JSON en s3://{BUCKET}/{prefix}")
    print(f"Descargados {downloaded} archivo(s) RAW a {LOCAL_RAW_DIR}")


def transform(spark):
    df = spark.read.option("multiline", "true").json(f"{LOCAL_RAW_DIR}/*.json")

    df = df.withColumn("price_usd", F.col("price_usd").cast("double"))
    df = df.withColumn("review_count", F.col("review_count").cast("integer"))

    df = df.withColumn("ingestion_date", F.current_date())

    # dedup por sku, se queda con el registro más reciente si hay repetidos
    window_spec = Window.partitionBy("sku").orderBy(F.col("scraped_at").desc())
    df = df.withColumn("row_num", F.row_number().over(window_spec))
    df_dedup = df.filter(F.col("row_num") == 1).drop("row_num")

    return df_dedup


def validate_quality(df):
    df_validado = df.withColumn(
        "dq_errors",
        F.array_compact(
            F.array(
                F.when(F.col("sku").isNull() | (F.col("sku") == ''), F.lit("sku_missing")).otherwise(None),
                F.when(F.col("price_usd").isNull() | (F.col("price_usd") <= 0), F.lit("invalid_price")).otherwise(None),
                F.when(F.col("product_url").isNull() | (F.col("product_url") == ''), F.lit("missing_url")).otherwise(None),
                F.when(F.col("title").isNull() | (F.col("title") == ''), F.lit("title_missing")).otherwise(None),
                F.when(F.col("review_count").isNull() | (F.col("review_count") < 0), F.lit("invalid_review_count")).otherwise(None),
            )
        )
    )

    df_validado = df_validado.withColumn(
        "dq_passed",
        F.size(F.col("dq_errors")) == 0
    )

    df_valid = df_validado.filter(F.col("dq_passed") == True).drop("dq_passed")
    df_rejected = df_validado.filter(F.col("dq_passed") == False).drop("dq_passed")

    return df_valid, df_rejected


def upload_curated(s3, local_dir, prefix):
    uploaded = 0
    for root, _, files in os.walk(local_dir):
        for file in files:
            if file.startswith("_") or file.startswith("."):
                continue
            local_path = os.path.join(root, file)
            key = f"{prefix}{file}"
            s3.upload_file(local_path, BUCKET, key)
            uploaded += 1
    print(f"Subidos {uploaded} archivo(s) a s3://{BUCKET}/{prefix}")


def main():
    s3 = get_s3_client()

    shutil.rmtree(LOCAL_RAW_DIR, ignore_errors=True)
    shutil.rmtree(LOCAL_CURATED_DIR, ignore_errors=True)
    shutil.rmtree(LOCAL_CURATED_DIR + "_rejected", ignore_errors=True)

    download_raw_json(s3)

    spark = SparkSession.builder.appName("ecommerce_ecommerce_raw_to_curated").getOrCreate()

    try:
        df = transform(spark)
        df_valid, df_rejected = validate_quality(df)

        valid_count = df_valid.count()
        rejected_count = df_rejected.count()
        print(f"Registros válidos: {valid_count} | Rechazados: {rejected_count}")

        df_valid.show(5, truncate=True)

        total_count = valid_count + rejected_count
        if total_count == 0:
            raise ValueError("No hay registros para procesar (total_count = 0)")

        rejection_rate = rejected_count / total_count
        if rejection_rate > MAX_REJECTION_RATE:
            raise ValueError(
                f"Tasa de rechazo alta: {rejection_rate:.1%} "
                f"(máximo aceptable: {MAX_REJECTION_RATE:.1%})"
            )

        df_valid.write.mode("overwrite").parquet(LOCAL_CURATED_DIR)
        upload_curated(s3, LOCAL_CURATED_DIR, f"curated/text/{SITE_NAME}/{DATE_PATH}/")

        if rejected_count > 0:
            df_rejected.write.mode("overwrite").parquet(LOCAL_CURATED_DIR + "_rejected")
            upload_curated(
                s3,
                LOCAL_CURATED_DIR + "_rejected",
                f"curated/rejected/{SITE_NAME}/{DATE_PATH}/"
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()