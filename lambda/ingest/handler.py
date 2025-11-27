import json
import boto3
import os
from datetime import datetime
import urllib.request
import zipfile
import io

s3 = boto3.client("s3", region_name="us-east-1")
glue = boto3.client("glue", region_name="us-east-1")

BUCKET_NAME = os.environ["BUCKET_NAME"]
GLUE_JOB_NAME = os.environ["GLUE_JOB_NAME"]

EXPECTED_FILES = [
    "olist_customers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
]


def write_metadata(bucket, execution_id, files_count):
    s3.put_object(
        Bucket=bucket,
        Key=f"raw/_metadata/{execution_id}.json",
        Body=json.dumps(
            {
                "execution_id": execution_id,
                "files_count": files_count,
                "timestamp": datetime.now().isoformat(),
            }
        ),
    )


def start_glue_job(glue_job_name, execution_id, bucket):
    response = glue.start_job_run(
        JobName=glue_job_name,
        Arguments={"--execution_id": execution_id, "--bucket_name": bucket},
    )
    return response["JobRunId"]


def download_dataset():
    url = "https://www.kaggle.com/api/v1/datasets/download/olistbr/brazilian-ecommerce"
    try:
        print("Baixando dataset do Kaggle...")
        with urllib.request.urlopen(url) as response:
            zip_data = response.read()
        print("Download concluído.")
        return zip_data
    except Exception as e:
        print(f"Erro ao baixar dataset: {str(e)}")
        raise


def process_and_upload_dataset(zip_data, bucket, expected_files):
    try:
        print("Extraindo arquivos...")
        with zipfile.ZipFile(io.BytesIO(zip_data)) as zip_ref:
            for file_name in zip_ref.namelist():
                if file_name in expected_files:
                    with zip_ref.open(file_name) as file:
                        s3.put_object(
                            Bucket=bucket, Key=f"raw/{file_name}", Body=file.read()
                        )
                        print(f"Arquivo {file_name} enviado para S3.")
        print("Upload dos arquivos concluído.")
    except Exception as e:
        print(f"Erro ao processar ou enviar dataset: {str(e)}")
        raise


def lambda_handler(event, context):
    """
    1. Baixa o dataset do Kaggle e envia para S3
    2. Dispara o Glue Job ETL
    """
    try:
        print(f"Iniciando pipeline Olist - Bucket: {BUCKET_NAME}")
        zip_data = download_dataset()
        process_and_upload_dataset(zip_data, BUCKET_NAME, EXPECTED_FILES)

        execution_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        write_metadata(BUCKET_NAME, execution_id, len(EXPECTED_FILES))


        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Pipeline started successfully",
                    "execution_id": execution_id,
                    "files_processed": len(EXPECTED_FILES),
                }
            ),
        }

        print(f"Iniciando Glue Job: {GLUE_JOB_NAME}")
        job_run_id = start_glue_job(GLUE_JOB_NAME, execution_id, BUCKET_NAME)
        print(f"Glue Job iniciado: {job_run_id}")

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Pipeline started successfully",
                    "execution_id": execution_id,
                    "glue_job_run_id": job_run_id,
                    "files_processed": len(EXPECTED_FILES),
                }
            ),
        }
    except Exception as e:
        print(f"Erro: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

