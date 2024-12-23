import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import awswrangler as wr
from awswrangler.exceptions import NoFilesFound
import os
import math
import pandas as pd
from src.transform_lambda.csv_utils import (
    get_csv_data_from_ingestion_bucket,
    censor_sensitive_data
)
from src.transform_lambda.parquet_utils import (
    get_parquet_data_from_ingestion_bucket
)
from src.transform_lambda.json_utils import (
    get_json_data_from_ingestion_bucket
)
import logging
import os
from moto import mock_aws

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    session = boto3.session.Session(region_name="eu-west-2")
    client = session.client("s3")

    bucket_path = "s3://test-bucket-1-ac-gdpr/staff.parquet"
    pii_fields = ["first_name","email_address"]

    filename, file_extension = os.path.splitext(bucket_path)

    print(file_extension)

    if file_extension == ".csv":
        response1 = get_csv_data_from_ingestion_bucket(bucket_path, session)
    elif file_extension == ".parquet":
        response1 = get_parquet_data_from_ingestion_bucket(bucket_path, session)
    elif file_extension == ".json":
        response1 = get_json_data_from_ingestion_bucket(bucket_path, session)
    else:
        return {
            "status": "failure",
            "message": f"Unsuported data type. Can only process csv, json, and parquet file types",
        }
    
    print(response1)

    data = response1["data"]

    response2 = censor_sensitive_data(data, pii_fields)
    print(response2)

def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


def s3_client(aws_creds):
    with mock_aws():
        yield boto3.client("s3")

lambda_handler("unused", "unused")