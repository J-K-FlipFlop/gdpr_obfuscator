import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import awswrangler as wr
from awswrangler.exceptions import NoFilesFound
import math
import pandas as pd
from src.transform_lambda.utils import (
    get_data_from_ingestion_bucket,
    censor_sensitive_data
)
import logging
import os
from moto import mock_aws

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    session = boto3.session.Session(region_name="eu-west-2")
    client = session.client("s3")

    bucket_path = "s3://test-bucket-1-ac-gdpr"
    pii_fields = ["first_name","email_address"]

    response = censor_sensitive_data(client, session, bucket_path, pii_fields)
    print(response)

def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


def s3_client(aws_creds):
    with mock_aws():
        yield boto3.client("s3")

lambda_handler("unused", "unused")