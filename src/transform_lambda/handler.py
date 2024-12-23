import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import awswrangler as wr
from awswrangler.exceptions import NoFilesFound
import os
import math
import pandas as pd
from src.transform_lambda.utils import (
    get_data_from_bucket,
    censor_sensitive_data,
    write_sensitive_data
)
import logging
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    session = boto3.session.Session(region_name="eu-west-2")
    client = session.client("s3")

    bucket_path = "s3://test-bucket-1-ac-gdpr/staff.json"
    pii_fields = ["first_name","email_address"]

    response1 = get_data_from_bucket(bucket_path, session)

    response2 = censor_sensitive_data(response1, pii_fields)
    print(response2)

    processed_bucket = "gdpr-processed-zone/censored_staff.csv"

    response3 = write_sensitive_data(response2)
    print(response3)

lambda_handler("unused", "unused")