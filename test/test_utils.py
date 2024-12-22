import pytest
import boto3
import os
import pandas as pd
import awswrangler as wr
from moto import mock_aws
from src.transform_lambda.utils import (
    get_data_from_ingestion_bucket
)
from botocore.exceptions import ClientError


@pytest.fixture(scope="function")
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3_client(aws_creds):
    with mock_aws():
        yield boto3.client("s3")

class TestGetFileContents:
    def test_function_returns_pandas_dataframe(self, s3_client):
        bucket = "ingested_data"
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        filename = "test/data/dummy_csv.csv"
        key = "dummy.csv"
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        path = "s3://ingested_data/dummy.csv"
        result = get_data_from_ingestion_bucket(
            path, session
        )
        assert isinstance(result["data"], pd.DataFrame)

    # def test_missing_bucket_raises_client_error(self, s3_client):
    #     session = boto3.session.Session(
    #         aws_access_key_id="test", aws_secret_access_key="test"
    #     )
    #     input_key = "ingested_data/2024-05-20 12:10:03.998128/staff.csv"
    #     result = get_data_from_ingestion_bucket(input_key, session)
    #     assert result["status"] == "failure"
    #     assert result["message"]["Error"]["Code"] == "NoSuchBucket"

    def test_missing_missing_bucket_raises_client_error(self, s3_client):
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        bucket = "ingested_data"
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        path = "s3://wrong_bucket/dummy.csv"
        result = get_data_from_ingestion_bucket(
            path, session=session
        )
        assert result["status"] == "failure"
        assert (
            str(result["message"]) == f"No files Found on: s3://{bucket}/dummy_csv."
        )