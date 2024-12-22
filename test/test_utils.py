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
        bucket = "blackwater-ingestion-zone"
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        timestamp = "2024-05-20 12:10:03.998128"
        filename = "test/data/dummy_csv.csv"
        key = f"ingested_data/{timestamp}/staff.csv"
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        key2 = "last_ran_at.csv"
        filename2 = f"test/data/last_ran_at.csv"
        s3_client.upload_file(Filename=filename2, Bucket=bucket, Key=key2)
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        result = get_data_from_ingestion_bucket(
            key=timestamp, filename=key.split("/")[-1], session=session
        )
        assert isinstance(result["data"], pd.DataFrame)

    def test_data_in_result_in_expected_format_and_type(self, s3_client):
        bucket = "blackwater-ingestion-zone"
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        timestamp = "2024-05-20 12:10:03.998128"
        filename = "test/data/TestGetFileContents.csv"
        key = f"ingested_data/{timestamp}/staff.csv"
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        key2 = "last_ran_at.csv"
        filename2 = f"test/data/last_ran_at.csv"
        s3_client.upload_file(Filename=filename2, Bucket=bucket, Key=key2)
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        result = get_data_from_ingestion_bucket(
            key=timestamp, filename=key.split("/")[-1], session=session
        )
        assert len(result["data"].columns) == 7
        expected = [
            "counterparty_id",
            "counterparty_legal_name",
            "legal_address_id",
            "commercial_contact",
            "delivery_contact",
            "created_at",
            "last_updated",
        ]
        columns = result["data"].columns.values
        for col in columns:
            assert col in expected
        assert len(result["data"]) == 5

    def test_missing_missing_bucket_raises_client_error(self, s3_client):
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        input_key = "ingested_data/2024-05-20 12:10:03.998128/staff.csv"
        result = get_data_from_ingestion_bucket(input_key, session)
        assert result["status"] == "failure"
        assert result["message"]["Error"]["Code"] == "NoSuchBucket"

    def test_missing_missing_bucket_raises_client_error(self, s3_client):
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        bucket = "blackwater-ingestion-zone"
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        input_key = "ingested_data/2024-05-20 12:10:03.998128/staff.csv"
        key = input_key.split("/")[1]
        filename = input_key.split("/")[-1]
        result = get_data_from_ingestion_bucket(
            key=key, filename=filename, session=session
        )
        assert result["status"] == "failure"
        assert (
            str(result["message"]) == f"No files Found on: s3://{bucket}/{input_key}."
        )