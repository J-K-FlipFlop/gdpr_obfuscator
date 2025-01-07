import pytest
import boto3
import os
import pandas as pd
import awswrangler as wr
from moto import mock_aws
from src.transform_lambda.utils import (
    censor_sensitive_data,
    get_data_from_bucket,
    write_sensitive_data
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
        filename = "../test/data/dummy_csv.csv"
        key = "dummy.csv"
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        path = "s3://ingested_data/dummy.csv"
        result = get_data_from_bucket(
            path, session
        )
        assert isinstance(result["data"], pd.DataFrame)

    def test_missing_bucket_raises_correct_client_error(self, s3_client):
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        bucket = "ingested_data"
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        path = "s3://wrong_bucket/dummy.csv"
        result = get_data_from_bucket(
            path, session=session
        )
        assert result["status"] == "failure"
        assert (
            str(result["message"]["Error"]["Code"]) == "NoSuchBucket"
        )
    
    def test_function_identifies_csv_correctly(self, s3_client):
        bucket = "ingested_data"
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        filename = "../test/data/dummy_csv.csv"
        key = "dummy.csv"
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        path = "s3://ingested_data/dummy.csv"
        result = get_data_from_bucket(
            path, session
        )
        assert result["format"] == ".csv"

    def test_function_identifies_parquet_correctly(self, s3_client):
        bucket = "ingested_data"
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        filename = "../test/data/dummy_parquet.parquet"
        key = "dummy.parquet"
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        path = "s3://ingested_data/dummy.parquet"
        result = get_data_from_bucket(
            path, session
        )
        assert result["format"] == ".parquet"

    def test_function_identifies_json_correctly(self, s3_client):
        bucket = "ingested_data"
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        filename = "../test/data/dummy_json.json"
        key = "dummy.json"
        s3_client.upload_file(Filename=filename, Bucket=bucket, Key=key)
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        path = "s3://ingested_data/dummy.json"
        result = get_data_from_bucket(
            path, session
        )
        assert result["format"] == ".json"

class TestWriteFileContents:
    def test_function_identifies_csv_correctly(self):

        d = {
        'F Name': ['John', 'Steve', 'Stefani'],
        'L Name': ['Doe', 'Buscemi', 'Germanotta'],
        'Username': ['lanfs', 'SBdog420', 'ThaQueenLessthanthree'],
        'email': ['JohnDoe@protonmail.com', 'SteveyBoy@askjeeves.co.uk', 'LadyGG@aol.com'],
        'favorate food': ['-', '-', '-'],
        }

        df = pd.DataFrame(d)
        response_dict = {'status': 'success', 'data': df, 'format': ".csv"}
        result = write_sensitive_data(
            response_dict
        )
        assert result["message"] == "csv written to byte stream"

    def test_function_identifies_parquet_correctly(self):

        d = {
        'F Name': ['John', 'Steve', 'Stefani'],
        'L Name': ['Doe', 'Buscemi', 'Germanotta'],
        'Username': ['lanfs', 'SBdog420', 'ThaQueenLessthanthree'],
        'email': ['JohnDoe@protonmail.com', 'SteveyBoy@askjeeves.co.uk', 'LadyGG@aol.com'],
        'favorate food': ['-', '-', '-'],
        }

        df = pd.DataFrame(d)
        response_dict = {'status': 'success', 'data': df, 'format': ".parquet"}
        result = write_sensitive_data(
            response_dict
        )
        assert result["message"] == "parquet written to byte stream"

    def test_function_identifies_json_correctly(self):

        d = {
        'F Name': ['John', 'Steve', 'Stefani'],
        'L Name': ['Doe', 'Buscemi', 'Germanotta'],
        'Username': ['lanfs', 'SBdog420', 'ThaQueenLessthanthree'],
        'email': ['JohnDoe@protonmail.com', 'SteveyBoy@askjeeves.co.uk', 'LadyGG@aol.com'],
        'favorate food': ['-', '-', '-'],
        }

        df = pd.DataFrame(d)
        response_dict = {'status': 'success', 'data': df, 'format': ".json"}
        result = write_sensitive_data(
            response_dict
        )
        assert result["message"] == "json written to byte stream"

    def test_function_identifies_unsuported_format_correctly(self):

        d = {
        'F Name': ['John', 'Steve', 'Stefani'],
        'L Name': ['Doe', 'Buscemi', 'Germanotta'],
        'Username': ['lanfs', 'SBdog420', 'ThaQueenLessthanthree'],
        'email': ['JohnDoe@protonmail.com', 'SteveyBoy@askjeeves.co.uk', 'LadyGG@aol.com'],
        'favorate food': ['-', '-', '-'],
        }

        df = pd.DataFrame(d)
        response_dict = {'status': 'success', 'data': df, 'format': ".txt"}
        result = write_sensitive_data(
            response_dict
        )
        assert result["message"] == "Unsuported data type. Can only process csv, json, and parquet file types"

    def test_function_identifies_unexpected_error_from_poor_input_correctly(self):

        response_dict = {'status': 'failed'}
        result = write_sensitive_data(
            response_dict
        )
        assert result["message"] == "unexpected error"