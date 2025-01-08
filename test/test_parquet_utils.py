import pytest
import boto3
import os
import pandas as pd
import awswrangler as wr
from moto import mock_aws
from src.transform_lambda.parquet_utils import write_parquet_data
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


class TestWriteParquet:
    def test_function_writes_to_s3_bucket(self, s3_client):
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        bucket = "blackwater-processed-zone"
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        d = {
            "F Name": ["John", "Steve", "Stefani"],
            "L Name": ["Doe", "Buscemi", "Germanotta"],
            "Username": ["lanfs", "SBdog420", "ThaQueenLessthanthree"],
            "email": [
                "JohnDoe@protonmail.com",
                "SteveyBoy@askjeeves.co.uk",
                "LadyGG@aol.com",
            ],
            "favorate food": ["-", "-", "-"],
        }
        df = pd.DataFrame(d)
        write_parquet_data(df, f"s3://{bucket}/movie.parquet", session)
        result = s3_client.list_objects_v2(Bucket=bucket)
        assert result["Contents"][0]["Key"] == f"movie.parquet"

    def test_function_writes_correct_data_to_s3_bucket(self, s3_client):
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        bucket = "blackwater-processed-zone"
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        d = {"F Name": ["John", "Steve", "Stefani"]}
        df = pd.DataFrame(d)
        write_parquet_data(df, f"s3://{bucket}/movie.parquet", session)
        result = wr.s3.read_parquet(path=f"s3://{bucket}/movie.parquet")
        assert result.to_dict(orient="index") == {
            0: {"F_Name": "John"},
            1: {"F_Name": "Steve"},
            2: {"F_Name": "Stefani"},
        }

    def test_missing_bucket_raises_client_error(self, s3_client):
        session = boto3.session.Session(
            aws_access_key_id="test", aws_secret_access_key="test"
        )
        bucket = "blackwater-processed-zone"
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        d = {"F Name": ["John", "Steve", "Stefani"]}
        df = pd.DataFrame(d)
        result = write_parquet_data(df, f"s3://fake_bucket/movie.parquet", session)
        assert result["status"] == "failure"
        assert result["message"]["Error"]["Code"] == "NoSuchBucket"
