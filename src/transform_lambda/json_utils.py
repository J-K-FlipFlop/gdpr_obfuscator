import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import pandas as pd
import awswrangler as wr
from awswrangler.exceptions import NoFilesFound

def get_json_data_from_ingestion_bucket(
    path: str, session: boto3.session.Session
) -> dict:
    """Downloads JSON data from S3 ingestion bucket and returns a pandas dataframe

    Args:
        key: string representing S3 object to be downloaded
        session: Boto3 session

    Returns:
        A dictionary containing the following:
            status: shows whether the function ran successfully
            data: a pandas dataframe containing downloaded data (if successful)
            message: a relevant error message (if unsuccessful)
    """

    try:
        df = wr.s3.read_parquet(path=path, boto3_session=session)
        return {"status": "success", "data": df}
    except ClientError as ce:
        return {"status": "failure", "message": ce.response}
    except NoFilesFound as nff:
        return {"status": "failure", "message": nff}