import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import pandas as pd
import awswrangler as wr
from awswrangler.exceptions import NoFilesFound

def get_csv_data_from_ingestion_bucket(
    path: str, session: boto3.session.Session
) -> dict:
    """Downloads csv data from S3 ingestion bucket and returns a pandas dataframe

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
        df = wr.s3.read_csv(path=path, boto3_session=session)
        return {"status": "success", "data": df}
    except ClientError as ce:
        return {"status": "failure", "message": ce.response}
    except NoFilesFound as nff:
        return {"status": "failure", "message": nff}
    
def censor_sensitive_data(data, pii_fields):

    """Reads a data frame and censors the given fields

    Args:
        data: dataframe containing sensitive information
        pii_fields: list containing personally identifiable information fields

    Returns:
        A dictionary containing the following:
            status: shows whether the function ran successfully
            data: a pandas dataframe containing censored data (if successful)
            message: a relevant error message (if unsuccessful)
    """

    df = data
    
    try:
        for field in pii_fields:
            df[field] = "***"
        return {"status": "success", "data": df}
    except ClientError as ce:
        return {"status": "failure", "message": ce.response}
