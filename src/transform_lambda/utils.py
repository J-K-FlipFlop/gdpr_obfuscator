import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import pandas as pd
import awswrangler as wr
from awswrangler.exceptions import NoFilesFound

def get_data_from_ingestion_bucket(
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

    path = path
    try:
        df = wr.s3.read_csv(path=path, boto3_session=session)
        # print(df.columns)
        return {"status": "success", "data": df}
    except ClientError as ce:
        return {"status": "failure", "message": ce.response}
    except NoFilesFound as nff:
        return {"status": "failure", "message": nff}
    
def censor_sensitive_data(client, session, bucket_path, pii_fields):

    response = get_data_from_ingestion_bucket(bucket_path, session)

    pii_fields = ["name", "email_address"]

    if response["status"] == "success":
        df = response["data"]
    else:
        return response
    
    print("hello")
    print(df)
    
    for field in pii_fields:
        df[field] = "***"

    output = {"status": "success", "data": df}

    print(df)
    return output