import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import pandas as pd
import awswrangler as wr
from awswrangler.exceptions import NoFilesFound
from src.transform_lambda.csv_utils import (
    get_csv_data_from_ingestion_bucket,
    write_csv_data
)
from src.transform_lambda.parquet_utils import (
    get_parquet_data_from_ingestion_bucket,
    write_parquet_data
)
from src.transform_lambda.json_utils import (
    get_json_data_from_ingestion_bucket,
    write_json_data
)
import os

def censor_sensitive_data(data: dict, pii_fields: list):

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

    df = data["data"]
    format = data["format"]
    
    try:
        for field in pii_fields:
            df[field] = "***"
        return {"status": "success", "data": df, "format": format}
    except ClientError as ce:
        return {"status": "failure", "message": ce.response}
    
def get_data_from_bucket(bucket_path: str, session: boto3.session.Session):

    """Reads a data file from a given path

    Args:
        bucket_path: path containing file
        session: boto3 session

    Returns:
        A dictionary containing the following:
            status: shows whether the function ran successfully
            data: a pandas dataframe (if successful)
            message: a relevant error message (if unsuccessful)
    """

    filename, file_extension = os.path.splitext(bucket_path)

    if file_extension == ".csv":
        response = get_csv_data_from_ingestion_bucket(bucket_path, session)
    elif file_extension == ".parquet":
        response = get_parquet_data_from_ingestion_bucket(bucket_path, session)
    elif file_extension == ".json":
        response = get_json_data_from_ingestion_bucket(bucket_path, session)
    else:
        return {
            "status": "failure",
            "message": f"Unsuported data type. Can only process csv, json, and parquet file types",
        }
    
    return response

def write_sensitive_data(response_dict: dict):

    """Reads a data frame into a file

    Args:
        response_dict: A dictionary of the form: {"status": "success", "data": df, "format": ".csv"} or
        if the response failed the dictionary will be :{"status": "failed", "message": "error message"}

    Returns:
        A dictionary containing the following:
            status: shows whether the function ran successfully
            data: a pandas dataframe (if successful)
            message: a relevant error message (if unsuccessful)
    """

    try:
        file_extension = response_dict["format"]
        df = response_dict["data"]

        if file_extension == ".csv":
            response = write_csv_data(df)
        elif file_extension == ".parquet":
            response = write_parquet_data(df)
        elif file_extension == ".json":
            response = write_json_data(df)
        else:
            return {
                "status": "failure",
                "message": "Unsuported data type. Can only process csv, json, and parquet file types",
            }
    except:
        return {
                "status": "failure",
                "source of error": response_dict,
                "message": "unexpected error",
            }
    
    return response
