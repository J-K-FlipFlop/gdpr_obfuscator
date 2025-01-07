import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import pandas as pd
import awswrangler as wr
from awswrangler.exceptions import NoFilesFound
import io

def get_parquet_data_from_ingestion_bucket(
    path: str, session: boto3.session.Session
) -> dict:
    """Downloads parquet data from S3 ingestion bucket and returns a pandas dataframe

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
        return {"status": "success", "data": df, "format": ".parquet"}
    except ClientError as ce:
        return {"status": "failure", "message": ce.response}
    except NoFilesFound as nff:
        return {"status": "failure", "message": nff}
    
def write_parquet_data(
    data: pd.DataFrame,
) -> dict:
    """Writes a pandas dataframe to parquet format

    Args:
        data: a pandas dataframe

    Returns:
        A dictionary containing the following:
            status: shows whether the function ran successfully
            message: a relevant success/failure message
    """

    file_name = "obfuscated_data.parquet"

    if isinstance(data, pd.DataFrame):
        try:
            new_parquet = data.to_parquet()
            return {
                "status": "success",
                "message": "parquet written to byte stream",
                "byte_stream": new_parquet
            }
        except ClientError as e:
            return {
                "status": "failure",
                "message": e.response,
            }
    else:
        return {
            "status": "failure",
            "message": f"Data is in wrong format {str(type(data))} is not a pandas dataframe",
        }