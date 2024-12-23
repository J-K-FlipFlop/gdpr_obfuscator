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
        return {"status": "success", "data": df, "format": ".csv"}
    except ClientError as ce:
        return {"status": "failure", "message": ce.response}
    except NoFilesFound as nff:
        return {"status": "failure", "message": nff}
    
def write_csv_data(
    data: pd.DataFrame,
) -> dict:
    """Writes a pandas dataframe to csv format

    Args:
        data: a pandas dataframe

    Returns:
        A dictionary containing the following:
            status: shows whether the function ran successfully
            message: a relevant success/failure message
    """

    file_name = "obfuscated_data.csv"

    if isinstance(data, pd.DataFrame):
        try:
            data.to_csv(f"src/{file_name}", sep='\t')
            return {
                "status": "success",
                "message": f"written to {file_name}",
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
