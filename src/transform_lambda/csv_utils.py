import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import pandas as pd
import awswrangler as wr
from awswrangler.exceptions import NoFilesFound
import io
import logging

logger = logging.getLogger("ftpuploader")


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
    data: pd.DataFrame, destination_bucket: str, session: boto3.session.Session
) -> dict:
    """Writes a pandas dataframe to csv format in destination bucket

    Args:
        data: a pandas dataframe

    Returns:
        A dictionary containing the following:
            status: shows whether the function ran successfully
            message: a relevant success/failure message
    """

    if isinstance(data, pd.DataFrame):
        try:
            wr.s3.to_csv(df=data, path=destination_bucket, boto3_session=session)
            return {
                "status": "success",
                "message": f"csv written to {destination_bucket}",
            }
        except ClientError as e:
            return {
                "status": "failure",
                "message": e.response,
            }
        except Exception as e:
            logger.error("Failed to upload to ftp: %s", repr(e))
            return {
                "status": "failure",
                "message": "did not write to s3. Please specify an appropriate destination i.e s3://my-bucket/my-file.csv",
            }
    else:
        return {
            "status": "failure",
            "message": f"Data is in wrong format {str(type(data))} is not a pandas dataframe",
        }
