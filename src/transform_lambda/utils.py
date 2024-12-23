import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import pandas as pd
import awswrangler as wr
from awswrangler.exceptions import NoFilesFound

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
    
