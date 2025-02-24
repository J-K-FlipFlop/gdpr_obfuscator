import boto3
from src.transform_lambda.utils import (
    get_data_from_bucket,
    censor_sensitive_data,
    write_sensitive_data,
)
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    session = boto3.session.Session(region_name="eu-west-2")

    try:
        bucket_path = event["file_to_obfuscate"]
        pii_fields = event["pii_fields"]
        destination = event["destination"]
    except:
        return {"status": "failure", "message": "json input is incorrect"}

    try:
        response1 = get_data_from_bucket(bucket_path, session)

        if response1["status"] == "failure":
            return response1

        response2 = censor_sensitive_data(response1, pii_fields)

        if response2["status"] == "failure":
            return response2

        response3 = write_sensitive_data(response2, destination, session)
        return response3
    except:
        return {"status": "failure", "message": "unexpected error"}


# event = {
#     "file_to_obfuscate": "s3://<source_bucket>/<source_file>",
#     "pii_fields": ["field1", "field2"],
#     "destination": "s3://<destination_bucket>/<destination_file>"
# }


lambda_handler(event, "unused")
