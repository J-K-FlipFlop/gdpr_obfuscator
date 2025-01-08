# GDPR Obfuscator Project

## Context
The purpose of this project is to create a general-purpose tool to process data being ingested to AWS and
intercept personally identifiable information (PII). All information stored by Northcoders data projects should be for bulk data analysis only. Consequently, there is a requirement under [GDPR](https://ico.org.uk/media/for-organisations/guide-to-data-protection/guide-to-the-general-data-protection-regulation-gdpr-1-1.pdf) to ensure that all data containing 
information that can be used to identify an individual should be anonymised.


## Description

The tool is designed to run within an AWS Lambda function. The tool expects a JSON input in the following format: 
{
    "file_to_obfuscate": "s3://<source_bucket>/<source_file>",
    "pii_fields": ["field1", "field2"],
    "destination": "s3://<destination_bucket>/<destination_file>"
}
The tool will then anonymize the selected fields in the chosen file by replacing each value with "***". The resulting file will then be placed in an s3 bucket, specified by the destination parameter.

Currently csv, parquet, and json file formats are accepted.

The project includes a Makefile to streamline setup, which includes the dowload of dependencies and the option to run other tools such as black, safety and bandit.

The project includes Terraform code to simplify the deployment of AWS resources required for this tool. If uploaded using terraform the state bucket in main.tf will have to be changed manually. The terraform does not upload all of the dependencies for using parquet files, the dependency for parquet will need to manually be added in a layer in aws if that wishes to be used. 
