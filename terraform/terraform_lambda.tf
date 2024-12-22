resource "aws_lambda_function" "extract_lambda" {
  function_name    = "extract_lambda"
  filename         = "${path.module}/../lambda_extract.zip"
  role             = aws_iam_role.extract_lambda_role.arn
  handler          = "handler.lambda_handler"
  runtime          = "python3.11"
  source_code_hash = data.archive_file.extract_lambda_dir_zip.output_base64sha256
  layers           = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python311:12", aws_lambda_layer_version.utility_layer.arn]
  timeout          = 45
  memory_size      = 1024
}

#create zip file for main lambda function
data "archive_file" "extract_lambda_dir_zip" {
  type        = "zip"
  source_file = "${path.module}/../src/extract_lambda/handler.py"
  output_path = "${path.module}/../lambda_extract.zip"
}