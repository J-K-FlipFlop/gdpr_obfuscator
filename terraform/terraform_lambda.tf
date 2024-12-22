resource "aws_lambda_function" "transform_lambda" {
  function_name    = "transform_lambda"
  filename         = "${path.module}/../lambda_transform.zip"
  role             = aws_iam_role.transform_lambda_role.arn
  handler          = "handler.lambda_handler"
  runtime          = "python3.11"
  source_code_hash = data.archive_file.transform_lambda_dir_zip.output_base64sha256
  layers           = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python311:12", aws_lambda_layer_version.utility_layer.arn]
  timeout          = 45
  memory_size      = 1024
}

#create zip file for main lambda function
data "archive_file" "transform_lambda_dir_zip" {
  type        = "zip"
  source_file = "${path.module}/../src/transform_lambda/handler.py"
  output_path = "${path.module}/../lambda_transform.zip"
}