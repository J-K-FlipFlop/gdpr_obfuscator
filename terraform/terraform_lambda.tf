resource "aws_lambda_function" "transform_lambda" {
  function_name    = "transform_lambda"
  filename         = "${path.module}/../lambda_transform.zip"
  role             = aws_iam_role.transform_lambda_role.arn
  handler          = "handler.lambda_handler"
  runtime          = "python3.12"
  source_code_hash = data.archive_file.transform_lambda_dir_zip.output_base64sha256
  layers           = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python311:19", aws_lambda_layer_version.utility_layer_transform.arn]
  timeout          = 45
  memory_size      = 1024
}

#create zip file for transform lambda function
data "archive_file" "transform_lambda_dir_zip" {
  type        = "zip"
  source_file = "${path.module}/../src/transform_lambda/handler.py"
  output_path = "${path.module}/../lambda_transform.zip"
}

locals {
  source_files_transform = ["${path.module}/../src/transform_lambda/csv_utils.py", "${path.module}/../src/transform_lambda/utils.py", "${path.module}/../src/transform_lambda/json_utils.py", "${path.module}/../src/transform_lambda/parquet_utils.py"]
}

data "template_file" "t_file_transform" {
  count    = length(local.source_files_transform)
  template = file(element(local.source_files_transform, count.index))
}

resource "local_file" "to_temp_dir_transform" {
  count    = length(local.source_files_transform)
  filename = "${path.module}/temp_transform/python/src/transform_lambda/${basename(element(local.source_files_transform, count.index))}"
  content  = element(data.template_file.t_file_transform.*.rendered, count.index)
}

data "archive_file" "archive_transform" {
  type        = "zip"
  output_path = "${path.module}/../aws_utils/utils_transform.zip"
  source_dir  = "${path.module}/temp_transform"

  depends_on = [
    local_file.to_temp_dir_transform,
  ]
}

resource "aws_lambda_layer_version" "utility_layer_transform" {
  layer_name          = "util_layer_transform"
  compatible_runtimes = ["python3.12"]
  filename            = "${path.module}/../aws_utils/utils_transform.zip"
  source_code_hash    = data.archive_file.archive_transform.output_base64sha256
}