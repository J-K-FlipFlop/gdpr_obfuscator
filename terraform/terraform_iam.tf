resource "aws_iam_role" "transform_lambda_role" {
  name               = "transform-lambda"
  assume_role_policy = data.aws_iam_policy_document.transform_lambda_trust_policy.json
}

data "aws_iam_policy_document" "transform_lambda_trust_policy" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

data "aws_iam_policy_document" "read_from_ingestion_zone" {
  statement {
    # actions   = ["s3:GetObject", "s3:ListBucket", "s3:*"]
    actions   = ["s3:*"]
    resources = ["*"]
  }
}

data "aws_iam_policy_document" "write_to_processed_zone" {
  statement {
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.tf_processed_zone.arn}/*"]
  }
}

data "aws_iam_policy_document" "access_all_buckets" {
  statement {
    sid    = "EnableAnotherAWSAccountToReadTheSecret"
    effect = "Allow"

    actions   = ["s3:ListAllMyBuckets"]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "list-buckets-policy" {
  name   = "list-buckets-policy-ingestion"
  policy = data.aws_iam_policy_document.access_all_buckets.json
}

resource "aws_iam_role_policy_attachment" "attach_list_policy_to_transform_lambda" {
  role       = aws_iam_role.transform_lambda_role.name
  policy_arn = aws_iam_policy.list-buckets-policy.arn
}

resource "aws_iam_policy" "read_policy_ingestion_zone" {
  name   = "read-policy-ingestion-zone"
  policy = data.aws_iam_policy_document.read_from_ingestion_zone.json
}

resource "aws_iam_role_policy_attachment" "attach_read_policy_to_transform_lambda" {
  role       = aws_iam_role.transform_lambda_role.name
  policy_arn = aws_iam_policy.read_policy_ingestion_zone.arn
}

resource "aws_iam_policy" "write_policy_processed_zone" {
  name   = "write-policy-processed-zone"
  policy = data.aws_iam_policy_document.write_to_processed_zone.json
}

resource "aws_iam_role_policy_attachment" "attach_write_policy_to_transform_lambda" {
  role       = aws_iam_role.transform_lambda_role.name
  policy_arn = aws_iam_policy.write_policy_processed_zone.arn
}


data "aws_iam_policy_document" "transform_lambda_cloudwatch" {
  statement {
    actions   = ["logs:CreateLogGroup"]
    resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"]
  }
  statement {
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${aws_lambda_function.transform_lambda.function_name}:*"]
  }
}

resource "aws_iam_policy" "cloudwatch_policy_transform_lambda" {
  name   = "cloudwatch-policy-transform-lambda"
  policy = data.aws_iam_policy_document.transform_lambda_cloudwatch.json
}

resource "aws_iam_role_policy_attachment" "attach_cloudwatch_to_transform_lambda" {
  role       = aws_iam_role.transform_lambda_role.name
  policy_arn = aws_iam_policy.cloudwatch_policy_transform_lambda.arn
}


data "aws_iam_policy_document" "set_alarm_status_transform" {
  statement {
    actions   = ["cloudwatch:SetAlarmState"]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "set_alarm_status_transform_policy" {
  name   = "set_alarm_status_transform_policy"
  policy = data.aws_iam_policy_document.set_alarm_status_transform.json
}

resource "aws_iam_role_policy_attachment" "attach_set_alarm_status_policy_to_transform_lambda" {
  role       = aws_iam_role.transform_lambda_role.name
  policy_arn = aws_iam_policy.set_alarm_status_transform_policy.arn
}