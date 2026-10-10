# Public read-only API: FastAPI on Lambda behind a function URL. It reads the gold export
# from the serving bucket and never the SQL warehouse.
#
# The zip (scripts/build_lambda.py) is over Lambda's 50 MB direct-upload limit, so the apply
# workflow copies it to the serving bucket under its sha256 and the function loads it from
# there. The plan role never needs to read the object.

locals {
  api_name    = "spotgrid-api"
  api_zip_key = "lambda/api-${filesha256(var.api_zip)}.zip"
}

resource "aws_cloudwatch_log_group" "api" {
  #checkov:skip=CKV_AWS_338:Logs are kept 7 days by rule; a year of request logs has no reader
  #checkov:skip=CKV_AWS_158:Request logs hold no secrets; a customer-managed KMS key costs 1 USD a month
  name              = "/aws/lambda/${local.api_name}"
  retention_in_days = 7
}

data "aws_iam_policy_document" "lambda_trust" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "api" {
  name                 = local.api_name
  assume_role_policy   = data.aws_iam_policy_document.lambda_trust.json
  permissions_boundary = local.workload_boundary
}

data "aws_iam_policy_document" "api" {
  statement {
    sid     = "ReadGold"
    actions = ["s3:GetObject"]
    resources = [
      "${aws_s3_bucket.serving.arn}/gold/*",
      "${aws_s3_bucket.serving.arn}/_manifest.json",
    ]
  }

  # Without it a missing key reads as AccessDenied instead of NoSuchKey.
  statement {
    sid       = "ListServing"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.serving.arn]
  }

  statement {
    sid       = "WriteLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.api.arn}:*"]
  }
}

resource "aws_iam_role_policy" "api" {
  name   = "read-gold"
  role   = aws_iam_role.api.id
  policy = data.aws_iam_policy_document.api.json
}

resource "aws_lambda_function" "api" {
  #checkov:skip=CKV_AWS_116:Invoked synchronously through the function URL; there is nothing to dead-letter
  #checkov:skip=CKV_AWS_117:Reads S3 only; a VPC would need a NAT gateway or endpoints and adds nothing
  #checkov:skip=CKV_AWS_115:New accounts have 10 concurrent executions, too few to reserve from; the traffic alarm watches use
  #checkov:skip=CKV_AWS_173:The only variable is the bucket name, which is not secret
  #checkov:skip=CKV_AWS_272:Code is built and deployed by CI from main only; signing adds a profile and a job
  #checkov:skip=CKV_AWS_50:X-Ray needs permissions outside the workload boundary; logs and alarms cover a single function
  function_name    = local.api_name
  role             = aws_iam_role.api.arn
  runtime          = "python3.13"
  architectures    = ["arm64"]
  handler          = "spotgrid_lakehouse.api.lambda_handler.handler"
  s3_bucket        = aws_s3_bucket.serving.id
  s3_key           = local.api_zip_key
  source_code_hash = filebase64sha256(var.api_zip)
  # pyarrow needs the memory to load fast; a cold start reads two Parquet files from S3.
  memory_size = 512
  timeout     = 15

  environment {
    variables = {
      SERVING_BUCKET = aws_s3_bucket.serving.id
    }
  }

  logging_config {
    log_format = "Text"
    log_group  = aws_cloudwatch_log_group.api.name
  }

  depends_on = [aws_iam_role_policy.api]
}

resource "aws_lambda_function_url" "api" {
  #checkov:skip=CKV_AWS_258:Public read-only API over open data (CC BY 4.0); alarms watch for abuse
  function_name      = aws_lambda_function.api.function_name
  authorization_type = "NONE"

  # The market page on GitHub Pages calls the API from the browser.
  cors {
    allow_origins = ["*"]
    allow_methods = ["GET"]
    max_age       = 86400
  }
}

# A public function URL needs both statements: one for the URL, one for the invoke behind it.
resource "aws_lambda_permission" "api_url" {
  #checkov:skip=CKV_AWS_301:Public by design; the function only serves read-only open data
  statement_id           = "PublicFunctionUrl"
  action                 = "lambda:InvokeFunctionUrl"
  function_name          = aws_lambda_function.api.function_name
  principal              = "*"
  function_url_auth_type = "NONE"
}

resource "aws_lambda_permission" "api_invoke" {
  #checkov:skip=CKV_AWS_301:Public by design; the function only serves read-only open data
  statement_id             = "PublicInvokeViaFunctionUrl"
  action                   = "lambda:InvokeFunction"
  function_name            = aws_lambda_function.api.function_name
  principal                = "*"
  invoked_via_function_url = true
}
