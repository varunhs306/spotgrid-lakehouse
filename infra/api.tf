# Public read-only API: FastAPI on Lambda behind the HTTP API in domain.tf. It reads the gold
# export from the serving bucket and never the warehouse. The probe Lambda times it from the
# public domain every 30 minutes.
#
# The zip (scripts/build_lambda.py) is over Lambda's 50 MB direct-upload limit, so the apply
# workflow copies it to the serving bucket under its sha256 and the functions load it from
# there. Both functions share it.

locals {
  api_name    = "spotgrid-api"
  probe_name  = "spotgrid-probe"
  api_zip_key = "lambda/api-${filesha256(var.api_zip)}.zip"
}

resource "aws_cloudwatch_log_group" "api" {
  #checkov:skip=CKV_AWS_338:Logs are kept 7 days by rule; a year of request logs has no reader
  #checkov:skip=CKV_AWS_158:Request logs hold no secrets; a customer-managed KMS key costs 1 USD a month
  name              = "/aws/lambda/${local.api_name}"
  retention_in_days = 7
}

resource "aws_cloudwatch_log_group" "probe" {
  #checkov:skip=CKV_AWS_338:Probe results are kept in the serving bucket; logs only for failures
  #checkov:skip=CKV_AWS_158:Probe logs hold no secrets; a customer-managed KMS key costs 1 USD a month
  name              = "/aws/lambda/${local.probe_name}"
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
      "${aws_s3_bucket.serving.arn}/bulk/*",
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
  #checkov:skip=CKV_AWS_116:Invoked synchronously through API Gateway; there is nothing to dead-letter
  #checkov:skip=CKV_AWS_117:Reads S3 only; a VPC would need a NAT gateway or endpoints and adds nothing
  #checkov:skip=CKV_AWS_115:New accounts have 10 concurrent executions, too few to reserve from; the stage throttle caps use
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
  # pyarrow needs the memory to load fast; a request reads only the partitions it needs.
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

resource "aws_iam_role" "probe" {
  name                 = local.probe_name
  assume_role_policy   = data.aws_iam_policy_document.lambda_trust.json
  permissions_boundary = local.workload_boundary
}

data "aws_iam_policy_document" "probe" {
  statement {
    sid       = "WriteProbeResults"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.serving.arn}/probes/*"]
  }

  statement {
    sid       = "WriteLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.probe.arn}:*"]
  }
}

resource "aws_iam_role_policy" "probe" {
  name   = "write-probe-results"
  role   = aws_iam_role.probe.id
  policy = data.aws_iam_policy_document.probe.json
}

resource "aws_lambda_function" "probe" {
  #checkov:skip=CKV_AWS_116:Invoked by the scheduler; a failure is the signal, raised by the errors alarm
  #checkov:skip=CKV_AWS_117:Calls the public API and writes S3; a VPC would need a NAT gateway
  #checkov:skip=CKV_AWS_115:Runs once every 30 minutes
  #checkov:skip=CKV_AWS_173:The variables are the public API URL and a bucket name
  #checkov:skip=CKV_AWS_272:Code is built and deployed by CI from main only
  #checkov:skip=CKV_AWS_50:A single short function; logs and alarms cover it
  function_name    = local.probe_name
  role             = aws_iam_role.probe.arn
  runtime          = "python3.13"
  architectures    = ["arm64"]
  handler          = "spotgrid_lakehouse.monitoring.probe.handler"
  s3_bucket        = aws_s3_bucket.serving.id
  s3_key           = local.api_zip_key
  source_code_hash = filebase64sha256(var.api_zip)
  memory_size      = 256
  timeout          = 120

  environment {
    variables = {
      API_URL        = "https://${var.api_domain}"
      SERVING_BUCKET = aws_s3_bucket.serving.id
    }
  }

  logging_config {
    log_format = "Text"
    log_group  = aws_cloudwatch_log_group.probe.name
  }

  depends_on = [aws_iam_role_policy.probe]
}

# EventBridge Scheduler invokes the API every 5 minutes to keep one container warm with the
# newest partitions loaded, and the probe every 30 minutes.
data "aws_iam_policy_document" "scheduler_trust" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }
}

resource "aws_iam_role" "api_warmup" {
  name                 = "${local.api_name}-warmup"
  assume_role_policy   = data.aws_iam_policy_document.scheduler_trust.json
  permissions_boundary = local.workload_boundary
}

data "aws_iam_policy_document" "api_warmup" {
  statement {
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_function.api.arn, aws_lambda_function.probe.arn]
  }
}

resource "aws_iam_role_policy" "api_warmup" {
  name   = "invoke-api-and-probe"
  role   = aws_iam_role.api_warmup.id
  policy = data.aws_iam_policy_document.api_warmup.json
}

resource "aws_scheduler_schedule" "api_warmup" {
  #checkov:skip=CKV_AWS_297:The schedule holds a constant payload; a customer-managed KMS key costs 1 USD a month
  name                = "${local.api_name}-warmup"
  group_name          = aws_scheduler_schedule_group.spotgrid.name
  schedule_expression = "rate(5 minutes)"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.api.arn
    role_arn = aws_iam_role.api_warmup.arn
    input    = jsonencode({ warmup = true })

    # A missed ping costs one slow request at worst; the next one is 5 minutes away.
    retry_policy {
      maximum_retry_attempts = 0
    }
  }
}

resource "aws_scheduler_schedule" "probe" {
  #checkov:skip=CKV_AWS_297:The schedule holds no payload; a customer-managed KMS key costs 1 USD a month
  name                = local.probe_name
  group_name          = aws_scheduler_schedule_group.spotgrid.name
  schedule_expression = "cron(13,43 * * * ? *)"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.probe.arn
    role_arn = aws_iam_role.api_warmup.arn

    retry_policy {
      maximum_retry_attempts = 0
    }
  }
}
