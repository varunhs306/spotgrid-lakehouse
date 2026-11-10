# Alarms to one email topic. Data problems open GitHub issues and Telegram messages from the
# pipeline itself; these watch the API, the probe and whether the pipeline runs at all.

resource "aws_sns_topic" "alarms" {
  #checkov:skip=CKV_AWS_26:CloudWatch cannot publish to a topic under the AWS-managed SNS key, and a customer-managed key costs 1 USD a month
  name = "spotgrid-alarms"
}

resource "aws_sns_topic_subscription" "alarm_email" {
  topic_arn = aws_sns_topic.alarms.arn
  protocol  = "email"
  endpoint  = var.alarm_email
}

locals {
  api_dimensions = { ApiId = aws_apigatewayv2_api.api.id, Stage = aws_apigatewayv2_stage.api.name }
  alarm_actions  = [aws_sns_topic.alarms.arn]
}

# Function errors and timeouts reach callers as 5xx.
resource "aws_cloudwatch_metric_alarm" "api_5xx" {
  alarm_name          = "spotgrid-api-5xx"
  alarm_description   = "The public API returned server errors."
  namespace           = "AWS/ApiGateway"
  metric_name         = "5xx"
  dimensions          = local.api_dimensions
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  threshold           = 1
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
  ok_actions          = local.alarm_actions
}

# Far above organic use; the stage throttle caps it at 20 requests a second.
resource "aws_cloudwatch_metric_alarm" "api_traffic" {
  alarm_name          = "spotgrid-api-traffic"
  alarm_description   = "Unusual request volume on the public API."
  namespace           = "AWS/ApiGateway"
  metric_name         = "Count"
  dimensions          = local.api_dimensions
  statistic           = "Sum"
  period              = 3600
  evaluation_periods  = 1
  comparison_operator = "GreaterThanThreshold"
  threshold           = 5000
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
  ok_actions          = local.alarm_actions
}

resource "aws_cloudwatch_metric_alarm" "api_latency" {
  alarm_name          = "spotgrid-api-latency"
  alarm_description   = "The public API is slow: p95 over 3 seconds for 15 minutes."
  namespace           = "AWS/ApiGateway"
  metric_name         = "Latency"
  dimensions          = local.api_dimensions
  extended_statistic  = "p95"
  period              = 300
  evaluation_periods  = 3
  comparison_operator = "GreaterThanThreshold"
  threshold           = 3000
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
  ok_actions          = local.alarm_actions
}

# Every successful run sends one datapoint; none for 26 hours means the pipeline stopped:
# a failing step, a crash, a disabled schedule or broken permissions.
resource "aws_cloudwatch_metric_alarm" "pipeline_heartbeat" {
  alarm_name          = "spotgrid-pipeline-heartbeat"
  alarm_description   = "No successful pipeline run for 26 hours."
  namespace           = "spotgrid"
  metric_name         = "PipelineSuccess"
  statistic           = "Sum"
  period              = 3600
  evaluation_periods  = 26
  datapoints_to_alarm = 26
  comparison_operator = "LessThanThreshold"
  threshold           = 1
  treat_missing_data  = "breaching"
  alarm_actions       = local.alarm_actions
  ok_actions          = local.alarm_actions
}

resource "aws_cloudwatch_metric_alarm" "probe_errors" {
  alarm_name          = "spotgrid-probe-errors"
  alarm_description   = "The probe could not get a 200 from every endpoint through the public domain."
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  dimensions          = { FunctionName = aws_lambda_function.probe.function_name }
  statistic           = "Sum"
  period              = 1800
  evaluation_periods  = 2
  datapoints_to_alarm = 2
  comparison_operator = "GreaterThanOrEqualToThreshold"
  threshold           = 1
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
  ok_actions          = local.alarm_actions
}
