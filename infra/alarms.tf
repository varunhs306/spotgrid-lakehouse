# Three alarms, one email topic. Pipeline failures open GitHub issues instead; these watch the
# public API.

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
  api_dimensions = { FunctionName = aws_lambda_function.api.function_name }
}

# Function errors and timeouts reach callers as 5xx too, so this one alarm covers both.
resource "aws_cloudwatch_metric_alarm" "api_5xx" {
  alarm_name          = "spotgrid-api-5xx"
  alarm_description   = "The public API returned server errors."
  namespace           = "AWS/Lambda"
  metric_name         = "Url5xxCount"
  dimensions          = local.api_dimensions
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  threshold           = 1
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alarms.arn]
  ok_actions          = [aws_sns_topic.alarms.arn]
}

# The API is public and unthrottled. Far above organic use, far below the free tier
# (1M requests a month).
resource "aws_cloudwatch_metric_alarm" "api_traffic" {
  alarm_name          = "spotgrid-api-traffic"
  alarm_description   = "Unusual request volume on the public API."
  namespace           = "AWS/Lambda"
  metric_name         = "UrlRequestCount"
  dimensions          = local.api_dimensions
  statistic           = "Sum"
  period              = 3600
  evaluation_periods  = 1
  comparison_operator = "GreaterThanThreshold"
  threshold           = 5000
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alarms.arn]
  ok_actions          = [aws_sns_topic.alarms.arn]
}

# Throttled callers get a 429, which the 5xx alarm does not see.
resource "aws_cloudwatch_metric_alarm" "api_throttles" {
  alarm_name          = "spotgrid-api-throttles"
  alarm_description   = "Requests to the public API were throttled."
  namespace           = "AWS/Lambda"
  metric_name         = "Throttles"
  dimensions          = local.api_dimensions
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  threshold           = 1
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alarms.arn]
  ok_actions          = [aws_sns_topic.alarms.arn]
}
