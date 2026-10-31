output "serving_bucket" {
  value = aws_s3_bucket.serving.bucket
}

output "export_role_arn" {
  value = aws_iam_role.export.arn
}

output "probe_role_arn" {
  value = aws_iam_role.probe.arn
}

output "api_url" {
  value = aws_lambda_function_url.api.function_url
}

output "api_zip_key" {
  value = local.api_zip_key
}

output "api_custom_url" {
  value = "https://${var.api_domain}/"
}

output "api_dns_target" {
  value = var.api_cdn ? aws_cloudfront_distribution.api[0].domain_name : aws_apigatewayv2_domain_name.api[0].domain_name_configuration[0].target_domain_name
}

output "api_cert_validation" {
  value = aws_acm_certificate.api.domain_validation_options
}
