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

# The CNAME target for var.api_domain at name.com.
output "api_cdn_domain" {
  value = aws_cloudfront_distribution.api.domain_name
}

output "api_cert_validation" {
  value = aws_acm_certificate.api.domain_validation_options
}
