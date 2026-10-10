output "serving_bucket" {
  value = aws_s3_bucket.serving.bucket
}

output "export_role_arn" {
  value = aws_iam_role.export.arn
}

output "api_url" {
  value = aws_lambda_function_url.api.function_url
}

output "api_zip_key" {
  value = local.api_zip_key
}
