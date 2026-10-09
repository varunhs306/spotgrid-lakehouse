output "serving_bucket" {
  value = aws_s3_bucket.serving.bucket
}

output "export_role_arn" {
  value = aws_iam_role.export.arn
}
