output "state_bucket" {
  value = aws_s3_bucket.tf_state.bucket
}

output "github_oidc_provider_arn" {
  value = aws_iam_openid_connect_provider.github.arn
}
