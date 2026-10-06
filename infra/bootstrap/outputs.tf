output "state_bucket" {
  value = aws_s3_bucket.tf_state.bucket
}

output "github_oidc_provider_arn" {
  value = aws_iam_openid_connect_provider.github.arn
}

output "ci_plan_role_arn" {
  value = aws_iam_role.ci["plan"].arn
}

output "ci_apply_role_arn" {
  value = aws_iam_role.ci["apply"].arn
}

output "workload_boundary_arn" {
  value = aws_iam_policy.workload_boundary.arn
}
