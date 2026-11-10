output "serving_bucket" {
  value = aws_s3_bucket.serving.bucket
}

output "lake_bucket" {
  value = aws_s3_bucket.lake.bucket
}

output "api_zip_key" {
  value = local.api_zip_key
}

output "api_endpoint" {
  value = aws_apigatewayv2_api.api.api_endpoint
}

output "api_custom_url" {
  value = "https://${var.api_domain}/"
}

output "api_dns_target" {
  value = aws_apigatewayv2_domain_name.api.domain_name_configuration[0].target_domain_name
}

output "api_cert_validation" {
  value = aws_acm_certificate.api.domain_validation_options
}

output "pipeline_repository_url" {
  value = aws_ecr_repository.pipeline.repository_url
}

output "pipeline_cluster" {
  value = aws_ecs_cluster.pipeline.name
}

output "pipeline_task_definition" {
  value = aws_ecs_task_definition.pipeline.family
}

output "pipeline_subnets" {
  value = aws_subnet.public[*].id
}

output "pipeline_security_group" {
  value = aws_security_group.pipeline.id
}
