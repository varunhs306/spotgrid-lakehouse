# The API's custom domain on an HTTP API. DNS records are added by hand at name.com.

resource "aws_acm_certificate" "api" {
  domain_name       = var.api_domain
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_acm_certificate_validation" "api" {
  certificate_arn = aws_acm_certificate.api.arn

  timeouts {
    create = "10m"
  }
}

resource "aws_apigatewayv2_api" "api" {
  name          = "spotgrid-api"
  protocol_type = "HTTP"

  # The market page calls the API from the browser.
  cors_configuration {
    allow_origins = ["*"]
    allow_methods = ["GET"]
    max_age       = 86400
  }
}

resource "aws_apigatewayv2_integration" "api" {
  api_id                 = aws_apigatewayv2_api.api.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.api.invoke_arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "api" {
  #checkov:skip=CKV_AWS_309:Public read-only API over open data (CC BY 4.0)
  api_id    = aws_apigatewayv2_api.api.id
  route_key = "$default"
  target    = "integrations/${aws_apigatewayv2_integration.api.id}"
}

resource "aws_apigatewayv2_stage" "api" {
  #checkov:skip=CKV_AWS_76:Lambda logs every request already
  api_id      = aws_apigatewayv2_api.api.id
  name        = "$default"
  auto_deploy = true

  # Also the cost ceiling: at most about 52 million requests a month reach Lambda.
  default_route_settings {
    throttling_rate_limit  = 20
    throttling_burst_limit = 50
  }
}

resource "aws_lambda_permission" "api_gateway" {
  statement_id  = "ApiGateway"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.api.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.api.execution_arn}/*/*"
}

resource "aws_apigatewayv2_domain_name" "api" {
  domain_name = var.api_domain

  domain_name_configuration {
    certificate_arn = aws_acm_certificate_validation.api.certificate_arn
    endpoint_type   = "REGIONAL"
    security_policy = "TLS_1_2"
  }
}

resource "aws_apigatewayv2_api_mapping" "api" {
  api_id      = aws_apigatewayv2_api.api.id
  domain_name = aws_apigatewayv2_domain_name.api.id
  stage       = aws_apigatewayv2_stage.api.id
}
