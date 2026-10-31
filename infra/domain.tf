# Custom domain for the API. DNS records are added by hand at name.com.

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

# Host is not forwarded: a function URL rejects any Host but its own.
resource "aws_cloudfront_cache_policy" "api" {
  name        = "spotgrid-api"
  default_ttl = 0
  min_ttl     = 0
  max_ttl     = 3600

  parameters_in_cache_key_and_forwarded_to_origin {
    enable_accept_encoding_brotli = true
    enable_accept_encoding_gzip   = true

    cookies_config {
      cookie_behavior = "none"
    }

    headers_config {
      header_behavior = "whitelist"
      headers {
        items = ["Origin"]
      }
    }

    query_strings_config {
      query_string_behavior = "all"
    }
  }
}

resource "aws_cloudfront_distribution" "api" {
  #checkov:skip=CKV_AWS_68:WAF costs 5 USD a month plus per-rule fees; alarms watch request volume
  #checkov:skip=CKV2_AWS_47:No WAF, see above
  #checkov:skip=CKV_AWS_86:Access logs need a log bucket; Lambda logs every request already
  #checkov:skip=CKV_AWS_310:A single function URL; there is no second origin to fail over to
  #checkov:skip=CKV_AWS_374:Open data for any country
  #checkov:skip=CKV2_AWS_32:JSON API; browser security headers protect HTML pages
  #checkov:skip=CKV_AWS_305:No root object; the API serves its own routes
  count = var.api_cdn ? 1 : 0

  enabled         = true
  is_ipv6_enabled = true
  comment         = "spotgrid-api"
  aliases         = [var.api_domain]
  price_class     = "PriceClass_100"

  origin {
    origin_id   = "spotgrid-api"
    domain_name = trimsuffix(trimprefix(aws_lambda_function_url.api.function_url, "https://"), "/")

    custom_origin_config {
      http_port              = 80
      https_port             = 443
      origin_protocol_policy = "https-only"
      origin_ssl_protocols   = ["TLSv1.2"]
    }
  }

  default_cache_behavior {
    target_origin_id       = "spotgrid-api"
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD", "OPTIONS"]
    cached_methods         = ["GET", "HEAD"]
    compress               = true
    cache_policy_id        = aws_cloudfront_cache_policy.api.id
    # Managed AllViewerExceptHostHeader
    origin_request_policy_id = "b689b0a8-53d0-40ab-baf2-68738e2966ac"
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    acm_certificate_arn      = aws_acm_certificate_validation.api.certificate_arn
    ssl_support_method       = "sni-only"
    minimum_protocol_version = "TLSv1.2_2021"
  }
}

resource "aws_apigatewayv2_api" "api" {
  count = var.api_cdn ? 0 : 1

  name          = "spotgrid-api"
  protocol_type = "HTTP"

  cors_configuration {
    allow_origins = ["*"]
    allow_methods = ["GET"]
    max_age       = 86400
  }
}

resource "aws_apigatewayv2_integration" "api" {
  count = var.api_cdn ? 0 : 1

  api_id                 = aws_apigatewayv2_api.api[0].id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.api.invoke_arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "api" {
  #checkov:skip=CKV_AWS_309:Public read-only API over open data (CC BY 4.0)
  count = var.api_cdn ? 0 : 1

  api_id    = aws_apigatewayv2_api.api[0].id
  route_key = "$default"
  target    = "integrations/${aws_apigatewayv2_integration.api[0].id}"
}

resource "aws_apigatewayv2_stage" "api" {
  #checkov:skip=CKV_AWS_76:Lambda logs every request already
  count = var.api_cdn ? 0 : 1

  api_id      = aws_apigatewayv2_api.api[0].id
  name        = "$default"
  auto_deploy = true

  default_route_settings {
    throttling_rate_limit  = 20
    throttling_burst_limit = 50
  }
}

resource "aws_lambda_permission" "api_gateway" {
  count = var.api_cdn ? 0 : 1

  statement_id  = "ApiGateway"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.api.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.api[0].execution_arn}/*/*"
}

resource "aws_apigatewayv2_domain_name" "api" {
  count = var.api_cdn ? 0 : 1

  domain_name = var.api_domain

  domain_name_configuration {
    certificate_arn = aws_acm_certificate_validation.api.certificate_arn
    endpoint_type   = "REGIONAL"
    security_policy = "TLS_1_2"
  }
}

resource "aws_apigatewayv2_api_mapping" "api" {
  count = var.api_cdn ? 0 : 1

  api_id      = aws_apigatewayv2_api.api[0].id
  domain_name = aws_apigatewayv2_domain_name.api[0].id
  stage       = aws_apigatewayv2_stage.api[0].id
}
