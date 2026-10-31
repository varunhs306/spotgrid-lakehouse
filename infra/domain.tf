# Custom domain for the API: CloudFront in front of the function URL, which cannot take a
# domain itself. DNS lives at name.com, so both records there are added by hand:
# the certificate's validation CNAME (api_cert_validation) and the CNAME to CloudFront (api_cdn_domain).

resource "aws_acm_certificate" "api" {
  domain_name       = var.api_domain
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }
}

# ACM reuses one validation CNAME per domain and account, so a record added before the
# first apply validates this certificate at once.
resource "aws_acm_certificate_validation" "api" {
  certificate_arn = aws_acm_certificate.api.arn

  timeouts {
    create = "10m"
  }
}

# The API sets Cache-Control (5 minutes) on data responses; anything without it is not cached.
# Origin is in the key so a cached CORS header always matches the caller. Host must not be:
# a function URL rejects any Host but its own.
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
    # Managed AllViewerExceptHostHeader: everything the caller sent except Host.
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
