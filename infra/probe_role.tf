# The latency probe writes its results under probes/ and nothing else. It trusts the same
# main-only OIDC subject as the export role.

resource "aws_iam_role" "probe" {
  name                 = "spotgrid-probe"
  assume_role_policy   = data.aws_iam_policy_document.export_trust.json
  permissions_boundary = local.workload_boundary
}

data "aws_iam_policy_document" "probe" {
  statement {
    sid       = "WriteProbeResults"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.serving.arn}/probes/*"]
  }
}

resource "aws_iam_role_policy" "probe" {
  name   = "write-probe-results"
  role   = aws_iam_role.probe.id
  policy = data.aws_iam_policy_document.probe.json
}
