# The export step publishes gold to the serving bucket and nothing else. It is a separate
# role from the CI apply role, so the job that moves data cannot change infrastructure.

locals {
  account_id           = data.aws_caller_identity.current.account_id
  github_oidc_provider = "arn:aws:iam::${local.account_id}:oidc-provider/token.actions.githubusercontent.com"
  workload_boundary    = "arn:aws:iam::${local.account_id}:policy/spotgrid-workload-boundary"
}

# Only workflows running on main can publish; pull requests cannot.
data "aws_iam_policy_document" "export_trust" {
  #checkov:skip=CKV_AWS_358:Sub is GitHub's immutable owner@id/repo@id form, pinned to main; the check's repo regex predates it
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [local.github_oidc_provider]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["${var.github_sub_prefix}:ref:refs/heads/main"]
    }
  }
}

resource "aws_iam_role" "export" {
  name                 = "spotgrid-export"
  assume_role_policy   = data.aws_iam_policy_document.export_trust.json
  permissions_boundary = local.workload_boundary
}

data "aws_iam_policy_document" "export" {
  statement {
    sid     = "PublishGold"
    actions = ["s3:PutObject"]
    resources = [
      "${aws_s3_bucket.serving.arn}/gold/*",
      "${aws_s3_bucket.serving.arn}/_manifest.json",
    ]
  }
}

resource "aws_iam_role_policy" "export" {
  name   = "publish-gold"
  role   = aws_iam_role.export.id
  policy = data.aws_iam_policy_document.export.json
}
