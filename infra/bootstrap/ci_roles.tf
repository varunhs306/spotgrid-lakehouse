# Everything the CI roles manage is named spotgrid-*. The roles themselves and the
# boundary are excluded, so a workflow cannot widen its own permissions.

locals {
  account_id = data.aws_caller_identity.current.account_id
  arn_prefix = "arn:aws:%s:${var.region}:${local.account_id}"

  state_objects = "${aws_s3_bucket.tf_state.arn}/infra/*"
  ci_role_arns  = "arn:aws:iam::${local.account_id}:role/spotgrid-ci-*"
  workload_role = "arn:aws:iam::${local.account_id}:role/spotgrid-*"
  workload_pol  = "arn:aws:iam::${local.account_id}:policy/spotgrid-*"
  boundary_arn  = "arn:aws:iam::${local.account_id}:policy/spotgrid-workload-boundary"
}

data "aws_iam_policy_document" "github_trust" {
  for_each = {
    plan  = "pull_request"
    apply = "ref:refs/heads/main"
  }

  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["${var.github_sub_prefix}:${each.value}"]
    }
  }
}

# Shared by both roles: state access and the reads terraform plan needs.
data "aws_iam_policy_document" "ci_read" {
  statement {
    sid       = "StateList"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.tf_state.arn]
  }

  statement {
    sid       = "StateRead"
    actions   = ["s3:GetObject"]
    resources = [local.state_objects]
  }

  statement {
    sid       = "StateLock"
    actions   = ["s3:PutObject", "s3:DeleteObject"]
    resources = ["${local.state_objects}.tflock"]
  }

  statement {
    sid       = "ReadBuckets"
    actions   = ["s3:GetBucket*", "s3:GetLifecycleConfiguration", "s3:GetEncryptionConfiguration", "s3:GetAccelerateConfiguration", "s3:GetReplicationConfiguration", "s3:ListBucket"]
    resources = ["arn:aws:s3:::spotgrid-serving-*"]
  }

  statement {
    sid       = "ReadLambda"
    actions   = ["lambda:Get*", "lambda:List*"]
    resources = ["${format(local.arn_prefix, "lambda")}:function:spotgrid-*"]
  }

  statement {
    sid       = "ReadIam"
    actions   = ["iam:Get*", "iam:List*"]
    resources = [local.workload_role, local.workload_pol]
  }

  statement {
    sid       = "ReadLogs"
    actions   = ["logs:ListTagsForResource"]
    resources = ["${format(local.arn_prefix, "logs")}:log-group:/aws/lambda/spotgrid-*"]
  }

  statement {
    sid       = "DescribeUnscoped"
    actions   = ["logs:DescribeLogGroups", "cloudwatch:DescribeAlarms"]
    resources = ["*"]
  }

  statement {
    sid       = "ReadAlarms"
    actions   = ["cloudwatch:ListTagsForResource"]
    resources = ["${format(local.arn_prefix, "cloudwatch")}:alarm:spotgrid-*"]
  }

  statement {
    sid       = "ReadSns"
    actions   = ["sns:GetTopicAttributes", "sns:GetSubscriptionAttributes", "sns:ListSubscriptionsByTopic", "sns:ListTagsForResource"]
    resources = ["${format(local.arn_prefix, "sns")}:spotgrid-*"]
  }
}

data "aws_iam_policy_document" "ci_write" {
  statement {
    sid       = "StateWrite"
    actions   = ["s3:PutObject", "s3:DeleteObject"]
    resources = [local.state_objects]
  }

  statement {
    sid       = "ManageServingBuckets"
    actions   = ["s3:*"]
    resources = ["arn:aws:s3:::spotgrid-serving-*", "arn:aws:s3:::spotgrid-serving-*/*"]
  }

  statement {
    sid       = "ManageLambda"
    actions   = ["lambda:*"]
    resources = ["${format(local.arn_prefix, "lambda")}:function:spotgrid-*"]
  }

  statement {
    sid       = "ManageLogs"
    actions   = ["logs:*"]
    resources = ["${format(local.arn_prefix, "logs")}:log-group:/aws/lambda/spotgrid-*"]
  }

  statement {
    sid       = "ManageAlarms"
    actions   = ["cloudwatch:PutMetricAlarm", "cloudwatch:DeleteAlarms", "cloudwatch:TagResource", "cloudwatch:UntagResource"]
    resources = ["${format(local.arn_prefix, "cloudwatch")}:alarm:spotgrid-*"]
  }

  statement {
    sid       = "ManageSns"
    actions   = ["sns:*"]
    resources = ["${format(local.arn_prefix, "sns")}:spotgrid-*"]
  }

  statement {
    sid       = "ManageWorkloadPolicies"
    actions   = ["iam:CreatePolicy", "iam:CreatePolicyVersion", "iam:DeletePolicy", "iam:DeletePolicyVersion", "iam:TagPolicy", "iam:UntagPolicy"]
    resources = [local.workload_pol]
  }

  statement {
    sid       = "CreateBoundedRoles"
    actions   = ["iam:CreateRole", "iam:PutRolePermissionsBoundary"]
    resources = [local.workload_role]

    condition {
      test     = "StringEquals"
      variable = "iam:PermissionsBoundary"
      values   = [local.boundary_arn]
    }
  }

  statement {
    sid = "ManageWorkloadRoles"
    actions = [
      "iam:DeleteRole", "iam:UpdateRole", "iam:UpdateAssumeRolePolicy", "iam:TagRole", "iam:UntagRole",
      "iam:AttachRolePolicy", "iam:DetachRolePolicy", "iam:PutRolePolicy", "iam:DeleteRolePolicy",
    ]
    resources = [local.workload_role]
  }

  statement {
    sid       = "PassRoleToLambda"
    actions   = ["iam:PassRole"]
    resources = [local.workload_role]

    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      values   = ["lambda.amazonaws.com"]
    }
  }

  statement {
    sid       = "ProtectCiRolesAndBoundary"
    effect    = "Deny"
    actions   = ["iam:*"]
    resources = [local.ci_role_arns, local.boundary_arn]
  }

  statement {
    sid       = "KeepBoundary"
    effect    = "Deny"
    actions   = ["iam:DeleteRolePermissionsBoundary"]
    resources = ["*"]
  }
}

# Ceiling for every role the apply role creates (Lambda API, export jobs).
data "aws_iam_policy_document" "workload_boundary" {
  statement {
    sid       = "ServingData"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = ["arn:aws:s3:::spotgrid-serving-*", "arn:aws:s3:::spotgrid-serving-*/*"]
  }

  statement {
    sid       = "LambdaLogs"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${format(local.arn_prefix, "logs")}:log-group:/aws/lambda/spotgrid-*"]
  }
}

resource "aws_iam_policy" "workload_boundary" {
  name   = "spotgrid-workload-boundary"
  policy = data.aws_iam_policy_document.workload_boundary.json
}

resource "aws_iam_role" "ci" {
  for_each = data.aws_iam_policy_document.github_trust

  name               = "spotgrid-ci-${each.key}"
  assume_role_policy = each.value.json
}

resource "aws_iam_role_policy" "ci_read" {
  for_each = aws_iam_role.ci

  name   = "read"
  role   = each.value.id
  policy = data.aws_iam_policy_document.ci_read.json
}

resource "aws_iam_role_policy" "ci_write" {
  name   = "write"
  role   = aws_iam_role.ci["apply"].id
  policy = data.aws_iam_policy_document.ci_write.json
}
