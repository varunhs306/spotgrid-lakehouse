# Everything the CI roles manage is named spotgrid-*. The roles themselves and the
# boundary are excluded, so a workflow cannot widen its own permissions.

locals {
  account_id = data.aws_caller_identity.current.account_id
  # The workload lives in var.workload_region; this stack (state bucket) in var.region.
  arn_prefix = "arn:aws:%s:${var.workload_region}:${local.account_id}"

  state_objects = "${aws_s3_bucket.tf_state.arn}/infra/*"
  ci_role_arns  = "arn:aws:iam::${local.account_id}:role/spotgrid-ci-*"
  workload_role = "arn:aws:iam::${local.account_id}:role/spotgrid-*"
  workload_pol  = "arn:aws:iam::${local.account_id}:policy/spotgrid-*"
  boundary_arn  = "arn:aws:iam::${local.account_id}:policy/spotgrid-workload-boundary"

  # Certificate IDs are generated, so this covers the whole region.
  certificates = "${format(local.arn_prefix, "acm")}:certificate/*"
  http_apis    = [for path in ["/apis", "/apis/*", "/domainnames", "/domainnames/*", "/tags/*"] : "arn:aws:apigateway:${var.workload_region}::${path}"]
  buckets      = ["arn:aws:s3:::spotgrid-serving-*", "arn:aws:s3:::spotgrid-lake-*"]
  objects      = [for b in local.buckets : "${b}/*"]
  log_groups   = ["${format(local.arn_prefix, "logs")}:log-group:/aws/lambda/spotgrid-*", "${format(local.arn_prefix, "logs")}:log-group:/spotgrid/*"]
  ecr_repos    = "${format(local.arn_prefix, "ecr")}:repository/spotgrid-*"
  ssm_params   = "${format(local.arn_prefix, "ssm")}:parameter/spotgrid/*"
  cluster      = "${format(local.arn_prefix, "ecs")}:cluster/spotgrid"
  schedules    = ["${format(local.arn_prefix, "scheduler")}:schedule/spotgrid/spotgrid-*", "${format(local.arn_prefix, "scheduler")}:schedule-group/spotgrid"]
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
  #checkov:skip=CKV_AWS_356:Read-only Describe calls Terraform makes for alarms and log groups; scope them when those resources land
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
    resources = local.buckets
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
    resources = local.log_groups
  }

  statement {
    sid = "DescribeUnscoped"
    actions = [
      "logs:DescribeLogGroups", "cloudwatch:DescribeAlarms", "ec2:Describe*",
      "ecs:DescribeTaskDefinition", "ecr:GetAuthorizationToken", "ssm:DescribeParameters",
    ]
    resources = ["*"]
  }

  statement {
    sid       = "ReadSchedules"
    actions   = ["scheduler:GetSchedule", "scheduler:GetScheduleGroup", "scheduler:ListTagsForResource"]
    resources = local.schedules
  }

  statement {
    sid = "ReadPipeline"
    actions = [
      "ecr:Describe*", "ecr:GetLifecyclePolicy", "ecr:ListTagsForResource", "ecs:DescribeClusters",
      "ecs:ListTagsForResource", "ssm:GetParameter*", "ssm:ListTagsForResource",
    ]
    resources = [local.ecr_repos, local.cluster, local.ssm_params]
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

  statement {
    sid       = "ReadCertificates"
    actions   = ["acm:DescribeCertificate", "acm:ListTagsForCertificate"]
    resources = [local.certificates]
  }

  statement {
    sid       = "ReadHttpApis"
    actions   = ["apigateway:GET"]
    resources = local.http_apis
  }
}

data "aws_iam_policy_document" "ci_write" {
  statement {
    sid       = "StateWrite"
    actions   = ["s3:PutObject", "s3:DeleteObject"]
    resources = [local.state_objects]
  }

  statement {
    sid       = "ManageBuckets"
    actions   = ["s3:*"]
    resources = concat(local.buckets, local.objects)
  }

  statement {
    sid       = "ManageLambda"
    actions   = ["lambda:*"]
    resources = ["${format(local.arn_prefix, "lambda")}:function:spotgrid-*"]
  }

  statement {
    sid       = "ManageLogs"
    actions   = ["logs:*"]
    resources = local.log_groups
  }

  statement {
    sid = "ManageSchedules"
    actions = [
      "scheduler:CreateSchedule", "scheduler:UpdateSchedule", "scheduler:DeleteSchedule",
      "scheduler:CreateScheduleGroup", "scheduler:DeleteScheduleGroup",
      "scheduler:TagResource", "scheduler:UntagResource",
    ]
    resources = local.schedules
  }

  # Push images; manage the pipeline's repository, cluster and secrets.
  statement {
    sid = "ManagePipeline"
    actions = [
      "ecr:*", "ecs:CreateCluster", "ecs:DeleteCluster", "ecs:UpdateCluster", "ecs:TagResource",
      "ecs:UntagResource", "ssm:PutParameter", "ssm:DeleteParameter", "ssm:AddTagsToResource",
      "ssm:RemoveTagsFromResource",
    ]
    resources = [local.ecr_repos, local.cluster, local.ssm_params]
  }

  # Registering a task definition has no resource-level permissions.
  statement {
    sid       = "ManageTaskDefinitions"
    actions   = ["ecs:RegisterTaskDefinition", "ecs:DeregisterTaskDefinition"]
    resources = ["*"]
  }

  # The pipeline's VPC: subnets, gateways, routes, the S3 endpoint and the security group.
  statement {
    sid = "ManageNetwork"
    actions = [
      "ec2:CreateVpc", "ec2:DeleteVpc", "ec2:ModifyVpcAttribute", "ec2:CreateSubnet",
      "ec2:DeleteSubnet", "ec2:ModifySubnetAttribute", "ec2:CreateInternetGateway",
      "ec2:DeleteInternetGateway", "ec2:AttachInternetGateway", "ec2:DetachInternetGateway",
      "ec2:CreateRouteTable", "ec2:DeleteRouteTable", "ec2:CreateRoute", "ec2:DeleteRoute",
      "ec2:AssociateRouteTable", "ec2:DisassociateRouteTable", "ec2:CreateVpcEndpoint",
      "ec2:DeleteVpcEndpoints", "ec2:ModifyVpcEndpoint", "ec2:CreateSecurityGroup",
      "ec2:DeleteSecurityGroup", "ec2:AuthorizeSecurityGroupEgress",
      "ec2:RevokeSecurityGroupEgress", "ec2:AuthorizeSecurityGroupIngress",
      "ec2:RevokeSecurityGroupIngress", "ec2:CreateTags", "ec2:DeleteTags",
    ]
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "aws:RequestedRegion"
      values   = [var.workload_region]
    }
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
    sid       = "ManageCertificates"
    actions   = ["acm:RequestCertificate", "acm:DeleteCertificate", "acm:AddTagsToCertificate", "acm:RemoveTagsFromCertificate"]
    resources = [local.certificates]
  }

  statement {
    sid       = "ManageHttpApis"
    actions   = ["apigateway:GET", "apigateway:POST", "apigateway:PUT", "apigateway:PATCH", "apigateway:DELETE", "apigateway:TagResource", "apigateway:UntagResource", "apigateway:AddCertificateToDomain", "apigateway:RemoveCertificateFromDomain"]
    resources = local.http_apis
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
    sid       = "PassRoleToServices"
    actions   = ["iam:PassRole"]
    resources = [local.workload_role]

    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      values   = ["lambda.amazonaws.com", "scheduler.amazonaws.com", "ecs-tasks.amazonaws.com"]
    }
  }

  statement {
    sid       = "EcsServiceLinkedRole"
    actions   = ["iam:CreateServiceLinkedRole"]
    resources = ["arn:aws:iam::${local.account_id}:role/aws-service-role/ecs.amazonaws.com/*"]

    condition {
      test     = "StringEquals"
      variable = "iam:AWSServiceName"
      values   = ["ecs.amazonaws.com"]
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

# Ceiling for every role the apply role creates: the API and probe Lambdas, their schedule,
# and the pipeline task, its execution role and its schedule.
data "aws_iam_policy_document" "workload_boundary" {
  #checkov:skip=CKV_AWS_356:GetAuthorizationToken and PutMetricData have no resource-level permissions
  statement {
    sid       = "Data"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = concat(local.buckets, local.objects)
  }

  statement {
    sid       = "Logs"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = [for g in local.log_groups : "${g}:*"]
  }

  statement {
    sid       = "InvokeFunctions"
    actions   = ["lambda:InvokeFunction"]
    resources = ["${format(local.arn_prefix, "lambda")}:function:spotgrid-*"]
  }

  statement {
    sid       = "PullImage"
    actions   = ["ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer", "ecr:BatchCheckLayerAvailability"]
    resources = [local.ecr_repos]
  }

  statement {
    sid       = "Unscoped"
    actions   = ["ecr:GetAuthorizationToken", "cloudwatch:PutMetricData"]
    resources = ["*"]
  }

  statement {
    sid       = "Secrets"
    actions   = ["ssm:GetParameters"]
    resources = [local.ssm_params]
  }

  statement {
    sid       = "Alerts"
    actions   = ["sns:Publish"]
    resources = ["${format(local.arn_prefix, "sns")}:spotgrid-*"]
  }

  statement {
    sid       = "RunPipeline"
    actions   = ["ecs:RunTask", "ecs:TagResource"]
    resources = ["${format(local.arn_prefix, "ecs")}:task-definition/spotgrid-*", "${format(local.arn_prefix, "ecs")}:task/spotgrid/*"]
  }

  statement {
    sid       = "PassPipelineRoles"
    actions   = ["iam:PassRole"]
    resources = ["arn:aws:iam::${local.account_id}:role/spotgrid-pipeline-*"]

    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      values   = ["ecs-tasks.amazonaws.com"]
    }
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

resource "aws_iam_service_linked_role" "apigateway" {
  aws_service_name = "ops.apigateway.amazonaws.com"
}
