# The pipeline: one container image run as a Fargate task by EventBridge Scheduler. A run
# lands bronze, merges silver, builds gold with dbt and exports it (spotgrid_lakehouse.pipeline).

locals {
  pipeline_name = "spotgrid-pipeline"
  # UTC, as before. Morning runs pick up yesterday's actuals and today's forecasts; SMARD
  # publishes tomorrow's prices at no fixed time in the afternoon, so it is polled every 30
  # minutes, and the evening run reports whether they arrived.
  schedules = {
    morning  = { cron = "cron(17 5 * * ? *)", command = ["run", "--mode", "morning"] }
    poll     = { cron = "cron(17,47 11-16 * * ? *)", command = ["run", "--mode", "poll"] }
    evening  = { cron = "cron(17 17 * * ? *)", command = ["run", "--mode", "evening"] }
    snapshot = { cron = "cron(23 7 1 * ? *)", command = ["snapshot"] }
    contract = { cron = "cron(41 6 ? * MON *)", command = ["contract"] }
  }
  # Set by hand after the first apply: aws ssm put-parameter --overwrite --name ... --value ...
  secrets = {
    GITHUB_TOKEN       = "/spotgrid/github_token"
    TELEGRAM_BOT_TOKEN = "/spotgrid/telegram_bot_token"
    TELEGRAM_CHAT_ID   = "/spotgrid/telegram_chat_id"
  }
}

resource "aws_ecr_repository" "pipeline" {
  #checkov:skip=CKV_AWS_136:AES256; a customer-managed KMS key costs 1 USD a month
  name                 = local.pipeline_name
  image_tag_mutability = "IMMUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "pipeline" {
  repository = aws_ecr_repository.pipeline.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep the last five images"
      selection    = { tagStatus = "any", countType = "imageCountMoreThan", countNumber = 5 }
      action       = { type = "expire" }
    }]
  })
}

resource "aws_cloudwatch_log_group" "pipeline" {
  #checkov:skip=CKV_AWS_338:A month of run logs; each run's report also goes to Telegram and issues
  #checkov:skip=CKV_AWS_158:Run logs hold no secrets; a customer-managed KMS key costs 1 USD a month
  name              = "/spotgrid/pipeline"
  retention_in_days = 30
}

resource "aws_ssm_parameter" "secret" {
  #checkov:skip=CKV2_AWS_34:SecureString under the AWS-managed key; a customer-managed key costs 1 USD a month
  #checkov:skip=CKV_AWS_337:Same as above
  for_each = local.secrets
  name     = each.value
  type     = "SecureString"
  value    = "unset"

  lifecycle {
    ignore_changes = [value]
  }
}

resource "aws_ecs_cluster" "pipeline" {
  #checkov:skip=CKV_AWS_65:Container Insights costs more than the runs; logs and the heartbeat alarm cover them
  name = "spotgrid"

  setting {
    name  = "containerInsights"
    value = "disabled"
  }
}

data "aws_iam_policy_document" "ecs_tasks_trust" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }
}

# Starts the container: pulls the image, writes its logs, reads the secrets.
resource "aws_iam_role" "pipeline_execution" {
  name                 = "${local.pipeline_name}-execution"
  assume_role_policy   = data.aws_iam_policy_document.ecs_tasks_trust.json
  permissions_boundary = local.workload_boundary
}

data "aws_iam_policy_document" "pipeline_execution" {
  #checkov:skip=CKV_AWS_356:ecr:GetAuthorizationToken has no resource-level permissions
  statement {
    sid       = "EcrLogin"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }

  statement {
    sid       = "PullImage"
    actions   = ["ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer", "ecr:BatchCheckLayerAvailability"]
    resources = [aws_ecr_repository.pipeline.arn]
  }

  statement {
    sid       = "WriteLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.pipeline.arn}:*"]
  }

  statement {
    sid       = "ReadSecrets"
    actions   = ["ssm:GetParameters"]
    resources = [for p in aws_ssm_parameter.secret : p.arn]
  }
}

resource "aws_iam_role_policy" "pipeline_execution" {
  name   = "start-task"
  role   = aws_iam_role.pipeline_execution.id
  policy = data.aws_iam_policy_document.pipeline_execution.json
}

# What the running pipeline may do.
resource "aws_iam_role" "pipeline_task" {
  name                 = "${local.pipeline_name}-task"
  assume_role_policy   = data.aws_iam_policy_document.ecs_tasks_trust.json
  permissions_boundary = local.workload_boundary
}

data "aws_iam_policy_document" "pipeline_task" {
  #checkov:skip=CKV_AWS_356:cloudwatch:PutMetricData has no resource-level permissions; scoped by namespace
  statement {
    sid       = "Lake"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/*"]
  }

  statement {
    sid       = "PublishGold"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.serving.arn, "${aws_s3_bucket.serving.arn}/*"]
  }

  statement {
    sid       = "Heartbeat"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["spotgrid"]
    }
  }

  statement {
    sid       = "Alert"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alarms.arn]
  }
}

resource "aws_iam_role_policy" "pipeline_task" {
  name   = "run-pipeline"
  role   = aws_iam_role.pipeline_task.id
  policy = data.aws_iam_policy_document.pipeline_task.json
}

resource "aws_ecs_task_definition" "pipeline" {
  #checkov:skip=CKV_AWS_336:dbt writes its target directory and DuckDB its file and spill under /tmp
  family                   = local.pipeline_name
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  # 1 vCPU and 2 GB, the smallest valid size at 1 vCPU. The first run overrides memory.
  cpu                = "1024"
  memory             = "2048"
  execution_role_arn = aws_iam_role.pipeline_execution.arn
  task_role_arn      = aws_iam_role.pipeline_task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  container_definitions = jsonencode([{
    name      = "pipeline"
    image     = "${aws_ecr_repository.pipeline.repository_url}:${var.pipeline_image_tag}"
    essential = true
    environment = [
      { name = "LAKE", value = "s3://${aws_s3_bucket.lake.id}/warehouse" },
      { name = "BRONZE", value = "s3://${aws_s3_bucket.lake.id}/bronze" },
      { name = "SERVING", value = "s3://${aws_s3_bucket.serving.id}" },
      { name = "API_URL", value = "https://${var.api_domain}" },
      { name = "GITHUB_REPOSITORY", value = var.github_repository },
      { name = "METRIC_NAMESPACE", value = "spotgrid" },
      { name = "ALARM_TOPIC_ARN", value = aws_sns_topic.alarms.arn },
      { name = "LOG_GROUP", value = aws_cloudwatch_log_group.pipeline.name },
      { name = "DUCKDB_MEMORY_LIMIT", value = "1500MB" },
    ]
    secrets = [for name, path in local.secrets : { name = name, valueFrom = aws_ssm_parameter.secret[name].arn }]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.pipeline.name
        awslogs-region        = var.region
        awslogs-stream-prefix = "pipeline"
      }
    }
  }])
}

# Tags reach schedules through their group.
resource "aws_scheduler_schedule_group" "spotgrid" {
  name = "spotgrid"
}

resource "aws_iam_role" "pipeline_scheduler" {
  name                 = "${local.pipeline_name}-scheduler"
  assume_role_policy   = data.aws_iam_policy_document.scheduler_trust.json
  permissions_boundary = local.workload_boundary
}

data "aws_iam_policy_document" "pipeline_scheduler" {
  statement {
    sid       = "RunTask"
    actions   = ["ecs:RunTask", "ecs:TagResource"]
    resources = ["${aws_ecs_task_definition.pipeline.arn_without_revision}:*", "arn:aws:ecs:${var.region}:${local.account_id}:task/${aws_ecs_cluster.pipeline.name}/*"]
  }

  statement {
    sid       = "PassTaskRoles"
    actions   = ["iam:PassRole"]
    resources = [aws_iam_role.pipeline_execution.arn, aws_iam_role.pipeline_task.arn]

    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      values   = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role_policy" "pipeline_scheduler" {
  name   = "run-pipeline-task"
  role   = aws_iam_role.pipeline_scheduler.id
  policy = data.aws_iam_policy_document.pipeline_scheduler.json
}

resource "aws_scheduler_schedule" "pipeline" {
  #checkov:skip=CKV_AWS_297:The schedules hold commands, not secrets; a customer-managed KMS key costs 1 USD a month
  for_each            = local.schedules
  name                = "${local.pipeline_name}-${each.key}"
  group_name          = aws_scheduler_schedule_group.spotgrid.name
  schedule_expression = each.value.cron

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_ecs_cluster.pipeline.arn
    role_arn = aws_iam_role.pipeline_scheduler.arn
    input = jsonencode({
      containerOverrides = [{
        name        = "pipeline"
        command     = each.value.command
        environment = [{ name = "TRIGGER", value = "schedule" }]
      }]
    })

    ecs_parameters {
      task_definition_arn     = aws_ecs_task_definition.pipeline.arn_without_revision
      launch_type             = "FARGATE"
      enable_ecs_managed_tags = true
      propagate_tags          = "TASK_DEFINITION"

      network_configuration {
        subnets          = aws_subnet.public[*].id
        security_groups  = [aws_security_group.pipeline.id]
        assign_public_ip = true
      }
    }

    # A missed run is picked up by the next one; the heartbeat alarm watches for a day of none.
    retry_policy {
      maximum_retry_attempts = 0
    }
  }
}
