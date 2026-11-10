# Two private buckets. serving: the gold export (Parquet, bulk CSV, manifest) the API reads,
# the Lambda zip and probe results; everything in it can be rebuilt. lake: bronze (SMARD's
# chunks exactly as served, never rewritten), the warehouse file and the run lock.

resource "aws_s3_bucket" "serving" {
  #checkov:skip=CKV_AWS_145:SSE-S3; a customer-managed KMS key costs 1 USD a month
  #checkov:skip=CKV_AWS_144:Data is rebuilt from the lake, so replication only adds cost
  #checkov:skip=CKV_AWS_18:Private and written only by the pipeline and CI; a log bucket costs more than it protects
  #checkov:skip=CKV2_AWS_62:No consumer for object events; the API reads on request
  bucket = "spotgrid-serving-${local.suffix}"
}

resource "aws_s3_bucket" "lake" {
  #checkov:skip=CKV_AWS_145:SSE-S3; a customer-managed KMS key costs 1 USD a month
  #checkov:skip=CKV_AWS_144:Bronze can be fetched again from SMARD; replication doubles storage cost
  #checkov:skip=CKV_AWS_18:Private and written only by the pipeline; a log bucket costs more than it protects
  #checkov:skip=CKV2_AWS_62:No consumer for object events; the pipeline runs on a schedule
  bucket = "spotgrid-lake-${local.suffix}"

  lifecycle {
    prevent_destroy = true
  }
}

locals {
  buckets = {
    serving = aws_s3_bucket.serving
    lake    = aws_s3_bucket.lake
  }
}

resource "aws_s3_bucket_ownership_controls" "this" {
  for_each = local.buckets
  bucket   = each.value.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each = local.buckets
  bucket   = each.value.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = local.buckets
  bucket   = each.value.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# The export and the warehouse file are overwritten on every run; versioning keeps a week to
# roll a bad one back.
resource "aws_s3_bucket_versioning" "this" {
  for_each = local.buckets
  bucket   = each.value.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "serving" {
  bucket = aws_s3_bucket.serving.id

  rule {
    id     = "keep-a-week-of-old-exports"
    status = "Enabled"
    filter {}

    noncurrent_version_expiration {
      noncurrent_days = 7
    }

    expiration {
      expired_object_delete_marker = true
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }

  # Lambda keeps its own copy of the code; the apply workflow uploads the zip again if needed.
  rule {
    id     = "expire-lambda-zips"
    status = "Enabled"

    filter {
      prefix = "lambda/"
    }

    expiration {
      days = 30
    }
  }

  # Three months of latency probes, two small files an hour.
  rule {
    id     = "expire-probe-results"
    status = "Enabled"

    filter {
      prefix = "probes/"
    }

    expiration {
      days = 90
    }
  }

  depends_on = [aws_s3_bucket_versioning.this]
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  # Each run uploads the warehouse file again; old copies go after a week.
  rule {
    id     = "keep-a-week-of-old-versions"
    status = "Enabled"
    filter {}

    noncurrent_version_expiration {
      noncurrent_days = 7
    }

    expiration {
      expired_object_delete_marker = true
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }

  depends_on = [aws_s3_bucket_versioning.this]
}

data "aws_iam_policy_document" "deny_insecure" {
  for_each = local.buckets

  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [each.value.arn, "${each.value.arn}/*"]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "this" {
  for_each = local.buckets
  bucket   = each.value.id
  policy   = data.aws_iam_policy_document.deny_insecure[each.key].json

  depends_on = [aws_s3_bucket_public_access_block.this]
}
