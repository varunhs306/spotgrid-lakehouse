# Daily gold export (Parquet + manifest) that the public API reads, plus the API's Lambda zip.
# Private: only CI, the export step and the API's role touch it. Everything here can be rebuilt.
resource "aws_s3_bucket" "serving" {
  #checkov:skip=CKV_AWS_145:SSE-S3; a customer-managed KMS key costs 1 USD a month
  #checkov:skip=CKV_AWS_144:Data is rebuilt from silver, so replication only adds cost
  #checkov:skip=CKV_AWS_18:Private and written only by CI; a log bucket costs more than it protects
  #checkov:skip=CKV2_AWS_62:No consumer for object events yet; the API reads on request
  bucket = "spotgrid-serving-${data.aws_caller_identity.current.account_id}"
}

resource "aws_s3_bucket_ownership_controls" "serving" {
  bucket = aws_s3_bucket.serving.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_public_access_block" "serving" {
  bucket = aws_s3_bucket.serving.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "serving" {
  bucket = aws_s3_bucket.serving.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Each export overwrites the previous one; versioning keeps a few days to roll a bad one back.
resource "aws_s3_bucket_versioning" "serving" {
  bucket = aws_s3_bucket.serving.id

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

  depends_on = [aws_s3_bucket_versioning.serving]
}

data "aws_iam_policy_document" "serving" {
  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]
    resources = [
      aws_s3_bucket.serving.arn,
      "${aws_s3_bucket.serving.arn}/*",
    ]

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

resource "aws_s3_bucket_policy" "serving" {
  bucket = aws_s3_bucket.serving.id
  policy = data.aws_iam_policy_document.serving.json

  depends_on = [aws_s3_bucket_public_access_block.serving]
}
