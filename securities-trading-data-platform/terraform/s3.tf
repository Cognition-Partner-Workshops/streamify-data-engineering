# ─── KMS Key for S3 Encryption ────────────────────────────────────────────────

resource "aws_kms_key" "data_platform" {
  count               = var.enable_kms_encryption ? 1 : 0
  description         = "KMS key for ${var.project_name} S3 encryption"
  enable_key_rotation = true

  tags = {
    Name = "${var.project_name}-kms"
  }
}

resource "aws_kms_alias" "data_platform" {
  count         = var.enable_kms_encryption ? 1 : 0
  name          = "alias/${var.project_name}"
  target_key_id = aws_kms_key.data_platform[0].key_id
}

# ─── Data Lake S3 Bucket ─────────────────────────────────────────────────────

resource "aws_s3_bucket" "data_lake" {
  bucket = "${var.project_name}-${var.environment}"

  tags = {
    Name = "${var.project_name}-data-lake"
    Tier = "all"
  }
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  versioning_configuration {
    status = var.enable_versioning ? "Enabled" : "Disabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = var.enable_kms_encryption ? "aws:kms" : "AES256"
      kms_master_key_id = var.enable_kms_encryption ? aws_kms_key.data_platform[0].arn : null
    }
    bucket_key_enabled = var.enable_kms_encryption
  }
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  # Bronze: move raw data to Intelligent-Tiering after 90 days, Glacier after configured days
  rule {
    id     = "bronze-lifecycle"
    status = "Enabled"

    filter {
      prefix = "bronze/"
    }

    transition {
      days          = 90
      storage_class = "INTELLIGENT_TIERING"
    }

    transition {
      days          = var.lifecycle_glacier_days
      storage_class = "GLACIER"
    }
  }

  # Silver: move to Intelligent-Tiering after 180 days
  rule {
    id     = "silver-lifecycle"
    status = "Enabled"

    filter {
      prefix = "silver/"
    }

    transition {
      days          = 180
      storage_class = "INTELLIGENT_TIERING"
    }
  }

  # Gold: keep in Standard (frequently accessed analytics)
  rule {
    id     = "gold-lifecycle"
    status = "Enabled"

    filter {
      prefix = "gold/"
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }

  # Checkpoints: DO NOT expire — required for Auto Loader incremental state.
  # Deleting checkpoint files causes streaming queries to lose track of
  # already-processed files, resulting in duplicate ingestion.
}

# ─── Folder Structure (empty objects as prefixes) ─────────────────────────────

locals {
  s3_prefixes = [
    "bronze/instruments/",
    "bronze/trades/",
    "bronze/orders/",
    "bronze/market_data/",
    "bronze/positions/",
    "silver/instruments/",
    "silver/trades/",
    "silver/orders/",
    "silver/market_data/",
    "silver/positions/",
    "gold/daily_pnl/",
    "gold/trading_volume/",
    "gold/portfolio_summary/",
    "gold/risk_metrics/",
    "gold/market_analytics/",
    "_checkpoints/",
  ]
}

resource "aws_s3_object" "prefixes" {
  for_each = toset(local.s3_prefixes)

  bucket  = aws_s3_bucket.data_lake.id
  key     = each.value
  content = ""
}

# ─── IAM Policy for Databricks Access ────────────────────────────────────────

resource "aws_iam_policy" "databricks_s3_access" {
  name        = "${var.project_name}-databricks-s3-access"
  description = "Allow Databricks to read/write the trading data lake"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat(
      [
        {
          Sid    = "ListBucket"
          Effect = "Allow"
          Action = [
            "s3:ListBucket",
            "s3:GetBucketLocation",
          ]
          Resource = aws_s3_bucket.data_lake.arn
        },
        {
          Sid    = "ReadWriteObjects"
          Effect = "Allow"
          Action = [
            "s3:GetObject",
            "s3:PutObject",
            "s3:DeleteObject",
            "s3:GetObjectVersion",
          ]
          Resource = "${aws_s3_bucket.data_lake.arn}/*"
        },
      ],
      var.enable_kms_encryption ? [
        {
          Sid    = "KMSAccess"
          Effect = "Allow"
          Action = [
            "kms:Decrypt",
            "kms:Encrypt",
            "kms:GenerateDataKey",
          ]
          Resource = aws_kms_key.data_platform[0].arn
        }
      ] : []
    )
  })
}
