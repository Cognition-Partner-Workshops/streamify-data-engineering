# ─── Unity Catalog ────────────────────────────────────────────────────────────

resource "databricks_catalog" "trading" {
  name    = var.unity_catalog_name
  comment = "Unity Catalog for securities trading data platform"
}

# ─── Schemas (one per medallion tier) ─────────────────────────────────────────

resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.trading.name
  name         = "bronze"
  comment      = "Raw ingested data from S3 — immutable landing zone"
}

resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.trading.name
  name         = "silver"
  comment      = "Cleansed, validated, and enriched data"
}

resource "databricks_schema" "gold" {
  catalog_name = databricks_catalog.trading.name
  name         = "gold"
  comment      = "Business-level aggregations and analytics-ready tables"
}

# ─── External Location for S3 ────────────────────────────────────────────────

resource "databricks_storage_credential" "s3_credential" {
  name = "${var.project_name}-s3-credential"

  aws_iam_role {
    role_arn = aws_iam_role.databricks_s3_role.arn
  }

  comment = "Credential for accessing trading data lake S3 bucket"
}

resource "aws_iam_role" "databricks_s3_role" {
  name = "${var.project_name}-databricks-s3-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::root" # Replace with Databricks account ID
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = var.unity_catalog_name
          }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "databricks_s3" {
  role       = aws_iam_role.databricks_s3_role.name
  policy_arn = aws_iam_policy.databricks_s3_access.arn
}

resource "databricks_external_location" "data_lake" {
  name            = "${var.project_name}-data-lake"
  url             = "s3://${aws_s3_bucket.data_lake.id}/"
  credential_name = databricks_storage_credential.s3_credential.name
  comment         = "External location for trading data lake S3 bucket"

  depends_on = [databricks_storage_credential.s3_credential]
}

# ─── Compute: All-Purpose Cluster for Development ────────────────────────────

resource "databricks_cluster" "dev" {
  cluster_name            = "${var.project_name}-dev"
  spark_version           = "14.3.x-scala2.12"
  node_type_id            = "i3.xlarge"
  autotermination_minutes = 30
  data_security_mode      = "SINGLE_USER"

  autoscale {
    min_workers = 1
    max_workers = 4
  }

  spark_conf = {
    "spark.databricks.delta.optimizeWrite.enabled" = "true"
    "spark.databricks.delta.autoCompact.enabled"   = "true"
    "spark.sql.adaptive.enabled"                   = "true"
  }

  custom_tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

# ─── SQL Warehouse for Gold Layer Analytics ──────────────────────────────────

resource "databricks_sql_endpoint" "analytics" {
  name             = "${var.project_name}-analytics"
  cluster_size     = "Small"
  max_num_clusters = 2
  auto_stop_mins   = 15

  tags {
    custom_tags {
      key   = "Project"
      value = var.project_name
    }
  }
}
