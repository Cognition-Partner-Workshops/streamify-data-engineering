output "s3_bucket_name" {
  description = "Name of the data lake S3 bucket"
  value       = aws_s3_bucket.data_lake.id
}

output "s3_bucket_arn" {
  description = "ARN of the data lake S3 bucket"
  value       = aws_s3_bucket.data_lake.arn
}

output "s3_bronze_path" {
  description = "S3 path for bronze (raw) data"
  value       = "s3://${aws_s3_bucket.data_lake.id}/bronze/"
}

output "s3_silver_path" {
  description = "S3 path for silver (cleansed) data"
  value       = "s3://${aws_s3_bucket.data_lake.id}/silver/"
}

output "s3_gold_path" {
  description = "S3 path for gold (aggregated) data"
  value       = "s3://${aws_s3_bucket.data_lake.id}/gold/"
}

output "kms_key_arn" {
  description = "ARN of the KMS key for S3 encryption"
  value       = var.enable_kms_encryption ? aws_kms_key.data_platform[0].arn : null
}

output "databricks_catalog" {
  description = "Unity Catalog name"
  value       = databricks_catalog.trading.name
}

output "databricks_cluster_id" {
  description = "ID of the development cluster"
  value       = databricks_cluster.dev.id
}

output "databricks_sql_endpoint_id" {
  description = "ID of the SQL analytics warehouse"
  value       = databricks_sql_endpoint.analytics.id
}

output "databricks_iam_role_arn" {
  description = "ARN of the IAM role for Databricks S3 access"
  value       = aws_iam_role.databricks_s3_role.arn
}
