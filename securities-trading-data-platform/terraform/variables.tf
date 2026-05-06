variable "aws_region" {
  description = "AWS region for S3 buckets and infrastructure"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project name used for resource naming"
  type        = string
  default     = "securities-trading-data-platform"
}

variable "databricks_host" {
  description = "Databricks workspace URL"
  type        = string
  sensitive   = true
}

variable "databricks_token" {
  description = "Databricks personal access token"
  type        = string
  sensitive   = true
}

variable "unity_catalog_name" {
  description = "Name of the Unity Catalog to use"
  type        = string
  default     = "trading_platform"
}

variable "enable_versioning" {
  description = "Enable S3 bucket versioning"
  type        = bool
  default     = true
}

variable "lifecycle_glacier_days" {
  description = "Days before transitioning bronze data to Glacier"
  type        = number
  default     = 365
}

variable "enable_kms_encryption" {
  description = "Enable KMS encryption for S3 buckets"
  type        = bool
  default     = true
}
