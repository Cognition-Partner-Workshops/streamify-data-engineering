terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.35"
    }
  }

  backend "s3" {
    bucket         = "trading-platform-terraform-state"
    key            = "securities-data-platform/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "terraform-locks"
    encrypt        = true
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "securities-trading-data-platform"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}
