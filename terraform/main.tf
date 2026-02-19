terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region                      = var.aws_region
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    s3 = var.localstack_endpoint
  }

  s3_use_path_style = true
}

resource "aws_s3_bucket" "bronze" {
  bucket = "bronze-layer"
}

resource "aws_s3_bucket" "silver" {
  bucket = "silver-layer"
}

resource "aws_s3_bucket" "gold" {
  bucket = "gold-layer"
}
