terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
    access_key = var.AWS_ACCESS_KEY_ID
    secret_key = var.AWS_SECRET_ACCESS_KEY
    region = var.aws_region
}

resource "aws_s3_bucket" "demo-bucket" {
  bucket = var.resource_s3_bucket_name

  tags = {
    Name        = "My bucket"
    Environment = "Dev"
  }
}

resource "aws_redshift_cluster" "example" {
  cluster_identifier = var.resource_redshift_cluster_id
  database_name      = "demodb"
  master_username    = "root_user"
  master_password    = "RootPass1!"
  node_type          = "dc2.large"
  cluster_type       = "single-node"
}

