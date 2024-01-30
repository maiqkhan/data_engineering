variable "aws_region" {
  description = "Region in which resources will be deployed"
  default = "ca-central-1"
}

variable "AWS_ACCESS_KEY_ID" {
  description = "AWS root access key"
}

variable "AWS_SECRET_ACCESS_KEY" {
    description = "AWS root secret key"  
}

variable "resource_s3_bucket_name" {
  description = "Name of S3 Bucket"
  default = "demo-bucket-mk2431-toronto"
}

variable "resource_redshift_cluster_id" {
    description = "Name of Redshift Cluster Identifier"
    default = "demo-redshift-mk2431-toronto"
}