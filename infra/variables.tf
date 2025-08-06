variable "aws_region" {
  description = "The AWS region to deploy resources in"
  type        = string
  default     = "us-east-1"  # You can change this to your preferred region
}

variable "output_bucket_name" {
  description = "S3 bucket name for storing cleaned/transformed output data"
  type        = string
  default     = "terraformoutputjob"
}
