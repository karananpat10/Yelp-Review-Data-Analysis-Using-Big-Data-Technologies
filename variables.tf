# Declare the AWS region
variable "region" {
  default = "us-east-1"
}

# Declare the bucket name
variable "bucket_name_prefix" {
  default = "data20031203"
}

# Declare the Glue job name
variable "glue_job_name" {
  default = "yelpsdataglue12"
}

# Declare the Glue crawler name
variable "glue_crawler_name" {
  default = "yelpdatacrawler2023"
}

# Declare the script path in S3
variable "script_s3_path" {
  default = "s3://data20031203/scripts/glue_job.py"
}

