
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

=======
# Declare the AWS region
variable "region" {           # AWS region for the resources
  default = "us-east-1"       # default region
}

# Declare the bucket name
variable "bucket_name_prefix" {           # S3 bucket name prefix
  default = "finalyelp201231011"          # default bucket name prefix
}

# Declare the Glue job name
variable "glue_job_name" {      # Glue job name
  default = "yelpdata1"         # default Glue job name
}

# Declare the Glue crawler name
variable "glue_crawler_name" {      # Glue crawler name
  default = "yelpcrawlers12"      # default Glue crawler name
}

# Declare the script path in S3
variable "script_s3_path" {     # S3 path for the ETL script
  default = "s3://finalyelp201231011/scripts/etl.py"    # default script path
}

