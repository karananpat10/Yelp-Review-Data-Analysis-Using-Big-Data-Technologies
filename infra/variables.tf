#declare a region
variable "region" {
  default = "us-east-1"
}

#declare a bucket name
variable "bucket_name_prefix" {
  default = "cdac-automation-bucket627"
}


#declare a glue job name
variable "glue_job_name" {
  default = "cdac-glue-etl-job"
}

#declare a crawler name
variable "glue_crawler_name" {
  default = "cdac-etl-crawler1"
}

#declare a script path
variable "script_s3_path" {
  default = "s3://cdac-automation-bucket627/scripts/cdac-etl-script.py"
}