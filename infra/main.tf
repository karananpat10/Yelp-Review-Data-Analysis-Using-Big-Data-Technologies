provider "aws" {
  region = var.aws_region
}

resource "aws_s3_bucket" "output_bucket" {
  bucket        = var.output_bucket_name
  force_destroy = true
}

data "aws_caller_identity" "current" {}

resource "aws_glue_job" "etl_job" {
  name     = "yelp-glue-etl"
  role_arn = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/LabRole"

  command {
    name            = "glueetl"
    script_location = "s3://firstterraformjob/script.py"
    python_version  = "3"
  }

  default_arguments = {
    "--TempDir"       = "s3://${aws_s3_bucket.output_bucket.bucket}/temp/"
    "--job-language"  = "python"
    "--input_bucket"  = "firstterraformjob"
    "--output_bucket" = aws_s3_bucket.output_bucket.bucket
  }

  glue_version       = "4.0"
  number_of_workers  = 2
  worker_type        = "G.1X"
  max_retries        = 1
}
