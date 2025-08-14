resource "aws_s3_bucket" "etl_bucket" {   # S3 bucket for ETL job
  bucket = var.bucket_name_prefix   # bucket name prefix
}

resource "aws_glue_catalog_database" "etl_db" {   # Glue database for ETL job
  name = "master"   # database name
}

locals {      # local variables for Glue job
  glue_role_arn = "arn:aws:iam::108394617907:role/LabRole"      # IAM role for Glue job
}

resource "aws_glue_job" "etl_job" {     # Glue job for ETL processing
  name     = var.glue_job_name      # job name
  role_arn = local.glue_role_arn      # IAM role for the job

  command {     # command to run the Glue job
    name            = "glueetl"        # ETL job type
    script_location = "s3://myscriptdatakaran/scripts/etl.py"     # script location in S3
    python_version  = "3"     # Python version for the job
  }

  glue_version      = "4.0"     # Glue version
  number_of_workers = 2    # number of workers for the job  
  worker_type       = "G.1X"    # worker type for the job
}

resource "aws_glue_crawler" "etl_crawler" {      #crawler to crawl the output of the Glue job
  name          = var.glue_crawler_name    # crawler name
  role          = local.glue_role_arn       # IAM role for the crawler
  database_name = aws_glue_catalog_database.etl_db.name   # database to store the output

  s3_target {   # S3 target for the crawler
    path = "s3://glue-file-1/final-output/"     #here we are pointing to the output of the Glue job
 }

  depends_on = [aws_glue_job.etl_job]   # ensures the crawler runs after the Glue job
}
