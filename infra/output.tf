# Output the name of the created AWS Glue Job
output "glue_job_name" {
  description = "The name of the AWS Glue job created"
  value       = aws_glue_job.etl_job.name
}

# Output the name of the output S3 bucket
output "output_bucket_name" {
  description = "The name of the S3 bucket where cleaned data is stored"
  value       = aws_s3_bucket.output_bucket.bucket
}
