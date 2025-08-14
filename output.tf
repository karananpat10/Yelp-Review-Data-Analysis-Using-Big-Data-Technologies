
output "glue_job_name" {    # Output for Glue job name
  value = aws_glue_job.etl_job.name         # returns the name of the Glue job
}

output "glue_crawler_name" {          # Output for Glue crawler name
  value = aws_glue_crawler.etl_crawler.name     # returns the name of the Glue crawler

}
