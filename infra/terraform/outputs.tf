output "bucket_name" {
  value = module.s3.bucket_name
}

output "glue_databases" {
  value = module.glue.database_names
}

output "athena_workgroup" {
  value = module.athena.workgroup_name
}

output "glue_job_role_arn" {
  value = module.iam.glue_job_role_arn
}

output "lambda_ingest_role_arn" {
  value = module.iam.lambda_ingest_role_arn
}

output "lambda_ingest_function" {
  value = module.lambda_ingest.function_name
}

output "glue_jobs" {
  value = module.glue_jobs.job_names
}

output "state_machine_arn" {
  value = module.stepfunctions.state_machine_arn
}

output "ecr_repositories" {
  value = module.ecr.repository_urls
}

output "api_url" {
  value = module.api.api_url
}

output "app_cluster" {
  value = module.ecs_app.cluster_name
}

output "app_service" {
  value = module.ecs_app.service_name
}
