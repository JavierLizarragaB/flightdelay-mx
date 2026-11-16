data "aws_caller_identity" "current" {}

locals {
  account_id  = data.aws_caller_identity.current.account_id
  bucket_name = coalesce(var.bucket_name, "${var.project}-${local.account_id}")

  # Bases del Glue Data Catalog, una por capa Medallion.
  databases = {
    bronze = "fd_bronze"
    silver = "fd_silver"
    gold   = "fd_gold"
  }
}

module "s3" {
  source        = "./modules/s3"
  bucket_name   = local.bucket_name
  force_destroy = var.force_destroy
}

module "glue" {
  source    = "./modules/glue"
  bucket    = module.s3.bucket_name
  databases = local.databases
}

module "athena" {
  source               = "./modules/athena"
  project              = var.project
  bucket               = module.s3.bucket_name
  bytes_scanned_cutoff = var.athena_bytes_scanned_cutoff
  force_destroy        = var.force_destroy
}

module "iam" {
  source         = "./modules/iam"
  project        = var.project
  region         = var.aws_region
  account_id     = local.account_id
  bucket_arn     = module.s3.bucket_arn
  database_names = values(local.databases)
}

module "lambda_ingest" {
  source           = "./modules/lambda_ingest"
  project          = var.project
  bucket           = module.s3.bucket_name
  role_arn         = module.iam.lambda_ingest_role_arn
  source_file      = "${path.root}/../../src/ingestion/handler.py"
  bts_url_template = var.bts_url_template
}

module "glue_jobs" {
  source    = "./modules/glue_jobs"
  project   = var.project
  bucket    = module.s3.bucket_name
  role_arn  = module.iam.glue_job_role_arn
  src_dir   = "${path.root}/../../src"
  databases = local.databases
}

module "stepfunctions" {
  source            = "./modules/stepfunctions"
  project           = var.project
  region            = var.aws_region
  account_id        = local.account_id
  asl_template      = "${path.root}/../../orchestration/pipeline.asl.json"
  ingest_lambda_arn = module.lambda_ingest.function_arn
  glue_job_names    = module.glue_jobs.job_names
}

# --- Servicio del modelo y app ------------------------------------------------------
# Orden del primer despliegue (las imágenes deben existir antes que Lambda/ECS):
#   make ecr  ->  make api-image app-image  ->  make tf-plan tf-apply
module "ecr" {
  source  = "./modules/ecr"
  project = var.project
}

module "api" {
  source          = "./modules/api"
  project         = var.project
  region          = var.aws_region
  account_id      = local.account_id
  bucket          = module.s3.bucket_name
  repository_name = module.ecr.repository_names["api"]
  repository_url  = module.ecr.repository_urls["api"]
}

module "ecs_app" {
  source          = "./modules/ecs_app"
  project         = var.project
  region          = var.aws_region
  repository_name = module.ecr.repository_names["app"]
  repository_url  = module.ecr.repository_urls["app"]
  api_url         = module.api.api_url
}
