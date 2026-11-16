# Glue jobs de PySpark (Glue 5.0: Spark 3.5, Python 3.11, Iceberg nativo).
# El script principal de cada job solo orquesta; la lógica vive en el paquete `src/`, que
# se sube como ZIP y se agrega con --extra-py-files.
variable "project" { type = string }
variable "bucket" { type = string }
variable "role_arn" { type = string }
variable "src_dir" {
  description = "Ruta local a src/ (raíz del paquete Python)."
  type        = string
}
variable "databases" { type = map(string) }
variable "worker_type" {
  type    = string
  default = "G.1X" # 4 vCPU, 16 GB; el más chico para glueetl
}
variable "number_of_workers" {
  type    = number
  default = 2 # mínimo permitido; un mes de BTS (~276 MB) cabe de sobra
}
variable "log_retention_days" {
  type    = number
  default = 14
}

locals {
  py_files = fileset(var.src_dir, "**/*.py")

  # Un job por capa; el script es src/glue_jobs/<capa>_job.py.
  jobs = {
    bronze = {
      description = "landing/ CSV -> ${var.databases["bronze"]}.flights_raw (Iceberg). Valida conteos contra el manifiesto de ingesta."
      args        = { "--bronze_db" = var.databases["bronze"] }
    }
    silver = {
      description = "${var.databases["bronze"]} -> ${var.databases["silver"]}.flights / flights_disrupted (Iceberg) + reports/quality_<yyyy_mm>.json"
      args = {
        "--bronze_db" = var.databases["bronze"]
        "--silver_db" = var.databases["silver"]
      }
    }
    gold = {
      description = "${var.databases["silver"]}.flights -> ${var.databases["gold"]}.features_flights + lookups (recalculo completo)"
      args = {
        "--silver_db" = var.databases["silver"]
        "--gold_db"   = var.databases["gold"]
      }
    }
  }
  lib_key = "glue-scripts/lib/fd_src.zip"

  # Argumentos comunes a todos los jobs del lakehouse.
  common_args = {
    "--job-language"                     = "python"
    "--datalake-formats"                 = "iceberg"
    "--extra-py-files"                   = "s3://${var.bucket}/${local.lib_key}"
    "--TempDir"                          = "s3://${var.bucket}/glue-tmp/"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-job-insights"              = "false"
    "--FD_BUCKET"                        = var.bucket
  }
}

# Paquete src/ con prefijo "src/" para que `from src.glue_jobs import bronze` funcione igual
# en Glue que en los tests locales.
data "archive_file" "src" {
  type        = "zip"
  output_path = "${path.root}/.build/fd_src.zip"

  dynamic "source" {
    for_each = local.py_files
    content {
      content  = file("${var.src_dir}/${source.value}")
      filename = "src/${source.value}"
    }
  }
}

resource "aws_s3_object" "lib" {
  bucket = var.bucket
  key    = local.lib_key
  source = data.archive_file.src.output_path
  etag   = data.archive_file.src.output_md5
}

resource "aws_s3_object" "script" {
  for_each = local.jobs
  bucket   = var.bucket
  key      = "glue-scripts/${each.key}_job.py"
  source   = "${var.src_dir}/glue_jobs/${each.key}_job.py"
  etag     = filemd5("${var.src_dir}/glue_jobs/${each.key}_job.py")
}

moved {
  from = aws_s3_object.bronze_script
  to   = aws_s3_object.script["bronze"]
}

# Glue escribe en estos log groups compartidos; los creamos para fijar retención (si los
# crea Glue, guardan logs para siempre) y que `terraform destroy` los borre.
resource "aws_cloudwatch_log_group" "glue" {
  for_each          = toset(["/aws-glue/jobs/output", "/aws-glue/jobs/error", "/aws-glue/jobs/logs-v2"])
  name              = each.value
  retention_in_days = var.log_retention_days
}

resource "aws_glue_job" "this" {
  for_each    = local.jobs
  name        = "${var.project}-${each.key}"
  description = each.value.description
  role_arn    = var.role_arn

  glue_version      = "5.0"
  worker_type       = var.worker_type
  number_of_workers = var.number_of_workers
  timeout           = 60 # minutos; Step Functions lo ajusta por corrida
  max_retries       = 0  # un fallo de validación no se arregla reintentando
  execution_class   = "STANDARD"

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket}/${aws_s3_object.script[each.key].key}"
    python_version  = "3"
  }

  # Una corrida a la vez por job: evita commits concurrentes sobre la misma tabla Iceberg.
  execution_property {
    max_concurrent_runs = 1
  }

  # --year_months se pasa en cada corrida (Step Functions o `make <capa> YM=...`).
  default_arguments = merge(local.common_args, each.value.args)

  depends_on = [aws_s3_object.lib, aws_cloudwatch_log_group.glue]
}

moved {
  from = aws_glue_job.bronze
  to   = aws_glue_job.this["bronze"]
}

output "job_names" { value = { for k, j in aws_glue_job.this : k => j.name } }
