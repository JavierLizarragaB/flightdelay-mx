# Workgroup propio: resultados en nuestro bucket, motor v3 (soporta Iceberg) y tope de
# bytes escaneados por consulta para que un SELECT * accidental no cueste dinero.
variable "project" { type = string }
variable "bucket" { type = string }
variable "bytes_scanned_cutoff" { type = number }
variable "force_destroy" { type = bool }

resource "aws_athena_workgroup" "this" {
  name          = var.project
  description   = "Consultas del lakehouse FlightDelay-MX"
  force_destroy = var.force_destroy

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.bytes_scanned_cutoff

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "s3://${var.bucket}/athena-results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

output "workgroup_name" { value = aws_athena_workgroup.this.name }
