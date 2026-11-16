# Bases del Glue Data Catalog (una por capa). Las tablas Iceberg las crean los Glue jobs.
variable "bucket" { type = string }
variable "databases" {
  description = "Mapa capa -> nombre de base (bronze -> fd_bronze)."
  type        = map(string)
}

resource "aws_glue_catalog_database" "layer" {
  for_each     = var.databases
  name         = each.value
  description  = "Capa ${each.key} del lakehouse FlightDelay-MX (Apache Iceberg)"
  location_uri = "s3://${var.bucket}/${each.key}/"
}

output "database_names" {
  value = [for db in aws_glue_catalog_database.layer : db.name]
}
