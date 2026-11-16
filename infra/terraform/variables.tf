variable "project" {
  description = "Nombre corto del proyecto; prefijo de todos los recursos."
  type        = string
  default     = "flightdelay-mx"
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "aws_profile" {
  description = "Perfil de AWS CLI. Las credenciales nunca viven en el repo."
  type        = string
  default     = "flightdelay"
}

variable "bucket_name" {
  description = "Nombre del bucket del lakehouse. Si es null: <project>-<account_id> (único en todo AWS)."
  type        = string
  default     = null
}

variable "force_destroy" {
  description = "Permite que `terraform destroy` borre el bucket aunque tenga datos. true para un proyecto escolar."
  type        = bool
  default     = true
}

variable "athena_bytes_scanned_cutoff" {
  description = "Límite de bytes escaneados por consulta en el workgroup (protección de costos). 10 GB por defecto."
  type        = number
  default     = 10737418240
}

variable "bts_url_template" {
  description = "Plantilla de URL del ZIP mensual de BTS ({year}, {month} sin cero a la izquierda)."
  type        = string
  default     = "https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"
}
