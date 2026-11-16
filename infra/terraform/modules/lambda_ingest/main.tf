# Lambda de ingesta: descarga el ZIP mensual de BTS y deja el CSV en landing/.
# El código es un solo archivo (stdlib + boto3 del runtime), así que se empaqueta con
# archive_file sin capas ni imagen de contenedor.
variable "project" { type = string }
variable "bucket" { type = string }
variable "role_arn" { type = string }
variable "source_file" { type = string }
variable "bts_url_template" { type = string }
variable "log_retention_days" {
  type    = number
  default = 14
}

locals {
  function_name = "${var.project}-ingest"
}

data "archive_file" "code" {
  type        = "zip"
  source_file = var.source_file
  output_path = "${path.root}/.build/lambda_ingest.zip"
}

# Log group creado por Terraform para fijar la retención (si lo crea Lambda, es infinita)
# y para que `terraform destroy` lo borre.
resource "aws_cloudwatch_log_group" "this" {
  name              = "/aws/lambda/${local.function_name}"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "this" {
  function_name    = local.function_name
  description      = "Descarga un mes de BTS On-Time Performance a s3://${var.bucket}/landing/"
  role             = var.role_arn
  runtime          = "python3.11"
  architectures    = ["arm64"] # ~20 % más barato que x86 por GB-segundo
  handler          = "handler.lambda_handler"
  filename         = data.archive_file.code.output_path
  source_code_hash = data.archive_file.code.output_base64sha256

  # BTS es lento (~2 min por mes medidos en local); 15 min es el máximo de Lambda.
  timeout     = 900
  memory_size = 1024 # más memoria = más CPU/red para descomprimir y subir en paralelo

  # Solo el ZIP (~32 MB) toca /tmp; el CSV (~270 MB) se sube en streaming.
  ephemeral_storage {
    size = 512
  }

  environment {
    variables = {
      FD_BUCKET           = var.bucket
      FD_BTS_URL_TEMPLATE = var.bts_url_template
    }
  }

  logging_config {
    log_format = "Text"
    log_group  = aws_cloudwatch_log_group.this.name
  }
}

output "function_name" { value = aws_lambda_function.this.function_name }
output "function_arn" { value = aws_lambda_function.this.arn }
