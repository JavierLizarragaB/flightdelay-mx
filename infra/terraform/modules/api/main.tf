# API del modelo: imagen Docker (FastAPI + Mangum) en Lambda detrás de API Gateway HTTP API.
# Sin costo fijo: Lambda y API Gateway cobran por request.
variable "project" { type = string }
variable "region" { type = string }
variable "account_id" { type = string }
variable "bucket" { type = string }
variable "repository_name" { type = string }
variable "repository_url" { type = string }
variable "image_tag" {
  type    = string
  default = "latest"
}
variable "throttle_rate" {
  description = "Requests por segundo sostenidos (protección de costos y abuso)."
  type        = number
  default     = 20
}
variable "throttle_burst" {
  type    = number
  default = 50
}
variable "log_retention_days" {
  type    = number
  default = 14
}

locals {
  name = "${var.project}-api"
}

# El digest fija la versión exacta: un `push` nuevo con el mismo tag cambia el digest y
# el siguiente `terraform apply` actualiza la Lambda.
data "aws_ecr_image" "api" {
  repository_name = var.repository_name
  image_tag       = var.image_tag
}

data "aws_iam_policy_document" "trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "api" {
  name               = local.name
  assume_role_policy = data.aws_iam_policy_document.trust.json
}

# Solo lectura de los artefactos del modelo y escritura de sus propios logs.
data "aws_iam_policy_document" "api" {
  statement {
    sid       = "ReadModels"
    actions   = ["s3:GetObject"]
    resources = ["arn:aws:s3:::${var.bucket}/models/*"]
  }
  # ListBucket (limitado a models/) para que un artefacto inexistente dé NoSuchKey y no
  # AccessDenied: el mensaje de /health es entonces "no hay modelo publicado".
  statement {
    sid       = "ListModels"
    actions   = ["s3:ListBucket"]
    resources = ["arn:aws:s3:::${var.bucket}"]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["models/*"]
    }
  }
  statement {
    sid       = "OwnLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.api.arn}:*"]
  }
}

resource "aws_iam_role_policy" "api" {
  name   = "read-models"
  role   = aws_iam_role.api.id
  policy = data.aws_iam_policy_document.api.json
}

resource "aws_cloudwatch_log_group" "api" {
  name              = "/aws/lambda/${local.name}"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "api" {
  function_name = local.name
  description   = "FlightDelay-MX: FastAPI + LightGBM (imagen ${var.image_tag})"
  role          = aws_iam_role.api.arn
  package_type  = "Image"
  image_uri     = "${var.repository_url}@${data.aws_ecr_image.api.image_digest}"
  architectures = ["arm64"]
  memory_size   = 1024 # carga del modelo y lookups en el arranque en frío
  timeout       = 29   # API Gateway corta a los 30 s

  environment {
    variables = {
      MODEL_BUCKET = var.bucket
    }
  }

  logging_config {
    log_format = "Text"
    log_group  = aws_cloudwatch_log_group.api.name
  }

  depends_on = [aws_iam_role_policy.api]
}

resource "aws_apigatewayv2_api" "http" {
  name          = local.name
  protocol_type = "HTTP"
  description   = "FlightDelay-MX REST API (Swagger en /docs)"
}

resource "aws_apigatewayv2_integration" "lambda" {
  api_id                 = aws_apigatewayv2_api.http.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.api.invoke_arn
  payload_format_version = "2.0"
  timeout_milliseconds   = 29000
}

# Una sola ruta comodín: FastAPI resuelve /health, /predict, /docs, etc.
resource "aws_apigatewayv2_route" "proxy" {
  api_id    = aws_apigatewayv2_api.http.id
  route_key = "ANY /{proxy+}"
  target    = "integrations/${aws_apigatewayv2_integration.lambda.id}"
}

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.http.id
  name        = "$default"
  auto_deploy = true

  default_route_settings {
    throttling_rate_limit  = var.throttle_rate
    throttling_burst_limit = var.throttle_burst
  }
}

resource "aws_lambda_permission" "apigw" {
  statement_id  = "AllowApiGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.api.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.http.execution_arn}/*/*"
}

output "api_url" { value = aws_apigatewayv2_api.http.api_endpoint }
output "function_name" { value = aws_lambda_function.api.function_name }
