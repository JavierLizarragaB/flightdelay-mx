# Orquestación: Step Functions (ingesta -> bronce -> plata -> oro) + EventBridge Scheduler mensual.
# La definición ASL vive en orchestration/pipeline.asl.json (plantilla con los ARNs).
variable "project" { type = string }
variable "region" { type = string }
variable "account_id" { type = string }
variable "asl_template" { type = string }
variable "ingest_lambda_arn" { type = string }
variable "glue_job_names" { type = map(string) }
variable "schedule_expression" {
  description = "Cuándo corre el pipeline mensual. BTS publica con ~2-3 meses de retraso y sin fecha fija; el día 15 da margen."
  type        = string
  default     = "cron(0 6 15 * ? *)"
}
variable "schedule_timezone" {
  type    = string
  default = "America/Mexico_City"
}
variable "schedule_enabled" {
  type    = bool
  default = true
}
variable "log_retention_days" {
  type    = number
  default = 14
}

locals {
  name      = "${var.project}-pipeline"
  glue_arns = [for j in values(var.glue_job_names) : "arn:aws:glue:${var.region}:${var.account_id}:job/${j}"]
}

# ---------------------------------------------------------------------------------------
# Rol de la state machine: invocar SOLO la Lambda de ingesta y correr SOLO nuestros jobs.
# ---------------------------------------------------------------------------------------
data "aws_iam_policy_document" "sfn_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [var.account_id]
    }
  }
}

resource "aws_iam_role" "sfn" {
  name               = "${local.name}-sfn"
  assume_role_policy = data.aws_iam_policy_document.sfn_trust.json
}

data "aws_iam_policy_document" "sfn" {
  statement {
    sid       = "InvokeIngest"
    actions   = ["lambda:InvokeFunction"]
    resources = [var.ingest_lambda_arn, "${var.ingest_lambda_arn}:*"]
  }

  statement {
    sid       = "RunGlueJobsSync"
    actions   = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"]
    resources = local.glue_arns
  }

  # La entrega de logs de Step Functions a CloudWatch exige estas acciones sobre "*"
  # (no admiten ARN de recurso; ver la guía de Step Functions "Logging using CloudWatch").
  statement {
    sid = "LogDelivery"
    actions = [
      "logs:CreateLogDelivery", "logs:GetLogDelivery", "logs:UpdateLogDelivery",
      "logs:DeleteLogDelivery", "logs:ListLogDeliveries", "logs:PutResourcePolicy",
      "logs:DescribeResourcePolicies", "logs:DescribeLogGroups",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "sfn" {
  name   = "pipeline"
  role   = aws_iam_role.sfn.id
  policy = data.aws_iam_policy_document.sfn.json
}

resource "aws_cloudwatch_log_group" "sfn" {
  name              = "/aws/vendedlogs/states/${local.name}"
  retention_in_days = var.log_retention_days
}

resource "aws_sfn_state_machine" "pipeline" {
  name     = local.name
  role_arn = aws_iam_role.sfn.arn
  type     = "STANDARD" # se cobra por transición (~25 por corrida): centavos al año

  definition = templatefile(var.asl_template, {
    ingest_lambda_arn = var.ingest_lambda_arn
    bronze_job        = var.glue_job_names["bronze"]
    silver_job        = var.glue_job_names["silver"]
    gold_job          = var.glue_job_names["gold"]
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }

  depends_on = [aws_iam_role_policy.sfn]
}

# ---------------------------------------------------------------------------------------
# EventBridge Scheduler: dispara la corrida mensual con entrada {} (último mes publicado).
# ---------------------------------------------------------------------------------------
data "aws_iam_policy_document" "scheduler_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [var.account_id]
    }
  }
}

resource "aws_iam_role" "scheduler" {
  name               = "${local.name}-scheduler"
  assume_role_policy = data.aws_iam_policy_document.scheduler_trust.json
}

data "aws_iam_policy_document" "scheduler" {
  statement {
    actions   = ["states:StartExecution"]
    resources = [aws_sfn_state_machine.pipeline.arn]
  }
}

resource "aws_iam_role_policy" "scheduler" {
  name   = "start-pipeline"
  role   = aws_iam_role.scheduler.id
  policy = data.aws_iam_policy_document.scheduler.json
}

resource "aws_scheduler_schedule" "monthly" {
  name                         = "${local.name}-monthly"
  description                  = "Corrida mensual: ingiere el último mes publicado por BTS y reprocesa bronce y plata."
  schedule_expression          = var.schedule_expression
  schedule_expression_timezone = var.schedule_timezone
  state                        = var.schedule_enabled ? "ENABLED" : "DISABLED"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_sfn_state_machine.pipeline.arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({})

    retry_policy {
      maximum_retry_attempts = 2
    }
  }
}

output "state_machine_arn" { value = aws_sfn_state_machine.pipeline.arn }
output "schedule_name" { value = aws_scheduler_schedule.monthly.name }
