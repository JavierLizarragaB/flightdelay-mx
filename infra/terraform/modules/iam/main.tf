# Roles de servicio con mínimo privilegio. El usuario humano (flightdelay-dev) tiene
# AdministratorAccess solo para que Terraform cree recursos; lo que corre el pipeline
# usa estos roles, limitados a nuestro bucket, nuestras bases y sus propios logs.
variable "project" { type = string }
variable "region" { type = string }
variable "account_id" { type = string }
variable "bucket_arn" { type = string }
variable "database_names" { type = list(string) }

locals {
  catalog_arn = "arn:aws:glue:${var.region}:${var.account_id}:catalog"
  db_arns     = [for d in var.database_names : "arn:aws:glue:${var.region}:${var.account_id}:database/${d}"]
  table_arns  = [for d in var.database_names : "arn:aws:glue:${var.region}:${var.account_id}:table/${d}/*"]
}

# ---------------------------------------------------------------------------------------
# Glue jobs (bronce, plata, oro)
# ---------------------------------------------------------------------------------------
data "aws_iam_policy_document" "glue_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
    # Protección contra "confused deputy": solo Glue actuando para esta cuenta.
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [var.account_id]
    }
  }
}

resource "aws_iam_role" "glue_job" {
  name               = "${var.project}-glue-job"
  assume_role_policy = data.aws_iam_policy_document.glue_trust.json
}

data "aws_iam_policy_document" "glue_job" {
  statement {
    sid       = "ListLakeBucket"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [var.bucket_arn]
  }

  statement {
    sid     = "ReadOnlyInputs"
    actions = ["s3:GetObject"]
    resources = [
      "${var.bucket_arn}/landing/*",
      "${var.bucket_arn}/glue-scripts/*",
    ]
  }

  statement {
    sid     = "ReadWriteLayers"
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = [
      "${var.bucket_arn}/bronze/*",
      "${var.bucket_arn}/silver/*",
      "${var.bucket_arn}/gold/*",
      "${var.bucket_arn}/reports/*",
      "${var.bucket_arn}/glue-tmp/*",
    ]
  }

  statement {
    sid = "GlueCatalogForIceberg"
    actions = [
      "glue:GetCatalog", "glue:GetDatabase", "glue:GetDatabases",
      "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
      "glue:GetPartition", "glue:GetPartitions", "glue:BatchCreatePartition",
      "glue:BatchDeletePartition", "glue:BatchGetPartition", "glue:UpdatePartition",
    ]
    resources = concat([local.catalog_arn], local.db_arns, local.table_arns)
  }

  statement {
    sid       = "JobLogs"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:${var.region}:${var.account_id}:log-group:/aws-glue/*"]
  }

  statement {
    sid       = "JobMetrics"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"] # PutMetricData no admite ARN de recurso; se limita por namespace.
    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["Glue"]
    }
  }
}

resource "aws_iam_role_policy" "glue_job" {
  name   = "lakehouse-access"
  role   = aws_iam_role.glue_job.id
  policy = data.aws_iam_policy_document.glue_job.json
}

# ---------------------------------------------------------------------------------------
# Lambda de ingesta: solo escribe en landing/ y en su propio log group
# ---------------------------------------------------------------------------------------
data "aws_iam_policy_document" "lambda_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda_ingest" {
  name               = "${var.project}-lambda-ingest"
  assume_role_policy = data.aws_iam_policy_document.lambda_trust.json
}

data "aws_iam_policy_document" "lambda_ingest" {
  statement {
    sid       = "WriteLanding"
    actions   = ["s3:PutObject", "s3:AbortMultipartUpload"]
    resources = ["${var.bucket_arn}/landing/*"]
  }

  statement {
    sid       = "CheckExistingMonth"
    actions   = ["s3:ListBucket"]
    resources = [var.bucket_arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["landing/*"]
    }
  }

  statement {
    sid       = "OwnLogs"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:${var.region}:${var.account_id}:log-group:/aws/lambda/${var.project}-*"]
  }
}

resource "aws_iam_role_policy" "lambda_ingest" {
  name   = "landing-write"
  role   = aws_iam_role.lambda_ingest.id
  policy = data.aws_iam_policy_document.lambda_ingest.json
}

output "glue_job_role_arn" { value = aws_iam_role.glue_job.arn }
output "lambda_ingest_role_arn" { value = aws_iam_role.lambda_ingest.arn }
