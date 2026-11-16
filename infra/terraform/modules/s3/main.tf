# Bucket único del lakehouse. Cada capa vive en su propio prefijo.
variable "bucket_name" { type = string }
variable "force_destroy" { type = bool }

locals {
  # Prefijos de datos (ver src/common/config.py) + prefijos operativos.
  prefixes = [
    "landing/", "bronze/", "silver/", "gold/", "models/", "reports/",
    "glue-scripts/", "glue-tmp/", "athena-results/",
  ]
}

resource "aws_s3_bucket" "lake" {
  bucket        = var.bucket_name
  force_destroy = var.force_destroy
}

# Nadie fuera de la cuenta puede leer el lakehouse.
resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Sin ACLs: los permisos se controlan solo con IAM y políticas de bucket.
resource "aws_s3_bucket_ownership_controls" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# Cifrado en reposo con SSE-S3 (sin costo, a diferencia de KMS por solicitud).
resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

# Sin versionado: Iceberg ya guarda snapshots, y versionar duplicaría el almacenamiento.

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }

  rule {
    id     = "expire-athena-results"
    status = "Enabled"
    filter {
      prefix = "athena-results/"
    }
    expiration {
      days = 7
    }
  }

  rule {
    id     = "expire-glue-tmp"
    status = "Enabled"
    filter {
      prefix = "glue-tmp/"
    }
    expiration {
      days = 3
    }
  }
}

# Solo tráfico cifrado (TLS).
resource "aws_s3_bucket_policy" "lake" {
  bucket     = aws_s3_bucket.lake.id
  depends_on = [aws_s3_bucket_public_access_block.lake]
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
}

# Marcadores vacíos para que la estructura Medallion se vea en la consola desde el día 1.
resource "aws_s3_object" "prefix" {
  for_each = toset(local.prefixes)
  bucket   = aws_s3_bucket.lake.id
  key      = each.value
  content  = ""
}

output "bucket_name" { value = aws_s3_bucket.lake.id }
output "bucket_arn" { value = aws_s3_bucket.lake.arn }
