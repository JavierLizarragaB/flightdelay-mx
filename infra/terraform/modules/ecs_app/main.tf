# App Streamlit en ECS Fargate (arm64), en la VPC por defecto con IP pública.
#
# Costo: Fargate cobra por segundo mientras la tarea corre (0.25 vCPU + 0.5 GB arm64
# ≈ USD 0.01/h, más ≈ USD 0.005/h por la IPv4 pública). Por eso desired_count = 0 por
# defecto: se enciende para la demo con `make app-up` y se apaga con `make app-down`.
# Sin ALB (≈ USD 16/mes fijo) ni NAT Gateway: la tarea sale a internet por su IP pública.
variable "project" { type = string }
variable "region" { type = string }
variable "repository_name" { type = string }
variable "repository_url" { type = string }
variable "api_url" { type = string }
variable "image_tag" {
  type    = string
  default = "latest"
}
variable "desired_count" {
  type    = number
  default = 0
}
variable "allowed_cidr" {
  description = "Quién puede abrir la app (puerto 8501). Restringe a tu IP/32 si prefieres."
  type        = string
  default     = "0.0.0.0/0"
}
variable "log_retention_days" {
  type    = number
  default = 14
}

locals {
  name = "${var.project}-app"
}

data "aws_vpc" "default" {
  default = true
}

# Fargate no está disponible en todas las AZ de us-east-1; se usan a-d.
data "aws_subnets" "public" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "default-for-az"
    values = ["true"]
  }
  filter {
    name   = "availability-zone"
    values = ["${var.region}a", "${var.region}b", "${var.region}c", "${var.region}d"]
  }
}

data "aws_ecr_image" "app" {
  repository_name = var.repository_name
  image_tag       = var.image_tag
}

resource "aws_security_group" "app" {
  name        = local.name
  description = "Streamlit 8501 entrante; salida HTTPS hacia la API"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "Streamlit"
    from_port   = 8501
    to_port     = 8501
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
  }

  egress {
    description = "HTTPS (API Gateway, ECR)"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_ecs_cluster" "this" {
  name = var.project
}

resource "aws_cloudwatch_log_group" "app" {
  name              = "/ecs/${local.name}"
  retention_in_days = var.log_retention_days
}

data "aws_iam_policy_document" "tasks_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

# Rol de ejecución: lo usa ECS para bajar la imagen y escribir logs (no la app).
resource "aws_iam_role" "execution" {
  name               = "${local.name}-execution"
  assume_role_policy = data.aws_iam_policy_document.tasks_trust.json
}

resource "aws_iam_role_policy_attachment" "execution" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_ecs_task_definition" "app" {
  family                   = local.name
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 256
  memory                   = 512
  execution_role_arn       = aws_iam_role.execution.arn
  # Sin task role: la app no llama a ningún servicio de AWS, solo a la API por HTTPS.

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64"
  }

  container_definitions = jsonencode([{
    name         = "app"
    image        = "${var.repository_url}@${data.aws_ecr_image.app.image_digest}"
    essential    = true
    portMappings = [{ containerPort = 8501, protocol = "tcp" }]
    environment = [
      { name = "API_URL", value = var.api_url },
      { name = "API_TIMEOUT_SECONDS", value = "5" },
      { name = "API_MAX_RETRIES", value = "2" },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.app.name
        awslogs-region        = var.region
        awslogs-stream-prefix = "app"
      }
    }
  }])
}

resource "aws_ecs_service" "app" {
  name            = local.name
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.app.arn
  desired_count   = var.desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = data.aws_subnets.public.ids
    security_groups  = [aws_security_group.app.id]
    assign_public_ip = true
  }

  # make app-up / app-down cambian desired_count fuera de Terraform.
  lifecycle {
    ignore_changes = [desired_count]
  }
}

output "cluster_name" { value = aws_ecs_cluster.this.name }
output "service_name" { value = aws_ecs_service.app.name }
