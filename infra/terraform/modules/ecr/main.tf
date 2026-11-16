# Repositorios de imágenes: api (Lambda) y app (ECS Fargate).
variable "project" { type = string }
variable "repos" {
  type    = list(string)
  default = ["api", "app"]
}

resource "aws_ecr_repository" "this" {
  for_each             = toset(var.repos)
  name                 = "${var.project}-${each.key}"
  image_tag_mutability = "MUTABLE" # el tag "latest" se mueve; Lambda/ECS fijan el digest
  force_delete         = true      # `terraform destroy` borra también las imágenes

  image_scanning_configuration {
    scan_on_push = true # escaneo básico de vulnerabilidades, sin costo
  }
}

# Solo las 5 imágenes más recientes: cada imagen de la API pesa ~400 MB comprimida.
resource "aws_ecr_lifecycle_policy" "this" {
  for_each   = aws_ecr_repository.this
  repository = each.value.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Conservar las 5 imagenes mas recientes"
      selection    = { tagStatus = "any", countType = "imageCountMoreThan", countNumber = 5 }
      action       = { type = "expire" }
    }]
  })
}

output "repository_urls" { value = { for k, r in aws_ecr_repository.this : k => r.repository_url } }
output "repository_names" { value = { for k, r in aws_ecr_repository.this : k => r.name } }
