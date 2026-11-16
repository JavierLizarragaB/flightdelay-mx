terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.7"
    }
  }

  # Estado local (proyecto individual, gitignored). Para trabajo en equipo se movería
  # a un backend S3 con bloqueo nativo (use_lockfile = true).
}

provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile

  default_tags {
    tags = {
      Project   = var.project
      ManagedBy = "terraform"
      Course    = "UP-MCDITN-BigData"
    }
  }
}
