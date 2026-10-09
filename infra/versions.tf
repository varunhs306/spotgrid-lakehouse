terraform {
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.67"
    }
  }

  # Applied by CI only. The CI roles can read and lock state under infra/ and nothing else;
  # the bucket name contains the account ID, so it is passed at init.
  backend "s3" {
    key          = "infra/terraform.tfstate"
    region       = "us-east-1"
    encrypt      = true
    use_lockfile = true
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project   = "spotgrid-lakehouse"
      Stack     = "serving"
      ManagedBy = "terraform"
    }
  }
}

data "aws_caller_identity" "current" {}
