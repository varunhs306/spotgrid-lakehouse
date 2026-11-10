terraform {
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.67"
    }
  }

  # Applied by CI only. The CI roles can read and lock state under infra/ and nothing else;
  # the bucket name contains the account ID, so it is passed at init. State stays in the
  # bootstrap bucket in us-east-1; the resources live in var.region.
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

locals {
  account_id        = data.aws_caller_identity.current.account_id
  workload_boundary = "arn:aws:iam::${local.account_id}:policy/spotgrid-workload-boundary"
  # Bucket names are global; the region suffix keeps them apart from the us-east-1 ones.
  suffix = "${local.account_id}-${var.region}"
}
