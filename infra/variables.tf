variable "region" {
  type    = string
  default = "eu-central-1"
}

variable "alarm_email" {
  type        = string
  description = "Address that receives alarms; kept out of the repo"
  # Plans are posted on public pull requests.
  sensitive = true
}

variable "api_zip" {
  type        = string
  description = "API Lambda zip built by scripts/build_lambda.py"
  default     = "../build/api-lambda.zip"
}

variable "api_domain" {
  type    = string
  default = "api.spotgrid.quarrylabs.dev"
}

variable "pipeline_image_tag" {
  type        = string
  description = "Tag of the pipeline image in ECR; CI passes the commit SHA"
  default     = "latest"
}

variable "github_repository" {
  type        = string
  description = "owner/name for incident and data-quality issues and data releases"
  default     = "varunhs306/spotgrid-lakehouse"
}
