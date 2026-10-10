variable "region" {
  type    = string
  default = "us-east-1"
}

# Same immutable subject as infra/bootstrap: owner and repo IDs, not just names.
variable "github_sub_prefix" {
  type        = string
  description = "OIDC sub claim prefix of the repository whose workflows may assume the export role"
  default     = "repo:varunhs306@53350637/spotgrid-lakehouse@1398649456"
}

variable "alarm_email" {
  type        = string
  description = "Address that receives API alarms; kept out of the repo"
  # Plans are posted on public pull requests.
  sensitive = true
}

variable "api_zip" {
  type        = string
  description = "API Lambda zip built by scripts/build_lambda.py"
  default     = "../build/api-lambda.zip"
}
