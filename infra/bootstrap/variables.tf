variable "region" {
  type    = string
  default = "us-east-1"
}

variable "github_repository" {
  type        = string
  description = "owner/name of the repository whose workflows may assume the CI roles"
  default     = "varunhs306/spotgrid-lakehouse"
}

variable "budget_alert_email" {
  type        = string
  description = "Address that receives budget alerts; kept out of the repo"
}
