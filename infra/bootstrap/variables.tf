variable "region" {
  type    = string
  default = "us-east-1"
}

# GitHub's immutable subject format embeds the owner and repo IDs, so a renamed or
# re-created repo with the same name cannot assume the roles. Read it with:
#   gh api repos/OWNER/REPO/actions/oidc/customization/sub
variable "github_sub_prefix" {
  type        = string
  description = "OIDC sub claim prefix of the repository whose workflows may assume the CI roles"
  default     = "repo:varunhs306@53350637/spotgrid-lakehouse@1398649456"
}

variable "budget_alert_email" {
  type        = string
  description = "Address that receives budget alerts; kept out of the repo"
}
