# One per account per URL; spotgrid-platform takes this over later.
# Thumbprints are no longer checked by AWS for this provider, so none are pinned.
resource "aws_iam_openid_connect_provider" "github" {
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
}
