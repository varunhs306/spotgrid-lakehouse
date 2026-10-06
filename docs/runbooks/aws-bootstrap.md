# AWS bootstrap (one time)

`infra/bootstrap/` creates what CI needs before it can run Terraform itself:
the state bucket, the GitHub OIDC provider, the `spotgrid-ci-plan` / `spotgrid-ci-apply`
roles with the workload permissions boundary, and a 1 USD monthly budget.
It is applied from a laptop with an admin profile. Everything else in `infra/` is applied by CI.

## Who can assume what

| Role | Trusted OIDC subject | Can do |
|---|---|---|
| `spotgrid-ci-plan` | `repo:<repo>:pull_request` | Read `spotgrid-*` resources, lock state under `infra/` |
| `spotgrid-ci-apply` | `repo:<repo>:ref:refs/heads/main` | Manage `spotgrid-*` resources; new roles must carry `spotgrid-workload-boundary` |

Neither role can change itself, the other CI role or the boundary.

## First apply

The state bucket does not exist yet, so the first apply uses local state.

```sh
export AWS_PROFILE=<admin profile>
cd infra/bootstrap
printf 'terraform {\n  backend "local" {}\n}\n' > backend_override.tf
terraform init
terraform apply -var budget_alert_email=<you@example.com>
```

## Move the state into the bucket

```sh
rm backend_override.tf
terraform init -migrate-state -backend-config="bucket=$(terraform output -raw state_bucket)"
rm terraform.tfstate terraform.tfstate.backup
```

`terraform plan -var budget_alert_email=…` should now report no changes.

## Hand the roles to GitHub

```sh
gh variable set AWS_PLAN_ROLE_ARN --body "$(terraform output -raw ci_plan_role_arn)"
gh variable set AWS_APPLY_ROLE_ARN --body "$(terraform output -raw ci_apply_role_arn)"
gh variable set TF_STATE_BUCKET --body "$(terraform output -raw state_bucket)"
```

Check: the `aws-auth` job of the `terraform` workflow passes on a pull request that touches `infra/`.

## Later changes

```sh
terraform init -backend-config="bucket=spotgrid-tfstate-<account id>"
terraform apply -var budget_alert_email=<you@example.com>
```

The state bucket has `prevent_destroy`; remove it deliberately before tearing the bootstrap down.
