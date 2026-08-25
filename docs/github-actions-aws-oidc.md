# GitHub Actions AWS auth: GitHub OIDC (no static keys)

The Wikimedia workflows (`wikimedia-launch`, `wikimedia-kill`, `wikimedia-retry`,
`wikimedia-upload-status`) authenticate to AWS by assuming a dedicated IAM role
through **GitHub OIDC** — short-lived credentials issued per run, with nothing
long-lived stored in the repository.

## Why

- Long-lived IAM access keys are a standing liability: they must be rotated, they
  persist if the owning person leaves, and a leak grants whatever the key allows
  until it is manually revoked.
- OIDC issues short-lived credentials per run, scoped by repository (and,
  optionally, by branch or GitHub Environment), with no secret key material in
  GitHub.
- The sibling repo `dpla/ingestion3` adopts the same OIDC pattern for its ingest
  workflow, keeping the two consistent.

## What the workflows actually need

The scripts touch AWS through one path only — SSM against the "wiki downloads"
instance (`ingest_wikimedia/ssm.py`):

- `ssm:SendCommand` to instance `i-033eff6c8c168f999`, document `AWS-RunShellScript`
- `ssm:GetCommandInvocation`

No EC2 start/stop (the instance is always running) and no S3 from the runner (S3
access happens *on* the instance under its own instance profile). The role's
permissions policy is scoped accordingly — tighter than a generic ingest role.

## Workflow side (in this repo)

Each workflow declares `id-token: write` and assumes the role before the step
that calls AWS:

```yaml
permissions:
  contents: read
  id-token: write

# ...
      - name: Configure AWS credentials (GitHub OIDC)
        uses: aws-actions/configure-aws-credentials@e6de054238d6b7531b4efff3b6587d9aade6a06c # v6.2.3
        with:
          role-to-assume: arn:aws:iam::283408157088:role/github-actions-ingest-wikimedia
          aws-region: us-east-1
```

`configure-aws-credentials` exports `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
/ `AWS_SESSION_TOKEN` / `AWS_DEFAULT_REGION` to the job environment, which the
boto3 scripts pick up through the default credential chain — so the explicit
`AWS_*` step env is gone.

The role ARN is hardcoded in the workflow YAML — it is an identifier, not a
credential, so nothing sensitive is exposed (the same account ID already appears
in the sibling `dpla/ingestion3` workflow). No GitHub secret is used for it.

## AWS side (one-time provisioning, by someone with IAM access)

This is not repo code — it is IAM that must exist before the workflows can run.
If DPLA manages IAM as code (Terraform / CDK / CloudFormation), add these there,
not via one-off CLI; the JSON below is the canonical spec to translate.

Before starting, verify the **current** principal behind the `WIKIMEDIA_AWS_*`
keys (CloudTrail on a recent `SendCommand` event, or a one-off
`aws sts get-caller-identity` step) and confirm it is a service principal, not an
individual's account. That is the liability this migration removes.

**1. Ensure the account's GitHub OIDC provider exists** (account-level; reuse if
present — do not duplicate):

```bash
aws iam list-open-id-connect-providers
# look for .../token.actions.githubusercontent.com ; create only if absent:
aws iam create-open-id-connect-provider \
  --url https://token.actions.githubusercontent.com \
  --client-id-list sts.amazonaws.com \
  --thumbprint-list 6938fd4d98bab03faadb97b34396831e3780aea1
```

**2. Create the role with an OIDC trust policy scoped to this repo's `main` branch
only.** Narrow from the start: a `workflow_dispatch` run on a non-`main` ref must
not be able to assume the role.

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {
      "Federated": "arn:aws:iam::283408157088:oidc-provider/token.actions.githubusercontent.com"
    },
    "Action": "sts:AssumeRoleWithWebIdentity",
    "Condition": {
      "StringEquals": {
        "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
        "token.actions.githubusercontent.com:sub": "repo:dpla/ingest-wikimedia:ref:refs/heads/main"
      }
    }
  }]
}
```

**3. Attach the least-privilege permissions policy** (only what the workflows use):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "SendCommandToWikiInstance",
      "Effect": "Allow",
      "Action": "ssm:SendCommand",
      "Resource": [
        "arn:aws:ec2:us-east-1:283408157088:instance/i-033eff6c8c168f999",
        "arn:aws:ssm:us-east-1::document/AWS-RunShellScript"
      ]
    },
    {
      "Sid": "ReadCommandInvocation",
      "Effect": "Allow",
      "Action": "ssm:GetCommandInvocation",
      "Resource": "*"
    }
  ]
}
```

`ssm:GetCommandInvocation` does not support resource-level scoping, hence `"*"`.
`ssm:SendCommand` IS scoped — to this one instance and only the
`AWS-RunShellScript` document.

The role ARN is hardcoded in the workflows, so there is **no GitHub secret to set**.

## Rollout order

1. Provision the OIDC provider + role (above).
2. Merge this change to `main`.
3. Verify a real run end-to-end (e.g. `wikimedia-upload-status`, the cheapest —
   it only reads instance state and posts to Slack). Because the trust is
   `main`-only, verification runs from `main` after merge — a non-`main` branch
   cannot assume the role.
4. Only then **delete the `WIKIMEDIA_AWS_ACCESS_KEY_ID` / `WIKIMEDIA_AWS_SECRET_ACCESS_KEY`
   secrets and deactivate + delete the old IAM access keys.**

## Rollback

Revert the workflow diff (which restores the `AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY` step env) and re-add the `WIKIMEDIA_AWS_ACCESS_KEY_ID` /
`WIKIMEDIA_AWS_SECRET_ACCESS_KEY` secrets if they were removed. Nothing else in
the pipeline depends on the auth method.
