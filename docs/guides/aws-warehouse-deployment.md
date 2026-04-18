# AWS Warehouse Deployment

This guide documents the AWS reference deployment for the SEC warehouse described in [specification.md](C:/work/projects/edgartools/specification.md).

The Snowflake mirror follow-on path is documented in [docs/guides/snowflake-gold-mirror.md](C:/work/projects/edgartools/docs/guides/snowflake-gold-mirror.md).

## Scope

The AWS deployment covers the warehouse platform only:

- immutable bronze storage
- mutable staging, silver, gold, and artifact storage
- dedicated Snowflake export storage
- ECS Fargate execution for warehouse commands
- Step Functions orchestration
- EventBridge Scheduler for recurring workflows
- Secrets Manager for `EDGAR_IDENTITY`
- SNS notifications for Snowflake run manifests
- CloudWatch logs and Step Functions failure alarms

Explicitly out of scope in v1:

- Glue Catalog
- Athena
- DynamoDB-based execution locking
- CI/CD automation inside Terraform
- always-on APIs, ALBs, or API Gateway
- NAT gateways, private subnets, or private ECS networking

## Terraform layout

```text
infra/terraform/
  bootstrap-state/
  accounts/
    dev/
    prod/
  modules/
    network_runtime/
    storage_buckets/
    warehouse_runtime/
```

Pinned toolchain:

- Terraform CLI `= 1.14.7`
- AWS provider `= 6.39.0`

Each account root uses an S3 backend with `use_lockfile = true`.

## Account model

Use one AWS account for `dev` and one AWS account for `prod`.

Each account owns its own:

- Terraform state bucket
- bronze bucket
- warehouse bucket
- Snowflake export bucket
- ECR repository
- ECS cluster
- Step Functions state machines
- EventBridge schedules
- Secrets Manager secret for `EDGAR_IDENTITY`
- CloudWatch log group and alarms

Deterministic names:

- `edgartools-dev-tfstate`
- `edgartools-dev-bronze`
- `edgartools-dev-warehouse`
- `edgartools-dev-snowflake-export`
- `edgartools-prod-tfstate`
- `edgartools-prod-bronze`
- `edgartools-prod-warehouse`
- `edgartools-prod-snowflake-export`

## Storage contract on AWS

Bronze and warehouse data live in separate buckets per account.

Snowflake export data lives in a third dedicated bucket per account.

Bronze bucket:

- stores immutable raw SEC payloads and raw daily index files
- enables bucket versioning
- is not used for staging, silver, gold, or derived artifacts

Warehouse bucket:

- stores mutable `staging`, `silver`, `gold`, and `artifacts` prefixes
- is the only bucket that holds Parquet datasets

Snowflake export bucket:

- stores one export package per business table per run for the Snowflake mirror
- is isolated from the canonical warehouse bucket
- publishes manifest notifications that Snowpipe consumes through SNS

Required prefixes:

- `s3://<bronze-bucket>/warehouse/bronze/...`
- `s3://<warehouse-bucket>/warehouse/staging/...`
- `s3://<warehouse-bucket>/warehouse/silver/sec/...`
- `s3://<warehouse-bucket>/warehouse/gold/...`
- `s3://<warehouse-bucket>/warehouse/artifacts/...`
- `s3://<snowflake-export-bucket>/warehouse/artifacts/snowflake_exports/...`

`bootstrap_full` and `bootstrap_recent_10` write to the same silver and gold prefixes. The only difference is row scope.

Current runtime modes:

- `infrastructure_validation` writes run manifests only
- `bronze_capture` writes real bronze raw objects for reference files, daily index files, and submissions JSON while downstream layers remain staged
- in `bronze_capture`, `daily_incremental` can derive impacted CIKs from the raw daily index and capture bounded main submissions JSON before tracked-universe state exists
- `WAREHOUSE_BRONZE_CIK_LIMIT` is the temporary safety cap for that bounded daily capture path

Bronze raw object paths now include:

- `s3://<bronze-bucket>/warehouse/bronze/reference/sec/company_tickers/...`
- `s3://<bronze-bucket>/warehouse/bronze/reference/sec/company_tickers_exchange/...`
- `s3://<bronze-bucket>/warehouse/bronze/daily_index/sec/...`
- `s3://<bronze-bucket>/warehouse/bronze/submissions/sec/...`

## Compute and orchestration

The runtime uses one container image built from this repo and executed on ECS Fargate.

The image installs the package without the full analysis dependency tree, then installs the curated warehouse runtime dependency set and exposes the `edgar-warehouse` CLI entrypoint.

For the current AWS runtime contract, that warehouse dependency set includes:

- `httpx`
- `duckdb`
- `pyarrow`
- `pytz`
- `zstandard`
- `fsspec`
- `s3fs`

`pytz` is required in the warehouse runtime because DuckDB's Python adapter needs it when
materializing `TIMESTAMPTZ` values back into Python.

The Docker build should copy only runtime-needed files such as `pyproject.toml`, `README.md`, `LICENSE.txt`, and `edgar/`, not the full repo tree.

All workflows use a single canonical warehouse ECS task on public subnets. Snowflake import happens
later through Snowflake-native pull after the final run manifest lands in S3.

Step Functions state machines:

- `daily_incremental`
- `bootstrap_recent_10`
- `bootstrap_full`
- `targeted_resync`
- `full_reconcile`

EventBridge Scheduler schedules:

- `daily_incremental`: weekdays at `06:30 America/New_York`
- `full_reconcile`: Saturday at `09:00 America/New_York`

Manual-only workflows:

- `bootstrap_recent_10`
- `bootstrap_full`
- `targeted_resync`

CLI commands exposed by the container:

- `bootstrap-full`
- `bootstrap-recent-10`
- `daily-incremental`
- `load-daily-form-index-for-date`
- `catch-up-daily-form-index`
- `seed-universe`
- `targeted-resync`
- `full-reconcile`

## Secrets and IAM

Two Secrets Manager secrets/resources per environment remain in AWS:

| Secret | Purpose | Injected into |
|---|---|---|
| `edgartools-<env>-edgar-identity` | SEC EDGAR user-agent identity | Warehouse ECS task |
| `edgartools-<env>-runner-credentials` | Manual runner access key for Step Functions triggering | Operator / runner client |

Warehouse task role access is scoped to:

- bronze bucket read and write without delete
- warehouse bucket read, write, and delete
- Snowflake export bucket write
- CloudWatch Logs
- the EDGAR identity secret

No Snowflake credential is required in AWS application runtime. Snowflake pulls from S3 natively after
AWS publishes the final run manifest.

## Bootstrap and apply flow

Bootstrap the state bucket inside each AWS account first:

```bash
cd infra/terraform/bootstrap-state
terraform init
terraform apply -var environment=dev
```

Then initialize each account root with its backend config:

```bash
cd infra/terraform/accounts/dev
terraform init -backend-config=backend.hcl
terraform plan
terraform apply
```

Example `backend.hcl` values are checked into each account root as `backend.hcl.example`.

The `container_image` value should be set to an ECR image tag or, preferably, an image digest.

Minimum `terraform.tfvars` values for a runnable deployment:

```hcl
container_image = "123456789012.dkr.ecr.us-east-1.amazonaws.com/edgartools-dev-warehouse@sha256:replace-me"
edgar_identity_value = "Your Name your.email@example.com"
```

Destroy policy:

- `infra/terraform/accounts/dev` is intentionally destroyable and uses force-delete semantics for
  the dev data buckets, ECR repository, and runner IAM user.
- `infra/terraform/accounts/prod` is intentionally not destroyable from the account root because
  the protected storage module keeps the bronze bucket behind `prevent_destroy`.
- `infra/terraform/bootstrap-state` remains separate from both account roots. Destroying an account
  root does not remove the Terraform state bucket.

## Building and pushing the container image

The ECR repository uses `image_tag_mutability = IMMUTABLE`. You cannot push a tag that already exists; always use a new tag (e.g. the git short SHA).

Recommended release policy:

- Primary release path is Linux-first direct registry push from CI, CodeBuild, EC2, or WSL2
- On this Windows workstation, the only documented local publish path is the checked-in WSL wrapper
- Build a single-platform Linux image for ECS/Fargate: `linux/amd64`
- Keep provenance and SBOM enabled on the primary publish path so ECR can retain OCI referrers for the image
- Use immutable tags such as the git SHA, then deploy ECS by digest after ECR verification
- Keep ECR `scan_on_push` enabled and add Amazon ECR managed signing outside Terraform when the environment is ready for signature enforcement
- The repository-standard publish helper is `infra/scripts/publish-warehouse-image.sh`
- The repository-standard Windows wrapper is `infra/scripts/publish-warehouse-image-via-wsl.sh`
- The reference CodeBuild entrypoint is `infra/codebuild/buildspec.publish-warehouse-image.yml`

Operator note for this workspace:

- the WSL bridge to `docker` and `aws` works
- use the Linux-style `buildx --push` path through WSL for future publishes from this Windows machine
- do not fall back to Windows `crane` or other local publish paths from this machine

Canonical local Windows push flow:

```bash
GIT_SHA=$(git rev-parse --short HEAD)
bash infra/scripts/publish-warehouse-image-via-wsl.sh \
  --aws-profile <profile> \
  --aws-region us-east-1 \
  --ecr-repository edgartools-<env>-warehouse \
  --image-tag "${GIT_SHA}" \
  --output-file image-ref.txt
```

The WSL wrapper:

- re-enters WSL so the publish runs as a Linux-style `buildx --push`
- bridges to the installed Windows `docker.exe` and `aws.exe`
- strips Windows CRLF from `aws.exe` output so ECR registry URLs stay valid
- normalizes the inner publish helper to LF before execution so existing Windows checkouts do not trip over shell line endings
- forces `--mode linux`
- defaults to `--push-attempts 3` so transient ECR upload failures get retried on the same immutable tag before the command fails

The helper performs the direct `buildx --push`, verifies the image in ECR, and writes the final
digest reference to `image-ref.txt`.

Primary native Linux push flow:

```bash
GIT_SHA=$(git rev-parse --short HEAD)
bash infra/scripts/publish-warehouse-image.sh \
  --aws-profile <profile> \
  --aws-region us-east-1 \
  --ecr-repository edgartools-<env>-warehouse \
  --image-tag "${GIT_SHA}" \
  --mode linux \
  --output-file image-ref.txt
```

In Linux mode the helper bootstraps a `docker-container` buildx builder so provenance and SBOM
attestations work in CI and CodeBuild.

When using CodeBuild, configure the project to read this repository and point the buildspec at:

`infra/codebuild/buildspec.publish-warehouse-image.yml`

### Why the local publish has been brittle

Docker Desktop on Windows routes registry traffic through an internal proxy (`192.168.65.1:3128`). In practice this makes ECR publication brittle for this image because the warehouse dependency set includes large layers, especially `pyarrow`. The usual failure mode is a mid-upload `broken pipe` or `connection reset` while publishing one large blob.

The WSL wrapper above is the documented local operator path because it removes the avoidable failure
classes we saw in ad hoc attempts:

1. CRLF shell scripts failing under WSL
2. Windows `aws.exe` emitting `\r` into shell variables and breaking ECR URLs
3. inconsistent one-off wrapper scripts under `.tmp/`
4. manual retries not being encoded in the publish command itself

Verification after push:

```bash
IMAGE_REF=$(cat image-ref.txt)
echo "${IMAGE_REF}"
aws ecr describe-images \
  --repository-name "edgartools-<env>-warehouse" \
  --region us-east-1 \
  --profile <profile> \
  --image-ids imageTag="${GIT_SHA}"
```

Only update `terraform.tfvars` after the image tag is visible in ECR and the digest is known.

After a successful push, update `container_image` in `terraform.tfvars` to the verified digest:

```bash
# image-ref.txt contains the immutable digest reference, e.g.:
# <ecr-url>@sha256:b7c361b843eb53b6c0afdd3ff9a03305c12ecc1619647f67a89211b648ead225
# Use the @sha256:... form in terraform.tfvars for immutable production references.
```

### Cold redeploy order

For a full `dev` destroy/recreate or a first deployment in a new account, use this order:

1. Apply the AWS account root to create the VPC, buckets, IAM roles, ECS cluster, Step Functions, and ECR repository.
2. Build and publish the image to ECR, then verify the tag and digest exist.
3. Update `container_image` in `terraform.tfvars` to the pushed digest.
4. Re-apply the AWS account root so ECS task definitions reference the new digest.
5. Bootstrap or refresh Snowflake-native pull objects to capture the current Snowflake storage external ID.
6. Re-apply the AWS account root with `snowflake_storage_external_id` so the Snowflake storage-role trust and export-CMK permissions are complete.
7. Rerun the Snowflake bootstrap validation path and only then trigger warehouse workflows.

Do not destroy and recreate Snowflake pull objects against a stale SNS topic ARN from the previous AWS deployment.

## Image publishing RCA

The repeated local ECR publication failures observed on Windows were caused by:

1. a large warehouse runtime layer, mainly from `pyarrow`
2. Docker Desktop proxying registry uploads through an unstable local proxy path
3. running ad hoc local wrapper commands instead of one checked-in WSL bridge entrypoint
4. CRLF shell files and CRLF `aws.exe` output crossing the Windows to WSL boundary
5. cold redeploy sequencing not making image publication and digest verification an explicit gate before workflow execution

## Operator runbook

### Scheduled workflows

- `daily_incremental` is the authoritative recurring ingestion path
- `full_reconcile` is the recurring truth-check and repair path

### Manual workflows

Before starting any manual mutating workflow, confirm there is no other active mutating Step Functions execution in the same environment.

Manual workflows:

- `bootstrap_recent_10`
- `bootstrap_full`
- `seed_universe`
- `targeted_resync`
- `full_reconcile`

This is a temporary v1 operational control because no distributed application lock is provisioned.

Always trigger workflows using the runner IAM account (`edgartools-<env>-runner`), not the Terraform deployer account. Runner credentials are stored in Secrets Manager under `edgartools-<env>-runner-credentials`.

#### Runner credential initialization

Terraform creates the runner IAM user and the runner credentials secret container, but it does not create an access key. Complete the bootstrap after apply:

```bash
RUNNER_USER="edgartools-dev-runner"
RUNNER_SECRET_ID="edgartools-dev-runner-credentials"
ACCESS_KEY_JSON=$(aws iam create-access-key --user-name "$RUNNER_USER" --profile edgartools-dev)
RUNNER_ACCESS_KEY_ID=$(echo "$ACCESS_KEY_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['AccessKey']['AccessKeyId'])")
RUNNER_SECRET_ACCESS_KEY=$(echo "$ACCESS_KEY_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['AccessKey']['SecretAccessKey'])")

aws secretsmanager put-secret-value \
  --secret-id "$RUNNER_SECRET_ID" \
  --profile edgartools-dev \
  --secret-string "{\"aws_access_key_id\":\"$RUNNER_ACCESS_KEY_ID\",\"aws_secret_access_key\":\"$RUNNER_SECRET_ACCESS_KEY\",\"aws_region\":\"us-east-1\"}"
```

Delete or rotate the access key through IAM and overwrite the secret if runner credentials are ever reissued.

#### Step Functions input requirements

Each state machine expects specific fields in the execution input JSON:

| Workflow | Required input fields |
|---|---|
| `bootstrap_recent_10` | none required; optional `{"cik_list": "320193,789019,..."}` override |
| `bootstrap_full` | none required; optional `{"cik_list": "320193,789019,..."}` override |
| `daily_incremental` | none required; optional `{"cik_list": "320193,789019,..."}` override |
| `load_daily_form_index_for_date` | `{"target_date": "YYYY-MM-DD"}` |
| `seed_universe` | none required |
| `targeted_resync` | `{"scope_type": "<type>", "scope_key": "<key>"}` |
| `catch_up_daily_form_index` | none required |
| `full_reconcile` | none required |

`bootstrap_recent_10`, `bootstrap_full`, and `daily_incremental` now default to the seeded
tracked universe when `cik_list` is omitted.

Example trigger using runner credentials from Secrets Manager:

```bash
# Retrieve runner credentials
CREDS=$(aws secretsmanager get-secret-value \
  --secret-id "edgartools-dev-runner-credentials" \
  --profile edgartools-dev \
  --query SecretString --output text)
RUNNER_KEY=$(echo $CREDS | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['aws_access_key_id'])")
RUNNER_SECRET=$(echo $CREDS | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['aws_secret_access_key'])")

# Trigger bootstrap-recent-10
RUN_ID="bootstrap-recent-10-$(date +%Y%m%d-%H%M%S)"
AWS_ACCESS_KEY_ID="$RUNNER_KEY" \
AWS_SECRET_ACCESS_KEY="$RUNNER_SECRET" \
AWS_DEFAULT_REGION="us-east-1" \
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:us-east-1:<account-id>:stateMachine:edgartools-dev-bootstrap-recent-10" \
  --name "$RUN_ID" \
  --input '{"cik_list":"320193,789019,1045810,1018724,1652044"}'
```

#### Reading silver table counts from a completed run

The warehouse ECS task emits `silver_table_counts` in its CloudWatch log output. To retrieve them after a run:

```bash
# Find the log stream for the ECS task (most recent warehouse-medium stream)
MSYS_NO_PATHCONV=1 aws logs describe-log-streams \
  --log-group-name "/aws/ecs/edgartools-<env>-warehouse" \
  --order-by LastEventTime --descending --max-items 5 \
  --profile edgartools-dev

# Read the log (contains the full JSON output including silver_table_counts)
MSYS_NO_PATHCONV=1 aws logs get-log-events \
  --log-group-name "/aws/ecs/edgartools-<env>-warehouse" \
  --log-stream-name "warehouse-medium/edgar-warehouse/<task-id>" \
  --start-from-head --profile edgartools-dev \
  --query "events[*].message" --output text
```

> **Windows note**: Prefix log group names with `MSYS_NO_PATHCONV=1` to prevent Git Bash from mangling the `/aws/ecs/...` path into a Windows filesystem path.

### Secret initialization

If Terraform creates the `EDGAR_IDENTITY` secret, the secret container exists after apply but no secret value is populated yet.

Populate it before running the workflows:

```bash
aws secretsmanager put-secret-value \
  --secret-id edgartools-dev-edgar-identity \
  --secret-string "Your Name your.email@example.com"
```

### Snowflake manifest topic handoff

After apply, capture these AWS account-root outputs:

- `snowflake_manifest_sns_topic_arn`
- `snowflake_storage_role_arn`
- `snowflake_export_root_url`
- `snowflake_export_kms_key_arn`

Run the Snowflake bootstrap driver with those outputs. It emits `snowflake_storage_external_id`,
which must be written back into AWS Terraform before the Snowflake storage role can assume the
bucket and CMK reader path end to end.

## Validation

Recommended validation sequence:

```bash
terraform fmt -check
terraform validate
terraform plan
```

Runtime checks in `dev`:

- trigger `bootstrap-recent-10` via the runner account with `{"cik_list":"320193,789019,1045810"}` as execution input
- confirm bronze objects land only in the bronze bucket under `warehouse/bronze/submissions/sec/cik=<cik>/...`
- confirm Snowflake export Parquet files land in the export bucket under `warehouse/artifacts/snowflake_exports/<table>/...`
- confirm final Snowflake run manifests land in the export bucket under `warehouse/artifacts/snowflake_exports/manifests/...`
- confirm `LIST @EDGARTOOLS_SOURCE.EDGARTOOLS_SOURCE_EXPORT_STAGE/manifests/` succeeds
- confirm `COPY_HISTORY` for `EDGARTOOLS_SOURCE.SNOWFLAKE_RUN_MANIFEST_INBOX` shows `STATUS = 'Loaded'`
- check CloudWatch log output for `silver_table_counts` - expect `sec_company=3`, `sec_company_filing=30` (10 x 3 companies) for the three test CIKs
- confirm failed Step Functions executions appear in CloudWatch alarms
- trigger `load-daily-form-index-for-date` with `{"target_date": "YYYY-MM-DD"}` for a known business date and confirm `sec_daily_index_checkpoint` checkpoint appears in the silver layer log output
