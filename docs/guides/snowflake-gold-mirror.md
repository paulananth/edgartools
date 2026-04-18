# Snowflake Gold Mirror

This guide documents the preferred Snowflake implementation path for the EdgarTools warehouse.

## Operating model

Snowflake is a downstream gold mirror, not the canonical warehouse.

- bronze remains immutable object storage
- silver remains the canonical normalized lakehouse layer
- canonical gold remains in the warehouse
- Snowflake mirrors selected gold-serving datasets for business access
- dbt owns Snowflake gold models only; it does not own ingestion

## Repo layout

- Terraform baseline: `infra/terraform/snowflake/`
- SnowCLI bootstrap SQL: `infra/snowflake/sql/`
- dbt gold project: `infra/snowflake/dbt/edgartools_gold/`

## Preferred build order

1. Terraform baseline platform objects
2. SnowCLI bootstrap for the storage integration, stage, manifest inbox, status table, source-load procedure, and triggered task
3. dbt deployment of business-facing gold models and dynamic tables
4. AWS runtime cutover from infrastructure validation to real Snowflake-native pull after export completion

## Why this is the preferred path

1. Keep Snowflake downstream so the canonical warehouse remains replayable without Snowflake.
2. Use S3 export plus Snowflake pull so AWS completion does not depend on Snowflake availability.
3. Keep Terraform separate from dbt so platform changes and model changes can evolve at different speeds.
4. Keep one public refresh wrapper so orchestration has one stable runtime contract.
5. Use manifest-gated native pull so Snowflake only reacts after a complete export package exists.

## Current object contract

Baseline object names:

- databases: `EDGARTOOLS_DEV`, `EDGARTOOLS_PROD`
- schemas: `EDGARTOOLS_SOURCE`, `EDGARTOOLS_GOLD`
- roles: `EDGARTOOLS_<ENV>_DEPLOYER`, `EDGARTOOLS_<ENV>_REFRESHER`, `EDGARTOOLS_<ENV>_READER`
- warehouses: `EDGARTOOLS_<ENV>_REFRESH_WH`, `EDGARTOOLS_<ENV>_READER_WH`

Bootstrap object names:

- storage integration: `EDGARTOOLS_<ENV>_EXPORT_INTEGRATION`
- stage: `EDGARTOOLS_SOURCE_EXPORT_STAGE`
- Parquet file format: `EDGARTOOLS_SOURCE_EXPORT_FILE_FORMAT`
- manifest file format: `EDGARTOOLS_SOURCE_RUN_MANIFEST_FILE_FORMAT`
- manifest inbox: `EDGARTOOLS_SOURCE.SNOWFLAKE_RUN_MANIFEST_INBOX`
- manifest pipe: `EDGARTOOLS_SOURCE.SNOWFLAKE_RUN_MANIFEST_PIPE`
- manifest stream: `EDGARTOOLS_SOURCE.SNOWFLAKE_RUN_MANIFEST_STREAM`
- manifest task: `EDGARTOOLS_GOLD.SNOWFLAKE_RUN_MANIFEST_TASK`
- status table: `EDGARTOOLS_SOURCE.SNOWFLAKE_REFRESH_STATUS`
- source load procedure: `EDGARTOOLS_SOURCE.LOAD_EXPORTS_FOR_RUN`
- refresh procedure: `EDGARTOOLS_GOLD.REFRESH_AFTER_LOAD`

dbt gold object names:

- `EDGARTOOLS_GOLD.COMPANY`
- `EDGARTOOLS_GOLD.FILING_ACTIVITY`
- `EDGARTOOLS_GOLD.OWNERSHIP_ACTIVITY`
- `EDGARTOOLS_GOLD.OWNERSHIP_HOLDINGS`
- `EDGARTOOLS_GOLD.ADVISER_OFFICES`
- `EDGARTOOLS_GOLD.ADVISER_DISCLOSURES`
- `EDGARTOOLS_GOLD.PRIVATE_FUNDS`
- `EDGARTOOLS_GOLD.FILING_DETAIL`
- `EDGARTOOLS_GOLD.EDGARTOOLS_GOLD_STATUS`

## Native Pull

Snowflake no longer relies on an AWS-managed sync task or Snowflake credentials in AWS.

Flow:

1. AWS writes deterministic Parquet export packages to the dedicated Snowflake export bucket.
2. AWS writes one final run manifest under `manifests/` after every table file for the run is durable.
3. S3 publishes manifest events to SNS.
4. Snowpipe auto-ingests the manifest into `EDGARTOOLS_SOURCE.SNOWFLAKE_RUN_MANIFEST_INBOX`.
5. A triggered Snowflake task reads the manifest stream, calls `EDGARTOOLS_SOURCE.LOAD_EXPORTS_FOR_RUN`, then calls `EDGARTOOLS_GOLD.REFRESH_AFTER_LOAD`.

## Runtime contract

AWS writes one export package per business table per run to the dedicated Snowflake export bucket,
then writes one final run manifest that Snowpipe watches:

- `manifests/workflow_name=<workflow_name>/business_date=<business_date>/run_id=<run_id>/run_manifest.json`

The manifest-triggered Snowflake task calls two stable SQL procedures:

- `CALL EDGARTOOLS_SOURCE.LOAD_EXPORTS_FOR_RUN(workflow_name, run_id)`
- `CALL EDGARTOOLS_GOLD.REFRESH_AFTER_LOAD(workflow_name, run_id)`

The first call handles Snowflake-side source registration and import logic.
The second call remains the single public refresh wrapper for downstream gold freshness and
must not mark a run successful until all dbt-owned dynamic tables have refreshed successfully.

## dbt ownership

dbt owns:

- `EDGARTOOLS_GOLD_STATUS`
- curated gold-facing tables and views
- dynamic-table definitions
- tests on business-facing objects

Terraform and SnowCLI do not own ongoing gold-model evolution.

## Manual steps required before dbt can run

The following objects must exist before dbt models can materialize. They are created by the bootstrap driver, not by Terraform or dbt:

- `EDGARTOOLS_SOURCE.SNOWFLAKE_REFRESH_STATUS` — dbt gold status model sources from this table; bootstrap must succeed before first dbt run
- Storage integration, external stage, Snowpipe, manifest stream, manifest task — all created by the bootstrap driver

**SNS topic subscription:** After Terraform creates the SNS manifest topic and after the bootstrap creates the Snowpipe, the Snowpipe's SQS queue must be subscribed to the SNS topic. This step is not automated:

1. Get the Snowpipe SQS ARN: `SHOW PIPES LIKE 'SNOWFLAKE_RUN_MANIFEST_PIPE' IN SCHEMA EDGARTOOLS_SOURCE;` — read `notification_channel`
2. In AWS console or CLI, add that SQS ARN as a subscriber to the SNS topic `edgartools-<env>-snowflake-manifest-events`
3. Confirm the subscription in SQS

## Bootstrap driver

The preferred operator path is the SnowCLI bootstrap driver:

```bash
uv run python infra/snowflake/sql/bootstrap_native_pull.py \
  --aws-root infra/terraform/accounts/dev \
  --snowflake-root infra/terraform/snowflake/accounts/dev \
  --connection snowconn \
  --artifact-path infra/snowflake/sql/dev_native_pull_handshake.json
```

It performs the two-pass handshake:

1. reads AWS and Snowflake Terraform outputs
2. creates or updates the storage integration, stage, pipe, procedures, and task
3. captures `DESC INTEGRATION`
4. emits `snowflake_storage_external_id` for the AWS Terraform re-apply
5. reruns with `--storage-external-id` and `--validate-native-pull` after the AWS trust and KMS update
