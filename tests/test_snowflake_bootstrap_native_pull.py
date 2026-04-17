import importlib.util
from pathlib import Path

import pytest


def _load_bootstrap_module():
    module_path = Path("infra/snowflake/sql/bootstrap_native_pull.py")
    spec = importlib.util.spec_from_file_location("bootstrap_native_pull", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@pytest.mark.fast
def test_parse_desc_integration_extracts_handshake_fields():
    module = _load_bootstrap_module()

    metadata = module._parse_desc_integration(
        [
            {"property": "STORAGE_AWS_IAM_USER_ARN", "property_value": "arn:aws:iam::247332612486:user/example"},
            {"property": "STORAGE_AWS_ROLE_ARN", "property_value": "arn:aws:iam::123456789012:role/edgartools-dev-snowflake-s3"},
            {"property": "STORAGE_AWS_EXTERNAL_ID", "property_value": "external-id"},
            {
                "property": "STORAGE_ALLOWED_LOCATIONS",
                "property_value": "s3://bucket/warehouse/artifacts/snowflake_exports/",
            },
        ]
    )

    assert metadata["STORAGE_AWS_IAM_USER_ARN"] == "arn:aws:iam::247332612486:user/example"
    assert metadata["STORAGE_AWS_ROLE_ARN"] == "arn:aws:iam::123456789012:role/edgartools-dev-snowflake-s3"
    assert metadata["STORAGE_AWS_EXTERNAL_ID"] == "external-id"
    assert metadata["STORAGE_ALLOWED_LOCATIONS"] == "s3://bucket/warehouse/artifacts/snowflake_exports/"


@pytest.mark.fast
def test_validate_integration_metadata_rejects_unexpected_subscriber():
    module = _load_bootstrap_module()

    with pytest.raises(module.BootstrapError):
        module._validate_integration_metadata(
            integration_metadata={
                "STORAGE_AWS_IAM_USER_ARN": "arn:aws:iam::247332612486:user/unexpected",
                "STORAGE_AWS_ROLE_ARN": "arn:aws:iam::123456789012:role/edgartools-dev-snowflake-s3",
                "STORAGE_ALLOWED_LOCATIONS": "s3://bucket/warehouse/artifacts/snowflake_exports/",
            },
            expected_subscriber_arn="arn:aws:iam::247332612486:user/expected",
            expected_storage_role_arn="arn:aws:iam::123456789012:role/edgartools-dev-snowflake-s3",
            expected_export_root_url="s3://bucket/warehouse/artifacts/snowflake_exports/",
        )


@pytest.mark.fast
def test_build_handshake_artifact_surfaces_external_id_for_terraform():
    module = _load_bootstrap_module()

    artifact = module._build_handshake_artifact(
        session_variables={
            "database_name": "EDGARTOOLS_DEV",
            "source_schema_name": "EDGARTOOLS_SOURCE",
            "gold_schema_name": "EDGARTOOLS_GOLD",
            "storage_integration_name": "EDGARTOOLS_DEV_EXPORT_INTEGRATION",
        },
        aws_outputs={
            "snowflake_manifest_sns_topic_arn": "arn:aws:sns:us-east-1:123456789012:topic",
            "snowflake_export_root_url": "s3://bucket/warehouse/artifacts/snowflake_exports/",
        },
        integration_metadata={
            "STORAGE_AWS_ROLE_ARN": "arn:aws:iam::123456789012:role/edgartools-dev-snowflake-s3",
            "STORAGE_AWS_IAM_USER_ARN": "arn:aws:iam::247332612486:user/example",
            "STORAGE_AWS_EXTERNAL_ID": "external-id",
            "STORAGE_ALLOWED_LOCATIONS": "s3://bucket/warehouse/artifacts/snowflake_exports/",
        },
        native_pull_validation=None,
    )

    assert artifact["storage_integration_name"] == "EDGARTOOLS_DEV_EXPORT_INTEGRATION"
    assert artifact["storage_aws_external_id"] == "external-id"
    assert artifact["next_terraform_input"]["snowflake_storage_external_id"] == "external-id"
    assert artifact["gold_dynamic_tables"] == list(module.GOLD_DYNAMIC_TABLES)
