import json
from pathlib import Path

import pytest


def _render_optional_cik_template() -> dict:
    template_path = Path("infra/terraform/modules/warehouse_runtime/templates/ecs_run_task_optional_cik_list.asl.json.tmpl")
    template = template_path.read_text(encoding="utf-8")
    replacements = {
        "cluster_arn": "arn:aws:ecs:us-east-1:123456789012:cluster/example",
        "task_definition_arn": "arn:aws:ecs:us-east-1:123456789012:task-definition/example:1",
        "container_name": "edgar-warehouse",
        "security_groups_json": '["sg-12345678"]',
        "subnets_json": '["subnet-12345678"]',
        "warehouse_command_expression": "States.Array('daily-incremental', '--run-id', $$.Execution.Name)",
        "warehouse_command_with_cik_list_expression": "States.Array('daily-incremental', '--run-id', $$.Execution.Name, '--cik-list', $.cik_list)",
    }
    for key, value in replacements.items():
        template = template.replace("${" + key + "}", value)
    return json.loads(template)


@pytest.mark.fast
def test_optional_cik_list_state_machine_has_default_and_override_paths():
    definition = _render_optional_cik_template()

    assert definition["StartAt"] == "HasCikListOverride"
    choice_state = definition["States"]["HasCikListOverride"]
    assert choice_state["Default"] == "RunWarehouseTaskDefault"
    assert choice_state["Choices"][0]["And"][0]["Variable"] == "$.cik_list"
    assert choice_state["Choices"][0]["Next"] == "RunWarehouseTaskWithCikList"

    default_command = definition["States"]["RunWarehouseTaskDefault"]["Parameters"]["Overrides"]["ContainerOverrides"][0]["Command.$"]
    override_command = definition["States"]["RunWarehouseTaskWithCikList"]["Parameters"]["Overrides"]["ContainerOverrides"][0]["Command.$"]

    assert default_command == "States.Array('daily-incremental', '--run-id', $$.Execution.Name)"
    assert override_command == "States.Array('daily-incremental', '--run-id', $$.Execution.Name, '--cik-list', $.cik_list)"
