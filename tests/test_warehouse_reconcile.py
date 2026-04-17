import json
import shutil
import uuid
from pathlib import Path

import pytest

import edgar_warehouse.runtime as warehouse_runtime
from edgar_warehouse.cli import main
from edgar_warehouse.reconcile import build_reconcile_findings
from edgar_warehouse.silver import SilverDatabase
from tests.warehouse_result_helpers import report_duckdb_table


@pytest.fixture
def workspace_tmp_dir():
    base_dir = Path.cwd() / ".tmp-warehouse-tests"
    base_dir.mkdir(parents=True, exist_ok=True)
    temp_dir = base_dir / f"run-{uuid.uuid4().hex}"
    temp_dir.mkdir(parents=True, exist_ok=True)
    try:
        yield temp_dir
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.mark.fast
def test_sync_control_tables_exist(tmp_path):
    db = SilverDatabase(str(tmp_path / "silver.duckdb"))
    tables = {row[0] for row in db._conn.execute("SHOW TABLES").fetchall()}
    assert "sec_sync_run" in tables
    assert "sec_source_checkpoint" in tables
    assert "sec_company_sync_state" in tables
    assert "sec_reconcile_finding" in tables


@pytest.mark.fast
def test_build_reconcile_findings_detects_company_drift(tmp_path):
    db = SilverDatabase(str(tmp_path / "silver.duckdb"))
    db.merge_company(
        [{"cik": 320193, "entity_name": "Apple Inc.", "entity_type": "operating"}],
        "seed-run",
    )
    payload = {
        "name": "Apple Incorporated",
        "entityType": "operating",
        "filings": {"recent": {"accessionNumber": []}, "files": []},
    }
    findings = build_reconcile_findings(
        db=db,
        cik=320193,
        sync_run_id="reconcile-run-001",
        submissions_payload=payload,
    )
    assert any(finding["object_type"] == "company" for finding in findings)


@pytest.mark.fast
def test_targeted_resync_reference_updates_checkpoint(capsys, monkeypatch, workspace_tmp_dir):
    bronze_root = workspace_tmp_dir / "bronze-root" / "warehouse" / "bronze"
    warehouse_root = workspace_tmp_dir / "warehouse-root" / "warehouse"
    snowflake_export_root = workspace_tmp_dir / "snowflake-export-root"
    exchange_payload = b'{"fields":["cik","name","ticker","exchange"],"data":[[320193,"Apple Inc.","AAPL","Nasdaq"]]}'

    def fake_download(url: str, identity: str) -> bytes:
        assert identity == "Warehouse Test warehouse-test@example.com"
        if url.endswith("/files/company_tickers_exchange.json"):
            return exchange_payload
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setattr(warehouse_runtime, "_download_sec_bytes", fake_download)
    monkeypatch.setenv("EDGAR_IDENTITY", "Warehouse Test warehouse-test@example.com")
    monkeypatch.setenv("WAREHOUSE_RUNTIME_MODE", "bronze_capture")
    monkeypatch.setenv("WAREHOUSE_BRONZE_ROOT", str(bronze_root))
    monkeypatch.setenv("WAREHOUSE_STORAGE_ROOT", str(warehouse_root))
    monkeypatch.setenv("SNOWFLAKE_EXPORT_ROOT", str(snowflake_export_root))

    exit_code = main(
        [
            "targeted-resync",
            "--scope-type",
            "reference",
            "--scope-key",
            "company_tickers_exchange",
        ]
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "ok"

    db = SilverDatabase(str(warehouse_root / "silver" / "sec" / "silver.duckdb"))
    checkpoint = db.get_source_checkpoint("company_tickers_exchange", "global")
    assert checkpoint is not None
    sync_run = db.get_sync_run(payload["run_id"])
    assert sync_run is not None
    assert sync_run["status"] == "succeeded"

    checkpoint_summary = report_duckdb_table(db._conn, "sec_source_checkpoint")
    sync_run_summary = report_duckdb_table(db._conn, "sec_sync_run")

    assert checkpoint_summary["row_count"] >= 1
    assert sync_run_summary["row_count"] >= 1


@pytest.mark.fast
def test_sync_control_reporting_summaries(tmp_path):
    db = SilverDatabase(str(tmp_path / "silver.duckdb"))
    db.start_sync_run(
        {
            "sync_run_id": "sync-report-001",
            "sync_mode": "reconcile",
            "scope_type": "reference",
            "scope_key": "company_tickers_exchange",
        }
    )
    db.complete_sync_run("sync-report-001", status="succeeded", rows_inserted=1)
    db.upsert_source_checkpoint(
        {
            "source_name": "company_tickers_exchange",
            "source_key": "global",
            "raw_object_id": "raw-001",
        }
    )
    db.upsert_company_sync_state(
        {
            "cik": 320193,
            "tracking_status": "active",
            "pagination_files_expected": 1,
            "pagination_files_loaded": 1,
        }
    )
    db.insert_reconcile_findings(
        [
            {
                "reconcile_run_id": "sync-report-001",
                "cik": 320193,
                "scope_type": "cik",
                "object_type": "company",
                "object_key": "320193",
                "drift_type": "company_mismatch",
                "expected_value_hash": "abc",
                "actual_value_hash": "def",
                "severity": "high",
                "recommended_action": "cik_resync",
                "status": "detected",
                "detected_at": warehouse_runtime.datetime.now(warehouse_runtime.UTC),
                "resolved_at": None,
                "resync_run_id": None,
            }
        ]
    )

    sync_run_summary = report_duckdb_table(db._conn, "sec_sync_run")
    checkpoint_summary = report_duckdb_table(db._conn, "sec_source_checkpoint")
    state_summary = report_duckdb_table(db._conn, "sec_company_sync_state")
    finding_summary = report_duckdb_table(db._conn, "sec_reconcile_finding")

    assert sync_run_summary["row_count"] == 1
    assert checkpoint_summary["row_count"] == 1
    assert state_summary["row_count"] == 1
    assert finding_summary["row_count"] == 1
