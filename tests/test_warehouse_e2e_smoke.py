from __future__ import annotations

import json
import shutil
import uuid
from pathlib import Path

import pytest

import edgar_warehouse.runtime as warehouse_runtime
from edgar_warehouse.cli import main
from edgar_warehouse.silver import SilverDatabase
from tests.warehouse_result_helpers import report_duckdb_table, report_parquet_table


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
def test_e2e_bronze_to_gold_reports_table_summaries(capsys, monkeypatch, workspace_tmp_dir):
    bronze_root = workspace_tmp_dir / "bronze-root" / "warehouse" / "bronze"
    warehouse_root = workspace_tmp_dir / "warehouse-root" / "warehouse"
    snowflake_export_root = workspace_tmp_dir / "snowflake-export-root"
    fetch_day_parts = warehouse_runtime.datetime.now(warehouse_runtime.UTC).date().strftime("%Y/%m/%d")
    company_tickers_payload = Path("data/company_tickers.json").read_bytes()
    exchange_payload = b'{"fields":["cik","name","ticker","exchange"],"data":[[320193,"Apple Inc.","AAPL","Nasdaq"]]}'
    daily_index_payload = (
        b"Description: Daily Index of EDGAR Dissemination Feed by Form Type\n"
        b"Last Data Received:    Apr 10, 2026\n"
        b"Comments:              webmaster@sec.gov\n"
        b"Anonymous FTP:         ftp://ftp.sec.gov/edgar/\n"
        b"\n\n\n\n"
        b"Form Type   Company Name                                                  CIK         Date Filed  File Name\n"
        b"---------------------------------------------------------------------------------------------------------------------------------------------\n"
        b"10-K             Apple Inc.                                                   320193     20260410    edgar/data/320193/0000320193-26-000001.txt\n"
    )
    submissions_payload = {
        "cik": "0000320193",
        "name": "Apple Inc.",
        "filings": {
            "recent": {
                "accessionNumber": ["0000320193-26-000001"],
                "filingDate": ["2026-04-10"],
                "reportDate": ["2026-03-28"],
                "acceptanceDateTime": ["2026-04-10T16:00:01.000Z"],
                "form": ["10-K"],
                "size": [1000000],
                "isXBRL": [1],
                "isInlineXBRL": [1],
                "primaryDocument": ["aapl-20260328.htm"],
                "primaryDocDescription": ["10-K"],
            },
            "files": [],
        },
    }

    def fake_download(url: str, identity: str) -> bytes:
        assert identity == "Warehouse Test warehouse-test@example.com"
        if url.endswith("/files/company_tickers.json"):
            return company_tickers_payload
        if url.endswith("/files/company_tickers_exchange.json"):
            return exchange_payload
        if url.endswith("/daily-index/2026/QTR2/form.20260410.idx"):
            return daily_index_payload
        if url.endswith("/submissions/CIK0000320193.json"):
            return json.dumps(submissions_payload).encode("utf-8")
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setattr(warehouse_runtime, "_download_sec_bytes", fake_download)
    monkeypatch.setenv("EDGAR_IDENTITY", "Warehouse Test warehouse-test@example.com")
    monkeypatch.setenv("WAREHOUSE_RUNTIME_MODE", "bronze_capture")
    monkeypatch.setenv("WAREHOUSE_BRONZE_CIK_LIMIT", "1")
    monkeypatch.setenv("WAREHOUSE_BRONZE_ROOT", str(bronze_root))
    monkeypatch.setenv("WAREHOUSE_STORAGE_ROOT", str(warehouse_root))
    monkeypatch.setenv("SNOWFLAKE_EXPORT_ROOT", str(snowflake_export_root))

    exit_code = main(
        [
            "daily-incremental",
            "--start-date",
            "2026-04-10",
            "--end-date",
            "2026-04-10",
            "--include-reference-refresh",
        ]
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "ok"
    assert payload["runtime_mode"] == "bronze_capture"
    assert payload["bronze_object_count"] == 4
    assert payload["snowflake_export_manifest"] is not None
    manifest_path = (
        snowflake_export_root
        / "manifests"
        / "workflow_name=daily_incremental"
        / "business_date=2026-04-10"
        / f"run_id={payload['run_id']}"
        / "run_manifest.json"
    )
    assert Path(payload["snowflake_export_manifest"]["path"]) == manifest_path
    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest_payload["environment"] == "local"
    assert manifest_payload["workflow_name"] == "daily_incremental"
    assert manifest_payload["tables"][0]["table_name"] == "COMPANY"

    db = SilverDatabase(str(warehouse_root / "silver" / "sec" / "silver.duckdb"))
    company_summary = report_duckdb_table(db._conn, "sec_company")
    filing_summary = report_duckdb_table(db._conn, "sec_company_filing")
    daily_index_summary = report_duckdb_table(db._conn, "stg_daily_index_filing")

    assert company_summary["row_count"] == 1
    assert filing_summary["row_count"] == 1
    assert daily_index_summary["row_count"] == 1

    dim_company_path = warehouse_root / "gold" / "dim_company" / f"run_id={payload['run_id']}" / "dim_company.parquet"
    fact_filing_activity_path = (
        warehouse_root / "gold" / "fact_filing_activity" / f"run_id={payload['run_id']}" / "fact_filing_activity.parquet"
    )
    company_export_path = (
        snowflake_export_root / "company" / "business_date=2026-04-10" / f"run_id={payload['run_id']}" / "company.parquet"
    )
    filing_activity_export_path = (
        snowflake_export_root
        / "filing_activity"
        / "business_date=2026-04-10"
        / f"run_id={payload['run_id']}"
        / "filing_activity.parquet"
    )

    dim_company_summary = report_parquet_table("dim_company", dim_company_path)
    fact_filing_activity_summary = report_parquet_table("fact_filing_activity", fact_filing_activity_path)
    company_export_summary = report_parquet_table("company", company_export_path)
    filing_activity_export_summary = report_parquet_table("filing_activity", filing_activity_export_path)

    assert dim_company_summary["row_count"] == 1
    assert fact_filing_activity_summary["row_count"] == 1
    assert company_export_summary["row_count"] == 1
    assert filing_activity_export_summary["row_count"] == 1
    assert any(
        item["relative_path"].endswith(
            f"submissions/sec/cik=320193/main/{fetch_day_parts}/CIK0000320193.json"
        )
        for item in payload["raw_writes"]
    )
