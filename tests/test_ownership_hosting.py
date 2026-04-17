import json
import shutil
import tempfile
import uuid
from pathlib import Path

import pytest

import edgar_warehouse.runtime as warehouse_runtime
from edgar_warehouse.cli import main
from edgar_warehouse.parsers.ownership import parse_ownership
from edgar_warehouse.silver import SilverDatabase
from tests.warehouse_result_helpers import report_duckdb_table


@pytest.fixture
def workspace_tmp_dir():
    base_dir = Path(tempfile.gettempdir()) / "ew-warehouse-tests"
    base_dir.mkdir(parents=True, exist_ok=True)
    temp_dir = base_dir / f"run-{uuid.uuid4().hex}"
    temp_dir.mkdir(parents=True, exist_ok=True)
    try:
        yield temp_dir
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.mark.fast
def test_parse_ownership_fixture_produces_owner_and_transaction_rows():
    content = Path("data/ownership/374WaterForm4.xml").read_text(encoding="utf-8")
    parsed = parse_ownership("0001213900-23-014145", content, "4")
    assert parsed["sec_ownership_reporting_owner"][0]["owner_name"] == "Rajesh Ramaswamy Melkote"
    assert len(parsed["sec_ownership_non_derivative_txn"]) >= 1


@pytest.mark.fast
def test_targeted_resync_accession_builds_artifacts_text_and_ownership_rows(
    capsys,
    monkeypatch,
    workspace_tmp_dir,
):
    bronze_root = workspace_tmp_dir / "bronze-root" / "warehouse" / "bronze"
    warehouse_root = workspace_tmp_dir / "warehouse-root" / "warehouse"
    snowflake_export_root = workspace_tmp_dir / "snowflake-export-root"
    accession_number = "0000933972-25-000001"
    cik = 933972
    accession_digits = accession_number.replace("-", "")
    index_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_digits}/{accession_digits}-index.html"
    document_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_digits}/form4.xml"
    ownership_xml = Path("data/ownership/374WaterForm4.xml").read_bytes()
    index_html = (
        "<html><body><table>"
        "<tr><td>1</td><td>Primary</td><td><a href=\"form4.xml\">form4.xml</a></td><td>4</td></tr>"
        "</table></body></html>"
    ).encode("utf-8")

    def fake_download(url: str, identity: str) -> bytes:
        assert identity == "Warehouse Test warehouse-test@example.com"
        if url == index_url:
            return index_html
        if url == document_url:
            return ownership_xml
        raise AssertionError(f"Unexpected URL {url}")

    db_path = warehouse_root / "silver" / "sec" / "silver.duckdb"
    db = SilverDatabase(str(db_path))
    db.merge_filings(
        [
            {
                "accession_number": accession_number,
                "cik": cik,
                "form": "4",
                "filing_date": "2023-02-15",
                "primary_document": "form4.xml",
                "primary_doc_desc": "Ownership document",
            }
        ],
        "seed-run",
    )
    db.close()

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
            "accession",
            "--scope-key",
            accession_number,
        ]
    )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "ok"

    db = SilverDatabase(str(db_path))
    attachments = db.get_filing_attachments(accession_number)
    assert len(attachments) == 1
    text_row = db.get_filing_text(accession_number, "generic_text_v1")
    assert text_row is not None
    owner_count = db._conn.execute(
        "SELECT COUNT(*) FROM sec_ownership_reporting_owner WHERE accession_number = ?",
        [accession_number],
    ).fetchone()[0]
    txn_count = db._conn.execute(
        "SELECT COUNT(*) FROM sec_ownership_non_derivative_txn WHERE accession_number = ?",
        [accession_number],
    ).fetchone()[0]
    assert owner_count >= 1
    assert txn_count >= 1

    attachment_summary = report_duckdb_table(db._conn, "sec_filing_attachment")
    text_summary = report_duckdb_table(db._conn, "sec_filing_text")
    owner_summary = report_duckdb_table(db._conn, "sec_ownership_reporting_owner")
    txn_summary = report_duckdb_table(db._conn, "sec_ownership_non_derivative_txn")
    parse_summary = report_duckdb_table(db._conn, "sec_parse_run")

    assert attachment_summary["row_count"] == 1
    assert text_summary["row_count"] == 1
    assert owner_summary["row_count"] >= 1
    assert txn_summary["row_count"] >= 1
    assert parse_summary["row_count"] >= 1
