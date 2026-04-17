"""
Verify pagination file name extraction, filing count tracking, and silver
merge of SEC company_submission_file records.

Fast tests only - no network calls. All payloads are synthetic fixtures.

Ground-truth from specification.md:
  CIK0000320193-submissions-001.json has filing_count = 1219
"""

import json
import pytest

from edgar_warehouse.loaders import (
    stage_manifest_loader,
    stage_pagination_filing_loader,
    stage_recent_filing_loader,
)
from edgar_warehouse.silver import SilverDatabase
from tests.warehouse_result_helpers import report_duckdb_table


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_AAPL_CIK = 320193
_SYNC_RUN_ID = "run-pagination-001"
_RAW_OBJECT_ID = "raw-pagination-001"
_LOAD_MODE = "bootstrap_full"

# Main submissions JSON with known pagination file referencing 1219 filings
_AAPL_WITH_PAGINATION = {
    "cik": "0000320193",
    "name": "Apple Inc.",
    "filings": {
        "recent": {
            "accessionNumber": ["0000320193-24-000123"],
            "filingDate": ["2024-11-01"],
            "reportDate": ["2024-09-28"],
            "acceptanceDateTime": ["2024-11-01T18:05:01.000Z"],
            "act": ["34"],
            "form": ["10-K"],
            "fileNumber": ["001-36743"],
            "filmNumber": ["241439131"],
            "items": [""],
            "size": [1000000],
            "isXBRL": [1],
            "isInlineXBRL": [1],
            "primaryDocument": ["aapl-20240928.htm"],
            "primaryDocDescription": ["10-K"],
        },
        "files": [
            {
                "name": "CIK0000320193-submissions-001.json",
                "filingCount": 1219,
                "filingFrom": "1993-12-17",
                "filingTo": "2021-10-28",
            },
            {
                "name": "CIK0000320193-submissions-002.json",
                "filingCount": 200,
                "filingFrom": "1980-01-01",
                "filingTo": "1993-12-16",
            },
        ],
    },
}

# Synthetic pagination file payload (second page of AAPL filings)
_AAPL_PAGINATION_001 = {
    "cik": "0000320193",
    "filings": {
        "accessionNumber": [f"0000320193-21-{str(i).zfill(6)}" for i in range(1219)],
        "filingDate": ["2021-10-28"] * 1219,
        "reportDate": ["2021-09-25"] * 1219,
        "acceptanceDateTime": ["2021-10-28T18:00:00.000Z"] * 1219,
        "act": ["34"] * 1219,
        "form": ["10-K"] * 1219,
        "fileNumber": ["001-36743"] * 1219,
        "filmNumber": [""] * 1219,
        "items": [""] * 1219,
        "size": [500000] * 1219,
        "isXBRL": [1] * 1219,
        "isInlineXBRL": [1] * 1219,
        "primaryDocument": ["aapl-pagination.htm"] * 1219,
        "primaryDocDescription": ["10-K"] * 1219,
    },
}


@pytest.fixture
def db(tmp_path):
    return SilverDatabase(str(tmp_path / "silver.duckdb"))


# ---------------------------------------------------------------------------
# stage_manifest_loader tests with two pagination files
# ---------------------------------------------------------------------------


@pytest.mark.fast
def test_stage_manifest_loader_two_pagination_files():
    rows = stage_manifest_loader(
        _AAPL_WITH_PAGINATION, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE
    )
    assert len(rows) == 2


@pytest.mark.fast
def test_stage_manifest_loader_first_file_name():
    rows = stage_manifest_loader(
        _AAPL_WITH_PAGINATION, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE
    )
    names = [r["file_name"] for r in rows]
    assert "CIK0000320193-submissions-001.json" in names


@pytest.mark.fast
def test_stage_manifest_loader_filing_count_1219():
    """Ground-truth: CIK0000320193-submissions-001.json has filing_count = 1219."""
    rows = stage_manifest_loader(
        _AAPL_WITH_PAGINATION, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE
    )
    row = next(r for r in rows if r["file_name"] == "CIK0000320193-submissions-001.json")
    assert row["filing_count"] == 1219


@pytest.mark.fast
def test_stage_manifest_loader_cik_on_all_rows():
    rows = stage_manifest_loader(
        _AAPL_WITH_PAGINATION, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE
    )
    for row in rows:
        assert row["cik"] == _AAPL_CIK


@pytest.mark.fast
def test_stage_manifest_loader_empty_files_section():
    payload = {"cik": "0000320193", "filings": {"recent": {}, "files": []}}
    rows = stage_manifest_loader(payload, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE)
    assert rows == []


@pytest.mark.fast
def test_stage_manifest_loader_missing_filings_key():
    payload = {"cik": "0000320193"}
    rows = stage_manifest_loader(payload, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE)
    assert rows == []


# ---------------------------------------------------------------------------
# Pagination filing rows loader (pagination file as filings payload)
# ---------------------------------------------------------------------------


@pytest.mark.fast
def test_stage_pagination_filing_loader_uses_top_level_filings():
    """Pagination files have filings at the top level (not nested under .recent)."""
    rows = stage_pagination_filing_loader(
        _AAPL_PAGINATION_001, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE
    )
    assert len(rows) == 1219


@pytest.mark.fast
def test_stage_pagination_filing_loader_cik_on_all_rows():
    rows = stage_pagination_filing_loader(
        _AAPL_PAGINATION_001, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE
    )
    for row in rows:
        assert row["cik"] == _AAPL_CIK


# ---------------------------------------------------------------------------
# Silver merge: accession deduplication across recent + pagination
# ---------------------------------------------------------------------------


@pytest.mark.fast
def test_silver_merge_filings_deduplication(db):
    """Merging the same accession twice produces exactly one row."""
    recent_rows = stage_recent_filing_loader(
        _AAPL_WITH_PAGINATION, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE
    )
    db.merge_filings(recent_rows, _SYNC_RUN_ID)
    db.merge_filings(recent_rows, _SYNC_RUN_ID)
    assert db.get_filing_count(_AAPL_CIK) == 1


@pytest.mark.fast
def test_silver_merge_submission_files_filing_count_1219(db):
    rows = stage_manifest_loader(
        _AAPL_WITH_PAGINATION, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE
    )
    db.merge_submission_files(rows, _SYNC_RUN_ID)
    files = db.get_submission_files(_AAPL_CIK)
    pagination_001 = next(f for f in files if f["file_name"] == "CIK0000320193-submissions-001.json")
    assert pagination_001["filing_count"] == 1219


@pytest.mark.fast
def test_silver_merge_submission_files_total_count(db):
    rows = stage_manifest_loader(
        _AAPL_WITH_PAGINATION, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE
    )
    db.merge_submission_files(rows, _SYNC_RUN_ID)
    files = db.get_submission_files(_AAPL_CIK)
    assert len(files) == 2


@pytest.mark.fast
def test_silver_merge_pagination_filings_count(db):
    rows = stage_pagination_filing_loader(
        _AAPL_PAGINATION_001, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE
    )
    db.merge_filings(rows, _SYNC_RUN_ID)
    assert db.get_filing_count(_AAPL_CIK) == 1219


@pytest.mark.fast
def test_pagination_backfill_reports_table_summaries(db):
    db.merge_submission_files(
        stage_manifest_loader(
            _AAPL_WITH_PAGINATION, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE
        ),
        _SYNC_RUN_ID,
    )
    db.merge_filings(
        stage_pagination_filing_loader(
            _AAPL_PAGINATION_001, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE
        ),
        _SYNC_RUN_ID,
    )

    submission_summary = report_duckdb_table(db._conn, "sec_company_submission_file")
    filing_summary = report_duckdb_table(db._conn, "sec_company_filing")

    assert submission_summary["row_count"] == 2
    assert filing_summary["row_count"] == 1219
