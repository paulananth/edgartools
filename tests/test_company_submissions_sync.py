"""
Verify sub-loaders and silver merge from CIK##########.json submissions payload.

Fast tests only - no network calls. All payloads are synthetic fixtures that
mirror the real SEC submissions JSON format.

Ground-truth assertions from specification.md / sec-hosting-verification-plan.md:
  AAPL name  -> "Apple Inc."
  AAPL sic   -> "3571"
  AAPL state -> "CA"
  AAPL former_names count -> 3
"""

import pytest
from edgar_warehouse.loaders import (
    stage_company_loader,
    stage_address_loader,
    stage_former_name_loader,
    stage_manifest_loader,
    stage_recent_filing_loader,
)
from edgar_warehouse.silver import SilverDatabase
from tests.warehouse_result_helpers import report_duckdb_table


# ---------------------------------------------------------------------------
# Synthetic AAPL-like fixture (all values match publicly known AAPL metadata)
# ---------------------------------------------------------------------------

_AAPL_CIK = 320193
_SYNC_RUN_ID = "run-test-001"
_RAW_OBJECT_ID = "raw-obj-001"
_LOAD_MODE = "bootstrap_recent_10"

_AAPL_PAYLOAD = {
    "cik": "0000320193",
    "entityType": "operating",
    "sic": "3571",
    "sicDescription": "Electronic Computers",
    "name": "Apple Inc.",
    "tickers": ["AAPL"],
    "exchanges": ["Nasdaq"],
    "stateOfIncorporation": "CA",
    "stateOfIncorporationDescription": "California",
    "fiscalYearEnd": "0930",
    "ein": "942404110",
    "description": "",
    "category": "Large accelerated filer",
    "addresses": {
        "mailing": {
            "street1": "One Apple Park Way",
            "street2": None,
            "city": "Cupertino",
            "stateOrCountry": "CA",
            "zipCode": "95014",
            "stateOrCountryDescription": "California",
        },
        "business": {
            "street1": "One Apple Park Way",
            "street2": None,
            "city": "Cupertino",
            "stateOrCountry": "CA",
            "zipCode": "95014",
            "stateOrCountryDescription": "California",
        },
    },
    "formerNames": [
        {"name": "APPLE COMPUTER INC", "date": "2007-01-10"},
        {"name": "APPLE COMPUTER CO", "date": "1987-02-10"},
        {"name": "APPLE COMPUTER INC", "date": "1977-05-26"},
    ],
    "filings": {
        "recent": {
            "accessionNumber": [
                "0000320193-24-000123",
                "0000320193-24-000100",
                "0000320193-23-000095",
            ],
            "filingDate": ["2024-11-01", "2024-08-02", "2024-05-03"],
            "reportDate": ["2024-09-28", "2024-06-29", "2024-03-30"],
            "acceptanceDateTime": [
                "2024-11-01T18:05:01.000Z",
                "2024-08-02T16:00:01.000Z",
                "2024-05-03T16:00:01.000Z",
            ],
            "act": ["34", "34", "34"],
            "form": ["10-K", "10-Q", "10-Q"],
            "fileNumber": ["001-36743", "001-36743", "001-36743"],
            "filmNumber": ["241439131", "241180994", "240793534"],
            "items": ["", "", ""],
            "size": [1000000, 800000, 750000],
            "isXBRL": [1, 1, 1],
            "isInlineXBRL": [1, 1, 1],
            "primaryDocument": ["aapl-20240928.htm", "aapl-20240629.htm", "aapl-20240330.htm"],
            "primaryDocDescription": ["10-K", "10-Q", "10-Q"],
        },
        "files": [
            {
                "name": "CIK0000320193-submissions-001.json",
                "filingCount": 1219,
                "filingFrom": "1993-12-17",
                "filingTo": "2021-10-28",
            }
        ],
    },
}


@pytest.fixture
def db(tmp_path):
    return SilverDatabase(str(tmp_path / "silver.duckdb"))


# ---------------------------------------------------------------------------
# stage_company_loader tests
# ---------------------------------------------------------------------------


@pytest.mark.fast
def test_stage_company_loader_returns_one_row():
    rows = stage_company_loader(_AAPL_PAYLOAD, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE)
    assert len(rows) == 1


@pytest.mark.fast
def test_stage_company_loader_entity_name():
    rows = stage_company_loader(_AAPL_PAYLOAD, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE)
    assert rows[0]["entity_name"] == "Apple Inc."


@pytest.mark.fast
def test_stage_company_loader_sic():
    rows = stage_company_loader(_AAPL_PAYLOAD, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE)
    assert rows[0]["sic"] == "3571"


@pytest.mark.fast
def test_stage_company_loader_entity_type():
    rows = stage_company_loader(_AAPL_PAYLOAD, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE)
    assert rows[0]["entity_type"] == "operating"


@pytest.mark.fast
def test_stage_company_loader_state_of_incorporation():
    rows = stage_company_loader(_AAPL_PAYLOAD, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE)
    assert rows[0]["state_of_incorporation"] == "CA"


@pytest.mark.fast
def test_stage_company_loader_cik():
    rows = stage_company_loader(_AAPL_PAYLOAD, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE)
    assert rows[0]["cik"] == _AAPL_CIK


# ---------------------------------------------------------------------------
# stage_address_loader tests
# ---------------------------------------------------------------------------


@pytest.mark.fast
def test_stage_address_loader_returns_two_rows():
    rows = stage_address_loader(_AAPL_PAYLOAD, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE)
    assert len(rows) == 2


@pytest.mark.fast
def test_stage_address_loader_address_types():
    rows = stage_address_loader(_AAPL_PAYLOAD, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE)
    types = {r["address_type"] for r in rows}
    assert types == {"mailing", "business"}


@pytest.mark.fast
def test_stage_address_loader_mailing_city():
    rows = stage_address_loader(_AAPL_PAYLOAD, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE)
    mailing = next(r for r in rows if r["address_type"] == "mailing")
    assert mailing["city"] == "Cupertino"


# ---------------------------------------------------------------------------
# stage_former_name_loader tests
# ---------------------------------------------------------------------------


@pytest.mark.fast
def test_stage_former_name_loader_returns_three_rows():
    rows = stage_former_name_loader(_AAPL_PAYLOAD, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE)
    assert len(rows) == 3


@pytest.mark.fast
def test_stage_former_name_loader_first_former_name():
    rows = stage_former_name_loader(_AAPL_PAYLOAD, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE)
    first = next(r for r in rows if r["ordinal"] == 1)
    assert first["former_name"] == "APPLE COMPUTER INC"


@pytest.mark.fast
def test_stage_former_name_loader_ordinals_are_one_based():
    rows = stage_former_name_loader(_AAPL_PAYLOAD, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE)
    ordinals = sorted(r["ordinal"] for r in rows)
    assert ordinals == [1, 2, 3]


# ---------------------------------------------------------------------------
# stage_manifest_loader tests
# ---------------------------------------------------------------------------


@pytest.mark.fast
def test_stage_manifest_loader_returns_one_row():
    rows = stage_manifest_loader(_AAPL_PAYLOAD, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE)
    assert len(rows) == 1


@pytest.mark.fast
def test_stage_manifest_loader_file_name():
    rows = stage_manifest_loader(_AAPL_PAYLOAD, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE)
    assert rows[0]["file_name"] == "CIK0000320193-submissions-001.json"


@pytest.mark.fast
def test_stage_manifest_loader_filing_count():
    rows = stage_manifest_loader(_AAPL_PAYLOAD, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE)
    assert rows[0]["filing_count"] == 1219


# ---------------------------------------------------------------------------
# stage_recent_filing_loader tests
# ---------------------------------------------------------------------------


@pytest.mark.fast
def test_stage_recent_filing_loader_returns_all_rows():
    rows = stage_recent_filing_loader(
        _AAPL_PAYLOAD, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE
    )
    assert len(rows) == 3


@pytest.mark.fast
def test_stage_recent_filing_loader_respects_recent_limit():
    rows = stage_recent_filing_loader(
        _AAPL_PAYLOAD, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE, recent_limit=2
    )
    assert len(rows) == 2


@pytest.mark.fast
def test_stage_recent_filing_loader_accession_number_format():
    rows = stage_recent_filing_loader(
        _AAPL_PAYLOAD, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE
    )
    assert rows[0]["accession_number"] == "0000320193-24-000123"


@pytest.mark.fast
def test_stage_recent_filing_loader_form():
    rows = stage_recent_filing_loader(
        _AAPL_PAYLOAD, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE
    )
    assert rows[0]["form"] == "10-K"


@pytest.mark.fast
def test_stage_recent_filing_loader_cik():
    rows = stage_recent_filing_loader(
        _AAPL_PAYLOAD, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE
    )
    for row in rows:
        assert row["cik"] == _AAPL_CIK


# ---------------------------------------------------------------------------
# Silver merge integration tests (sub-loaders -> silver.merge_*)
# ---------------------------------------------------------------------------


@pytest.mark.fast
def test_silver_merge_company_name(db):
    rows = stage_company_loader(_AAPL_PAYLOAD, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE)
    db.merge_company(rows, _SYNC_RUN_ID)
    company = db.get_company(_AAPL_CIK)
    assert company["entity_name"] == "Apple Inc."


@pytest.mark.fast
def test_silver_merge_company_sic(db):
    rows = stage_company_loader(_AAPL_PAYLOAD, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE)
    db.merge_company(rows, _SYNC_RUN_ID)
    company = db.get_company(_AAPL_CIK)
    assert company["sic"] == "3571"


@pytest.mark.fast
def test_silver_merge_company_state_of_incorporation(db):
    rows = stage_company_loader(_AAPL_PAYLOAD, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE)
    db.merge_company(rows, _SYNC_RUN_ID)
    company = db.get_company(_AAPL_CIK)
    assert company["state_of_incorporation"] == "CA"


@pytest.mark.fast
def test_silver_merge_former_names_count(db):
    rows = stage_former_name_loader(_AAPL_PAYLOAD, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE)
    db.merge_former_names(rows, _SYNC_RUN_ID)
    names = db.get_former_names(_AAPL_CIK)
    assert len(names) == 3


@pytest.mark.fast
def test_silver_merge_addresses_count(db):
    rows = stage_address_loader(_AAPL_PAYLOAD, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE)
    db.merge_addresses(rows, _SYNC_RUN_ID)
    addresses = db.get_addresses(_AAPL_CIK)
    assert len(addresses) == 2


@pytest.mark.fast
def test_silver_merge_submission_file_count(db):
    rows = stage_manifest_loader(_AAPL_PAYLOAD, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE)
    db.merge_submission_files(rows, _SYNC_RUN_ID)
    files = db.get_submission_files(_AAPL_CIK)
    assert len(files) == 1


@pytest.mark.fast
def test_silver_merge_submission_file_filing_count(db):
    rows = stage_manifest_loader(_AAPL_PAYLOAD, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE)
    db.merge_submission_files(rows, _SYNC_RUN_ID)
    files = db.get_submission_files(_AAPL_CIK)
    assert files[0]["filing_count"] == 1219


@pytest.mark.fast
def test_silver_merge_filings_count(db):
    rows = stage_recent_filing_loader(
        _AAPL_PAYLOAD, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE
    )
    db.merge_filings(rows, _SYNC_RUN_ID)
    assert db.get_filing_count(_AAPL_CIK) == 3


@pytest.mark.fast
def test_silver_merge_filings_idempotent(db):
    rows = stage_recent_filing_loader(
        _AAPL_PAYLOAD, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE
    )
    db.merge_filings(rows, _SYNC_RUN_ID)
    db.merge_filings(rows, _SYNC_RUN_ID)
    assert db.get_filing_count(_AAPL_CIK) == 3


@pytest.mark.fast
def test_silver_merge_filing_primary_document(db):
    rows = stage_recent_filing_loader(
        _AAPL_PAYLOAD, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE
    )
    db.merge_filings(rows, _SYNC_RUN_ID)
    filing = db.get_filing("0000320193-24-000123")
    assert filing["primary_document"] == "aapl-20240928.htm"


@pytest.mark.fast
def test_company_submission_merge_reports_table_summaries(db):
    db.merge_company(
        stage_company_loader(_AAPL_PAYLOAD, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE),
        _SYNC_RUN_ID,
    )
    db.merge_addresses(
        stage_address_loader(_AAPL_PAYLOAD, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE),
        _SYNC_RUN_ID,
    )
    db.merge_former_names(
        stage_former_name_loader(_AAPL_PAYLOAD, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE),
        _SYNC_RUN_ID,
    )
    db.merge_submission_files(
        stage_manifest_loader(_AAPL_PAYLOAD, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE),
        _SYNC_RUN_ID,
    )
    db.merge_filings(
        stage_recent_filing_loader(_AAPL_PAYLOAD, _AAPL_CIK, _SYNC_RUN_ID, _RAW_OBJECT_ID, _LOAD_MODE),
        _SYNC_RUN_ID,
    )

    summaries = {
        table_name: report_duckdb_table(db._conn, table_name)
        for table_name in [
            "sec_company",
            "sec_company_address",
            "sec_company_former_name",
            "sec_company_submission_file",
            "sec_company_filing",
        ]
    }

    assert summaries["sec_company"]["row_count"] == 1
    assert summaries["sec_company_address"]["row_count"] == 2
    assert summaries["sec_company_former_name"]["row_count"] == 3
    assert summaries["sec_company_submission_file"]["row_count"] == 1
    assert summaries["sec_company_filing"]["row_count"] == 3
