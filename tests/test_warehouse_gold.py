from __future__ import annotations

from pathlib import Path

import pytest

from edgar_warehouse.gold import build_gold, write_gold_to_snowflake_export, write_gold_to_storage
from edgar_warehouse.parsers.adv import parse_adv
from edgar_warehouse.parsers.ownership import parse_ownership
from edgar_warehouse.runtime import StorageLocation
from edgar_warehouse.silver import SilverDatabase
from tests.warehouse_result_helpers import report_arrow_table, report_parquet_table

_ADV_XML = """<?xml version="1.0" encoding="UTF-8"?>
<advFiling>
  <adviserName>Example Adviser LLC</adviserName>
  <secFileNumber>801-99999</secFileNumber>
  <crdNumber>123456</crdNumber>
  <effectiveDate>2026-04-10</effectiveDate>
  <filingStatus>Amendment</filingStatus>
  <office>
    <officeName>Principal Office</officeName>
    <city>New York</city>
    <stateOrCountry>NY</stateOrCountry>
    <country>US</country>
    <isHeadquarters>true</isHeadquarters>
  </office>
  <disclosureEvent>
    <disclosureCategory>regulatory</disclosureCategory>
    <eventDate>2026-03-31</eventDate>
    <description>Resolved regulatory matter</description>
  </disclosureEvent>
  <privateFund>
    <fundName>Example Fund I</fundName>
    <fundType>Hedge Fund</fundType>
    <jurisdiction>Delaware</jurisdiction>
    <aumAmount>1250000</aumAmount>
  </privateFund>
</advFiling>
"""


@pytest.fixture
def db(tmp_path):
    database = SilverDatabase(str(tmp_path / "silver.duckdb"))
    database.merge_company(
        [
            {
                "cik": 320193,
                "entity_name": "Apple Inc.",
                "entity_type": "operating",
                "sic": "3571",
                "sic_description": "Electronic Computers",
                "state_of_incorporation": "CA",
                "fiscal_year_end": "0930",
            }
        ],
        "gold-seed-run",
    )
    database.merge_filings(
        [
            {
                "accession_number": "0000320193-24-000123",
                "cik": 320193,
                "form": "10-K",
                "filing_date": "2024-11-01",
                "report_date": "2024-09-28",
                "is_xbrl": True,
                "size": 1000000,
            },
            {
                "accession_number": "0000320193-24-000100",
                "cik": 320193,
                "form": "10-Q",
                "filing_date": "2024-08-02",
                "report_date": "2024-06-29",
                "is_xbrl": True,
                "size": 800000,
            },
        ],
        "gold-seed-run",
    )
    return database


@pytest.fixture
def extended_db(db):
    ownership_accession = "0001213900-25-014145"
    adv_accession = "0001111111-26-000001"
    db.merge_company(
        [
            {
                "cik": 933972,
                "entity_name": "374Water Inc.",
                "entity_type": "operating",
            },
            {
                "cik": 1111111,
                "entity_name": "Example Adviser LLC",
                "entity_type": "operating",
            },
        ],
        "gold-extended-run",
    )
    db.merge_filings(
        [
            {
                "accession_number": ownership_accession,
                "cik": 933972,
                "form": "4",
                "filing_date": "2025-05-01",
                "report_date": "2025-04-30",
                "is_xbrl": False,
                "size": 4096,
            },
            {
                "accession_number": adv_accession,
                "cik": 1111111,
                "form": "ADV",
                "filing_date": "2026-04-10",
                "report_date": "2026-04-10",
                "is_xbrl": False,
                "size": 8192,
            },
        ],
        "gold-extended-run",
    )

    ownership_content = Path("data/ownership/374WaterForm4.xml").read_text(encoding="utf-8")
    ownership_parsed = parse_ownership(ownership_accession, ownership_content, "4")
    db.merge_ownership_reporting_owners(ownership_parsed["sec_ownership_reporting_owner"], "gold-extended-run")
    db.merge_ownership_non_derivative_txns(ownership_parsed["sec_ownership_non_derivative_txn"], "gold-extended-run")
    db.merge_ownership_derivative_txns(
        [
            {
                "accession_number": ownership_accession,
                "owner_index": 1,
                "txn_index": 1,
                "security_title": "Stock Option",
                "transaction_date": "2025-04-30",
                "transaction_code": "M",
                "transaction_shares": 500.0,
                "transaction_price": 1.5,
                "acquired_disposed_code": "A",
                "shares_owned_after": 500.0,
                "ownership_nature": "Direct",
                "ownership_direct_indirect": "D",
                "conversion_or_exercise_price": 1.5,
                "exercise_date": "2025-04-30",
                "expiration_date": "2030-04-30",
                "underlying_security_title": "Common Stock",
                "underlying_security_shares": 500.0,
                "parser_version": "1",
            }
        ],
        "gold-extended-run",
    )

    adv_parsed = parse_adv(adv_accession, _ADV_XML, "ADV", 1111111)
    db.merge_adv_filings(adv_parsed["sec_adv_filing"], "gold-extended-run")
    db.merge_adv_offices(adv_parsed["sec_adv_office"], "gold-extended-run")
    db.merge_adv_disclosure_events(adv_parsed["sec_adv_disclosure_event"], "gold-extended-run")
    db.merge_adv_private_funds(adv_parsed["sec_adv_private_fund"], "gold-extended-run")
    return db


@pytest.mark.fast
def test_build_gold_reports_baseline_table_summaries(db):
    tables = build_gold(db)

    summaries = {
        table_name: report_arrow_table(table_name, tables[table_name])
        for table_name in [
            "dim_company",
            "dim_form",
            "dim_date",
            "dim_filing",
            "fact_filing_activity",
        ]
    }

    assert summaries["dim_company"]["row_count"] == 1
    assert summaries["dim_form"]["row_count"] == 2
    assert summaries["dim_date"]["row_count"] >= 1
    assert summaries["dim_filing"]["row_count"] == 2
    assert summaries["fact_filing_activity"]["row_count"] == 2


@pytest.mark.fast
def test_gold_storage_and_export_report_parquet_summaries(db, tmp_path):
    tables = build_gold(db)
    storage_root = StorageLocation(str(tmp_path / "warehouse-root"))
    export_root = StorageLocation(str(tmp_path / "snowflake-export-root"))
    run_id = "gold-report-run"
    business_date = "2026-04-10"

    storage_counts = write_gold_to_storage(tables, storage_root, run_id)
    export_counts = write_gold_to_snowflake_export(tables, export_root, run_id, business_date)

    assert storage_counts["dim_company"] == 1
    assert storage_counts["fact_filing_activity"] == 2
    assert len(export_counts) == 8

    for table_name in ["dim_company", "dim_form", "dim_date", "dim_filing", "fact_filing_activity"]:
        parquet_path = Path(storage_root.join("gold", table_name, f"run_id={run_id}", f"{table_name}.parquet"))
        summary = report_parquet_table(table_name, parquet_path)
        assert summary["row_count"] == storage_counts[table_name]

    for export_name, expected_rows in export_counts.items():
        parquet_path = Path(
            export_root.join(
                export_name,
                f"business_date={business_date}",
                f"run_id={run_id}",
                f"{export_name}.parquet",
            )
        )
        summary = report_parquet_table(export_name, parquet_path)
        assert summary["row_count"] == expected_rows


@pytest.mark.fast
def test_build_gold_populates_ownership_and_adv_tables(extended_db):
    tables = build_gold(extended_db)

    summaries = {
        table_name: report_arrow_table(table_name, tables[table_name])
        for table_name in [
            "dim_party",
            "dim_security",
            "dim_ownership_txn_type",
            "dim_geography",
            "dim_disclosure_category",
            "dim_private_fund",
            "fact_ownership_transaction",
            "fact_ownership_holding_snapshot",
            "fact_adv_office",
            "fact_adv_disclosure",
            "fact_adv_private_fund",
        ]
    }

    assert summaries["dim_party"]["row_count"] == 2
    assert summaries["dim_security"]["row_count"] == 3
    assert summaries["dim_ownership_txn_type"]["row_count"] == 2
    assert summaries["dim_geography"]["row_count"] == 1
    assert summaries["dim_disclosure_category"]["row_count"] == 1
    assert summaries["dim_private_fund"]["row_count"] == 1
    assert summaries["fact_ownership_transaction"]["row_count"] == 2
    assert summaries["fact_ownership_holding_snapshot"]["row_count"] == 2
    assert summaries["fact_adv_office"]["row_count"] == 1
    assert summaries["fact_adv_disclosure"]["row_count"] == 1
    assert summaries["fact_adv_private_fund"]["row_count"] == 1

    party_rows = tables["dim_party"].to_pylist()
    txn_rows = tables["fact_ownership_transaction"].to_pylist()
    office_rows = tables["fact_adv_office"].to_pylist()
    fund_rows = tables["fact_adv_private_fund"].to_pylist()

    assert any(row["party_name"] == "Rajesh Ramaswamy Melkote" for row in party_rows)
    assert any(row["party_name"] == "Example Adviser LLC" for row in party_rows)
    assert any(row["transaction_code"] == "A" and row["transaction_shares"] == 757756.0 for row in txn_rows)
    assert any(row["transaction_code"] == "M" and row["is_derivative"] is True for row in txn_rows)
    assert office_rows[0]["office_name"] == "Principal Office"
    assert fund_rows[0]["aum_amount"] == 1250000.0


@pytest.mark.fast
def test_gold_exports_include_non_empty_ownership_and_adv_tables(extended_db, tmp_path):
    tables = build_gold(extended_db)
    storage_root = StorageLocation(str(tmp_path / "warehouse-root"))
    export_root = StorageLocation(str(tmp_path / "snowflake-export-root"))
    run_id = "gold-extended-run"
    business_date = "2026-04-10"

    storage_counts = write_gold_to_storage(tables, storage_root, run_id)
    export_counts = write_gold_to_snowflake_export(tables, export_root, run_id, business_date)

    assert storage_counts["fact_ownership_transaction"] == 2
    assert storage_counts["fact_ownership_holding_snapshot"] == 2
    assert storage_counts["fact_adv_office"] == 1
    assert storage_counts["fact_adv_disclosure"] == 1
    assert storage_counts["fact_adv_private_fund"] == 1
    assert export_counts["ownership_activity"] == 2
    assert export_counts["ownership_holdings"] == 2
    assert export_counts["adviser_offices"] == 1
    assert export_counts["adviser_disclosures"] == 1
    assert export_counts["private_funds"] == 1

    for table_name in [
        "fact_ownership_transaction",
        "fact_ownership_holding_snapshot",
        "fact_adv_office",
        "fact_adv_disclosure",
        "fact_adv_private_fund",
    ]:
        parquet_path = Path(storage_root.join("gold", table_name, f"run_id={run_id}", f"{table_name}.parquet"))
        summary = report_parquet_table(table_name, parquet_path)
        assert summary["row_count"] == storage_counts[table_name]

    for export_name in [
        "ownership_activity",
        "ownership_holdings",
        "adviser_offices",
        "adviser_disclosures",
        "private_funds",
    ]:
        parquet_path = Path(
            export_root.join(
                export_name,
                f"business_date={business_date}",
                f"run_id={run_id}",
                f"{export_name}.parquet",
            )
        )
        summary = report_parquet_table(export_name, parquet_path)
        assert summary["row_count"] == export_counts[export_name]
