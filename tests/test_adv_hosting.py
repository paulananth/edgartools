import pytest

from edgar_warehouse.parsers.adv import parse_adv
from edgar_warehouse.silver import SilverDatabase
from tests.warehouse_result_helpers import report_duckdb_table

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
    return SilverDatabase(str(tmp_path / "silver.duckdb"))


@pytest.mark.fast
def test_parse_adv_returns_structured_rows():
    parsed = parse_adv("0001111111-26-000001", _ADV_XML, "ADV", 1111111)
    filing_rows = parsed["sec_adv_filing"]
    assert len(filing_rows) == 1
    assert filing_rows[0]["adviser_name"] == "Example Adviser LLC"
    assert filing_rows[0]["source_format"] == "xml"
    assert parsed["sec_adv_office"][0]["city"] == "New York"
    assert parsed["sec_adv_private_fund"][0]["fund_name"] == "Example Fund I"


@pytest.mark.fast
def test_parse_adv_rows_merge_into_silver(db):
    parsed = parse_adv("0001111111-26-000001", _ADV_XML, "ADV", 1111111)
    assert db.merge_adv_filings(parsed["sec_adv_filing"], "run-adv-001") == 1
    assert db.merge_adv_offices(parsed["sec_adv_office"], "run-adv-001") == 1
    assert db.merge_adv_disclosure_events(parsed["sec_adv_disclosure_event"], "run-adv-001") == 1
    assert db.merge_adv_private_funds(parsed["sec_adv_private_fund"], "run-adv-001") == 1

    row = db._conn.execute(
        "SELECT adviser_name, source_format FROM sec_adv_filing WHERE accession_number = ?",
        ["0001111111-26-000001"],
    ).fetchone()
    assert row == ("Example Adviser LLC", "xml")


@pytest.mark.fast
def test_adv_merge_reports_table_summaries(db):
    parsed = parse_adv("0001111111-26-000001", _ADV_XML, "ADV", 1111111)
    db.merge_adv_filings(parsed["sec_adv_filing"], "run-adv-001")
    db.merge_adv_offices(parsed["sec_adv_office"], "run-adv-001")
    db.merge_adv_disclosure_events(parsed["sec_adv_disclosure_event"], "run-adv-001")
    db.merge_adv_private_funds(parsed["sec_adv_private_fund"], "run-adv-001")

    summaries = {
        table_name: report_duckdb_table(db._conn, table_name)
        for table_name in [
            "sec_adv_filing",
            "sec_adv_office",
            "sec_adv_disclosure_event",
            "sec_adv_private_fund",
        ]
    }

    assert summaries["sec_adv_filing"]["row_count"] == 1
    assert summaries["sec_adv_office"]["row_count"] == 1
    assert summaries["sec_adv_disclosure_event"]["row_count"] == 1
    assert summaries["sec_adv_private_fund"]["row_count"] == 1
