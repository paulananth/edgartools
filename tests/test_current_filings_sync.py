import pytest

from edgar_warehouse.silver import SilverDatabase
from tests.warehouse_result_helpers import report_duckdb_table


@pytest.fixture
def db(tmp_path):
    return SilverDatabase(str(tmp_path / "silver.duckdb"))


@pytest.mark.fast
def test_merge_current_filing_feed_round_trip(db):
    rows = [
        {
            "accession_number": "0000320193-26-000001",
            "cik": 320193,
            "form": "8-K",
            "company_name": "Apple Inc.",
            "filing_date": "2026-04-15",
            "filing_href": "https://www.sec.gov/Archives/edgar/data/320193/000032019326000001.txt",
            "index_href": "https://www.sec.gov/Archives/edgar/data/320193/000032019326000001-index.html",
            "summary": "Current report",
            "source_url": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent",
            "raw_object_id": "feed-001",
        }
    ]
    count = db.merge_current_filing_feed(rows, sync_run_id="run-feed-001")
    assert count == 1

    result = db.get_current_filing_feed("0000320193-26-000001")
    assert result is not None
    assert result["company_name"] == "Apple Inc."
    assert result["form"] == "8-K"

    summary = report_duckdb_table(db._conn, "sec_current_filing_feed")

    assert summary["row_count"] == 1
