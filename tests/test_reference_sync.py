"""
Verify sec_tracked_universe seeding from company_tickers_exchange.json.

Fast tests only - no network calls. All SEC payloads are synthetic fixtures.

Ground-truth assertions from specification.md:
  AAPL -> CIK 320193
  MSFT -> CIK 789019
  NVDA -> CIK 1045810
  AAPL input_ticker -> "AAPL"
"""

import pytest
import duckdb
from edgar_warehouse.silver import SilverDatabase
from tests.warehouse_result_helpers import report_duckdb_table


_EXCHANGE_FIXTURE = {
    "0": {"cik_str": 320193, "ticker": "AAPL", "name": "Apple Inc.", "exchange": "Nasdaq"},
    "1": {"cik_str": 789019, "ticker": "MSFT", "name": "Microsoft Corp.", "exchange": "Nasdaq"},
    "2": {"cik_str": 1045810, "ticker": "NVDA", "name": "NVIDIA Corp", "exchange": "Nasdaq"},
    "3": {"cik_str": 1067983, "ticker": "BRK-B", "name": "Berkshire Hathaway Inc", "exchange": "NYSE"},
}


@pytest.fixture
def db(tmp_path):
    return SilverDatabase(str(tmp_path / "silver.duckdb"))


@pytest.mark.fast
def test_seed_tracked_universe_aapl_cik(db):
    db.seed_tracked_universe(_EXCHANGE_FIXTURE)
    row = db.get_tracked_universe_entry(320193)
    assert row is not None
    assert row["cik"] == 320193


@pytest.mark.fast
def test_seed_tracked_universe_msft_cik(db):
    db.seed_tracked_universe(_EXCHANGE_FIXTURE)
    row = db.get_tracked_universe_entry(789019)
    assert row is not None
    assert row["cik"] == 789019


@pytest.mark.fast
def test_seed_tracked_universe_nvda_cik(db):
    db.seed_tracked_universe(_EXCHANGE_FIXTURE)
    row = db.get_tracked_universe_entry(1045810)
    assert row is not None
    assert row["cik"] == 1045810


@pytest.mark.fast
def test_seed_tracked_universe_aapl_ticker(db):
    db.seed_tracked_universe(_EXCHANGE_FIXTURE)
    row = db.get_tracked_universe_entry(320193)
    assert row["input_ticker"] == "AAPL"


@pytest.mark.fast
def test_seed_tracked_universe_returns_inserted_count(db):
    count = db.seed_tracked_universe(_EXCHANGE_FIXTURE)
    assert count == 4


@pytest.mark.fast
def test_seed_tracked_universe_history_mode_is_recent_only(db):
    db.seed_tracked_universe(_EXCHANGE_FIXTURE)
    row = db.get_tracked_universe_entry(320193)
    assert row["history_mode"] == "recent_only"


@pytest.mark.fast
def test_seed_tracked_universe_tracking_status_is_active(db):
    db.seed_tracked_universe(_EXCHANGE_FIXTURE)
    row = db.get_tracked_universe_entry(320193)
    assert row["tracking_status"] == "active"


@pytest.mark.fast
def test_seed_tracked_universe_source_is_seeded_from_sec_reference(db):
    db.seed_tracked_universe(_EXCHANGE_FIXTURE)
    row = db.get_tracked_universe_entry(320193)
    assert row["universe_source"] == "seeded_from_sec_reference"


@pytest.mark.fast
def test_seed_tracked_universe_idempotent(db):
    db.seed_tracked_universe(_EXCHANGE_FIXTURE)
    db.seed_tracked_universe(_EXCHANGE_FIXTURE)
    count = db.get_tracked_universe_count()
    assert count == 4


@pytest.mark.fast
def test_seed_tracked_universe_new_entry_updates_existing_ticker(db):
    db.seed_tracked_universe(_EXCHANGE_FIXTURE)
    updated = dict(_EXCHANGE_FIXTURE)
    updated["0"] = {"cik_str": 320193, "ticker": "AAPL2", "name": "Apple Inc.", "exchange": "Nasdaq"}
    db.seed_tracked_universe(updated)
    row = db.get_tracked_universe_entry(320193)
    assert row["input_ticker"] == "AAPL2"


@pytest.mark.fast
def test_get_tracked_universe_ciks_returns_active_ciks(db):
    db.seed_tracked_universe(_EXCHANGE_FIXTURE)
    ciks = db.get_tracked_universe_ciks(status_filter="active")
    assert 320193 in ciks
    assert 789019 in ciks
    assert 1045810 in ciks
    assert 1067983 in ciks


@pytest.mark.fast
def test_get_tracked_universe_entry_missing_returns_none(db):
    db.seed_tracked_universe(_EXCHANGE_FIXTURE)
    assert db.get_tracked_universe_entry(9999999999) is None


@pytest.mark.fast
def test_seed_tracked_universe_reports_table_summary(db):
    db.seed_tracked_universe(_EXCHANGE_FIXTURE)

    summary = report_duckdb_table(db._conn, "sec_tracked_universe")

    assert summary["table"] == "sec_tracked_universe"
    assert summary["row_count"] == 4


@pytest.mark.fast
def test_seed_tracked_universe_migrates_legacy_table_columns(tmp_path):
    db_path = tmp_path / "silver.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE sec_tracked_universe (
            cik BIGINT PRIMARY KEY,
            input_ticker TEXT
        )
        """
    )
    conn.close()

    db = SilverDatabase(str(db_path))
    db.seed_tracked_universe(_EXCHANGE_FIXTURE)

    row = db.get_tracked_universe_entry(320193)
    assert row is not None
    assert row["current_ticker"] == "AAPL"
    assert row["tracking_status"] == "active"
