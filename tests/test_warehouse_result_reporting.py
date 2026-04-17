from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tests.warehouse_result_helpers import report_duckdb_table, report_parquet_table


@pytest.mark.fast
def test_report_duckdb_table_returns_table_columns_and_row_count(capsys):
    conn = duckdb.connect(database=":memory:")
    conn.execute("CREATE TABLE sample_table (id INTEGER, company_name VARCHAR)")
    conn.execute("INSERT INTO sample_table VALUES (1, 'Apple Inc.'), (2, 'Microsoft Corp.')")

    summary = report_duckdb_table(conn, "sample_table")

    assert summary == {
        "table": "sample_table",
        "columns": ["id", "company_name"],
        "row_count": 2,
    }
    assert (
        capsys.readouterr().out.strip()
        == "TABLE sample_table | columns=id,company_name | row_count=2"
    )


@pytest.mark.fast
def test_report_parquet_table_returns_table_columns_and_row_count(tmp_path, capsys):
    parquet_path = Path(tmp_path) / "dim_company.parquet"
    pq.write_table(
        pa.table(
            {
                "company_key": pa.array([320193], type=pa.int64()),
                "entity_name": pa.array(["Apple Inc."], type=pa.string()),
            }
        ),
        parquet_path,
    )

    summary = report_parquet_table("dim_company", parquet_path)

    assert summary == {
        "table": "dim_company",
        "columns": ["company_key", "entity_name"],
        "row_count": 1,
    }
    assert (
        capsys.readouterr().out.strip()
        == "TABLE dim_company | columns=company_key,entity_name | row_count=1"
    )
