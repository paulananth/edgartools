from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

_TABLE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")


def _validate_table_name(table_name: str) -> str:
    if not _TABLE_NAME_RE.fullmatch(table_name):
        raise ValueError(f"Unsupported table name for summary reporting: {table_name!r}")
    return table_name


def format_table_summary(summary: dict[str, Any]) -> str:
    columns = ",".join(summary["columns"])
    return f"TABLE {summary['table']} | columns={columns} | row_count={summary['row_count']}"


def summarize_duckdb_table(conn: Any, table_name: str) -> dict[str, Any]:
    safe_table_name = _validate_table_name(table_name)
    pragma_name = safe_table_name.replace("'", "''")
    columns = [
        row[0]
        for row in conn.execute(
            f"SELECT name FROM pragma_table_info('{pragma_name}') ORDER BY cid"
        ).fetchall()
    ]
    if not columns:
        raise AssertionError(f"Could not summarize DuckDB table {table_name!r}: no columns found")
    row_count = int(conn.execute(f"SELECT COUNT(*) FROM {safe_table_name}").fetchone()[0])
    return {
        "table": table_name,
        "columns": columns,
        "row_count": row_count,
    }


def summarize_arrow_table(table_name: str, table: pa.Table) -> dict[str, Any]:
    return {
        "table": table_name,
        "columns": list(table.schema.names),
        "row_count": table.num_rows,
    }


def summarize_parquet_table(table_name: str, parquet_path: str | Path) -> dict[str, Any]:
    parquet_file = pq.ParquetFile(str(parquet_path))
    return {
        "table": table_name,
        "columns": list(parquet_file.schema_arrow.names),
        "row_count": parquet_file.metadata.num_rows,
    }


def report_duckdb_table(conn: Any, table_name: str) -> dict[str, Any]:
    summary = summarize_duckdb_table(conn, table_name)
    print(format_table_summary(summary))
    return summary


def report_arrow_table(table_name: str, table: pa.Table) -> dict[str, Any]:
    summary = summarize_arrow_table(table_name, table)
    print(format_table_summary(summary))
    return summary


def report_parquet_table(table_name: str, parquet_path: str | Path) -> dict[str, Any]:
    summary = summarize_parquet_table(table_name, parquet_path)
    print(format_table_summary(summary))
    return summary
