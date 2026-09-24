"""
Unit tests for src/verify_conversion.py's verify_all_parquet().

Uses synthetic Parquet fixtures (tmp_path) instead of the real 131-file
dataset, matching the approach in test_validate_parquet.py. The function
itself is print-heavy with no internal assertions (it's an interactive
spot-check script, not a library) -- these tests are the first automated
check on its return-value contract: one dict per file, correct row/column
aggregation.
"""
import pandas as pd

from src.verify_conversion import verify_all_parquet


def _write(base_dir, year, month, n_rows, n_cols):
    out_dir = base_dir / f"year={year}" / f"month={month:02d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    data = {f"col_{i}": list(range(n_rows)) for i in range(n_cols)}
    df = pd.DataFrame(data) if n_rows else pd.DataFrame({f"col_{i}": pd.Series(dtype="int64") for i in range(n_cols)})
    df.to_parquet(out_dir / f"solar_data_{year}_{month:02d}.parquet", index=False)


def test_verify_all_parquet_aggregates_rows_and_columns(tmp_path):
    _write(tmp_path, 2013, 2, n_rows=150, n_cols=5)
    _write(tmp_path, 2013, 3, n_rows=200, n_cols=5)
    results = verify_all_parquet(str(tmp_path))
    assert len(results) == 2
    assert sum(r["rows"] for r in results) == 350
    assert all(r["columns"] == 5 for r in results)


def test_verify_all_parquet_returns_one_entry_per_file_with_expected_fields(tmp_path):
    _write(tmp_path, 2013, 2, n_rows=10, n_cols=3)
    results = verify_all_parquet(str(tmp_path))
    assert results[0]["file"] == "solar_data_2013_02.parquet"
    assert results[0]["rows"] == 10
    assert results[0]["columns"] == 3
    assert set(results[0].keys()) == {"file", "rows", "columns", "size_mb", "path"}
