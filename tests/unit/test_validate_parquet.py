"""
Unit tests for src/validate_parquet.py.

Two things are covered:
  1. PHYSICAL_BOUNDS static sanity — would catch a transposed min/max or a
     missing unit/desc key, the kind of typo that silently disables a bounds
     check without ever raising.
  2. validate_parquet_quality() end-to-end against synthetic Parquet
     fixtures written to tmp_path — exercises the real file-count,
     schema-consistency, non-empty, timestamp-integrity, and physical-bounds
     checks without needing the real 131-file / ~14.3M-row Meteocontrol
     dataset (not committed to this repo; see README "How to Run").

EXPECTED_FILES / EXPECTED_COLUMNS are monkeypatched per-test to match the
tiny fixture set — the validation LOGIC under test is identical to what runs
against the real dataset; only the size constants change.
"""
import pandas as pd
import pytest

from src import validate_parquet as vp


def _write_fixture(base_dir, year, month, columns: dict, filename=None):
    """Write one synthetic monthly Parquet file at the real partition layout
    (year=YYYY/month=MM/solar_data_YYYY_MM.parquet)."""
    out_dir = base_dir / f"year={year}" / f"month={month:02d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(columns)
    path = out_dir / (filename or f"solar_data_{year}_{month:02d}.parquet")
    df.to_parquet(path, index=False)
    return path


def test_physical_bounds_min_less_than_max():
    for key, bounds in vp.PHYSICAL_BOUNDS.items():
        if bounds["min"] is not None and bounds["max"] is not None:
            assert bounds["min"] < bounds["max"], key
        assert "unit" in bounds and "desc" in bounds, key


def test_validate_parquet_quality_raises_on_file_count_mismatch(tmp_path):
    # Only 1 file written; EXPECTED_FILES stays at the real value (131) --
    # this is exactly the June-2026-audited CHECK 1 scenario.
    _write_fixture(tmp_path, 2013, 2, {
        "timestamp": pd.date_range("2013-02-01", periods=3, freq="D"),
        "G_H": [100, 200, 300],
    })
    with pytest.raises(ValueError, match="critical issue"):
        vp.validate_parquet_quality(input_dir=str(tmp_path))


def test_validate_parquet_quality_flags_schema_mismatch_and_empty_file(tmp_path, monkeypatch):
    monkeypatch.setattr(vp, "EXPECTED_FILES", 2)
    monkeypatch.setattr(vp, "EXPECTED_COLUMNS", 2)
    _write_fixture(tmp_path, 2013, 2, {
        "timestamp": pd.date_range("2013-02-01", periods=3, freq="D"),
        "G_H": [100, 200, 300],
    })
    # Second file: wrong column count (3 vs EXPECTED_COLUMNS=2) AND empty --
    # both CHECK 2 (schema) and CHECK 3 (non-empty) should fire.
    _write_fixture(tmp_path, 2013, 3, {
        "timestamp": pd.Series([], dtype="datetime64[ns]"),
        "G_H": pd.Series([], dtype="float64"),
        "extra_col": pd.Series([], dtype="float64"),
    })
    with pytest.raises(ValueError, match="critical issue"):
        vp.validate_parquet_quality(input_dir=str(tmp_path))


def test_validate_parquet_quality_warns_on_out_of_bounds_reading(tmp_path, monkeypatch):
    monkeypatch.setattr(vp, "EXPECTED_FILES", 1)
    monkeypatch.setattr(vp, "EXPECTED_COLUMNS", 2)
    _write_fixture(tmp_path, 2013, 2, {
        "timestamp": pd.date_range("2013-02-01", periods=2, freq="D"),
        "G_H": [100, 5000],  # 5000 W/m^2 is far above the 1500 max bound
    })
    results = vp.validate_parquet_quality(input_dir=str(tmp_path))
    assert results["critical_failures"] == []
    assert any("G_H" in w for w in results["warnings"])


def test_validate_parquet_quality_clean_file_has_no_warnings(tmp_path, monkeypatch):
    monkeypatch.setattr(vp, "EXPECTED_FILES", 1)
    monkeypatch.setattr(vp, "EXPECTED_COLUMNS", 2)
    _write_fixture(tmp_path, 2013, 2, {
        "timestamp": pd.date_range("2013-02-01", periods=2, freq="D"),
        "G_H": [100, 200],
    })
    results = vp.validate_parquet_quality(input_dir=str(tmp_path))
    assert results["critical_failures"] == []
    assert results["warnings"] == []
    assert results["total_rows"] == 2
