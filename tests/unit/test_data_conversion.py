"""
Unit tests for src/data_conversion.py's filename-parsing logic.

Scope: parse_filename() only. load_solar_csv() / csv_to_parquet() /
convert_all_files() are NOT covered here — they require the real Meteocontrol
source CSVs (UTF-16LE, tab-separated, Spanish-locale decimals), which are not
committed to this repo (raw data lives outside git; see README "How to Run").
tests/unit/test_validate_parquet.py and tests/unit/test_verify_conversion.py
cover the I/O-level logic instead, using synthetic Parquet fixtures.
"""
from src.data_conversion import parse_filename, SPANISH_MONTHS


def test_parse_filename_valid():
    result = parse_filename("2013 02 Febrero Todos los Inversores.csv")
    assert result == {"year": "2013", "month": "02", "month_name": "Febrero"}


def test_parse_filename_all_spanish_months_resolve():
    # Every month name the regex could capture from a real filename must
    # also exist as a SPANISH_MONTHS key — catches a future typo in either
    # table before it silently drops a month's data.
    for month_name in SPANISH_MONTHS:
        filename = f"2020 01 {month_name} Todos los Inversores.csv"
        result = parse_filename(filename)
        assert result is not None, filename
        assert result["month_name"] == month_name


def test_parse_filename_rejects_wrong_suffix():
    # Missing the literal "Todos los Inversores" suffix must not match --
    # a silent partial match here would misfile a CSV into the wrong
    # year/month partition instead of raising.
    assert parse_filename("2013 02 Febrero.csv") is None


def test_parse_filename_rejects_non_csv():
    assert parse_filename("2013 02 Febrero Todos los Inversores.txt") is None


def test_parse_filename_tolerates_extra_whitespace():
    # The source regex uses \s+ between the year/month/month-name groups,
    # so an inconsistently-exported filename with doubled spaces should
    # still parse correctly rather than silently failing to match.
    result = parse_filename("2013  02  Febrero  Todos los Inversores.csv")
    assert result == {"year": "2013", "month": "02", "month_name": "Febrero"}


def test_parse_filename_rejects_malformed_year():
    # A 3-digit year must not match the \d{4} group.
    assert parse_filename("201 02 Febrero Todos los Inversores.csv") is None
