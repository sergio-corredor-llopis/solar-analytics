"""
validate_parquet.py — Data quality validation for solar Parquet files.

Checks:
  1. File count: exactly 131 Parquet files
  2. Schema consistency: 108 columns in every file
  3. No empty files: all files have > 0 rows
  4. Timestamp integrity: dates fall within expected year/month
  5. Physical bounds: irradiance, temperature, power, voltage within limits

Critical failures (1-3): raise exception → stop pipeline
Quality warnings (4-5): log + report via XCom → pipeline continues

Bounds use envelope approach (widest range across all 13 systems).
Per-system validation with auxiliary files planned for dbt layer.
"""

import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

EXPECTED_FILES = 131
EXPECTED_COLUMNS = 107

# Envelope bounds: widest range across all 13 solar systems
# Source: auxiliary files (ISO standards + AEMET historical data for Madrid)
PHYSICAL_BOUNDS = {
    'G_H': {'min': 0, 'max': 1500, 'unit': 'W/m²',
            'desc': 'Global Horizontal Irradiance'},
    'G_M': {'min': 0, 'max': 1500, 'unit': 'W/m²',
            'desc': 'Module Plane Irradiance'},
    'T_U': {'min': -17.4, 'max': 50, 'unit': '°C',
            'desc': 'Ambient Temperature'},
    'T_M': {'min': -17.4, 'max': 70, 'unit': '°C',
            'desc': 'Module Temperature'},
    'T_WR': {'min': -17.4, 'max': 80, 'unit': '°C',
             'desc': 'Inverter Temperature'},
    'P_AC': {'min': 0, 'max': 5880, 'unit': 'W',
             'desc': 'AC Power'},
    'U_DC': {'min': 0, 'max': 548.5, 'unit': 'V',
             'desc': 'DC Voltage'},
    'U_AC': {'min': 0, 'max': 280, 'unit': 'V',
             'desc': 'AC Voltage'},
}


def validate_parquet_quality(input_dir='data/staging/monthly', **kwargs):
    """
    Main validation function. Called by Airflow PythonOperator.
    Returns results dict via XCom.
    """
    results = {
        'total_files': 0,
        'total_rows': 0,
        'critical_failures': [],
        'warnings': [],
        'files_validated': [],
    }

    base_path = Path(input_dir)
    parquet_files = sorted(base_path.rglob('*.parquet'))
    results['total_files'] = len(parquet_files)

    # ── CHECK 1: File count ──────────────────────────────────────
    if len(parquet_files) != EXPECTED_FILES:
        results['critical_failures'].append(
            f"File count mismatch: expected {EXPECTED_FILES}, "
            f"found {len(parquet_files)}"
        )
        logger.error(f"CHECK 1 FAILED: {results['critical_failures'][-1]}")
    else:
        logger.info(f"CHECK 1 PASSED: {len(parquet_files)} files found")

    # ── Process each file ────────────────────────────────────────
    for pf in parquet_files:
        # Extract year/month from partition path
        year = int(
            [p for p in pf.parts if p.startswith('year=')][0].split('=')[1]
        )
        month = int(
            [p for p in pf.parts if p.startswith('month=')][0].split('=')[1]
        )

        df = pd.read_parquet(pf)
        file_info = {
            'file': pf.name,
            'year': year,
            'month': month,
            'rows': len(df),
            'columns': len(df.columns),
        }

        # ── CHECK 2: Schema consistency ──────────────────────────
        if len(df.columns) != EXPECTED_COLUMNS:
            results['critical_failures'].append(
                f"{pf.name}: expected {EXPECTED_COLUMNS} columns, "
                f"found {len(df.columns)}"
            )

        # ── CHECK 3: Non-empty ───────────────────────────────────
        if len(df) == 0:
            results['critical_failures'].append(
                f"{pf.name}: empty file (0 rows)"
            )

        # ── CHECK 4: Timestamp integrity ─────────────────────────
        if 'timestamp' in df.columns and len(df) > 0:
            ts = pd.to_datetime(df['timestamp'], errors='coerce')
            valid_ts = ts.dropna()

            if len(valid_ts) > 0:
                wrong_year = (valid_ts.dt.year != year).sum()
                wrong_month = (valid_ts.dt.month != month).sum()

                # Meteocontrol exports include up to 2 rows
                # from the start of the next month — expected
                if wrong_month > 2 or wrong_year > 2:
                    results['warnings'].append(
                        f"{pf.name}: {wrong_year} timestamps wrong year, "
                        f"{wrong_month} wrong month (>2 = unexpected)"
                    )

            unparseable = ts.isna().sum() - df['timestamp'].isna().sum()
            if unparseable > 0:
                results['warnings'].append(
                    f"{pf.name}: {unparseable} unparseable timestamps"
                )

        # ── CHECK 5: Physical bounds ─────────────────────────────
        for col_prefix, bounds in PHYSICAL_BOUNDS.items():
            matching_cols = [
                c for c in df.columns if c.startswith(col_prefix)
            ]

            for col in matching_cols:
                numeric = pd.to_numeric(df[col], errors='coerce').dropna()

                if len(numeric) == 0:
                    continue

                below_min = 0
                above_max = 0

                if bounds['min'] is not None:
                    below_min = (numeric < bounds['min']).sum()
                if bounds['max'] is not None:
                    above_max = (numeric > bounds['max']).sum()

                if below_min > 0 or above_max > 0:
                    results['warnings'].append(
                        f"{pf.name} | {col} ({bounds['desc']}): "
                        f"{below_min} below {bounds['min']}"
                        f"{bounds['unit']}, "
                        f"{above_max} above {bounds['max']}"
                        f"{bounds['unit']}"
                    )

        results['total_rows'] += len(df)
        results['files_validated'].append(file_info)

    # ── Final Summary ────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("VALIDATION SUMMARY")
    logger.info(f"  Files validated: {results['total_files']}")
    logger.info(f"  Total rows: {results['total_rows']:,}")
    logger.info(f"  Critical failures: {len(results['critical_failures'])}")
    logger.info(f"  Warnings: {len(results['warnings'])}")
    logger.info("=" * 60)

    if results['critical_failures']:
        for failure in results['critical_failures']:
            logger.error(f"  CRITICAL: {failure}")
        raise ValueError(
            f"Validation failed: {len(results['critical_failures'])} "
            f"critical issue(s). Bad data will NOT be uploaded to S3."
        )

    if results['warnings']:
        logger.warning(
            f"  {len(results['warnings'])} quality warnings (non-blocking):"
        )
        for w in results['warnings']:
            logger.warning(f"    ⚠️ {w}")
    else:
        logger.info("  No warnings — data quality looks clean!")

    return results