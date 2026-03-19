{{ config(materialized='view') }}

/*
  int_ambient_temp_derived
  ─────────────────────────────────────────────────────────────────────
  Derives ambient temperature for the faulty sensor period (Feb 2013 –
  Mar 29 2016) using the reverse-TNOC formula:

    temp_ambient_est = temp_module - (tnoc_celsius - 20) × (irr_cell / 800)

  Calculated per eligible system at each timestamp, then averaged across
  all available systems for a robust estimate.

  Eligible system at a timestamp requires:
    - has_temp_sensor = TRUE (systems 2, 3, 8, 9, 10, 11)
    - temp_module IS NOT NULL (not manually overridden)
    - irr_cell IS NOT NULL AND irr_cell > 0 (formula requires irradiance)
    - is_ambient_nullified = TRUE (only derive during the faulty period)

  Output is a narrow lookup table: one row per reading_at with the derived
  ambient temp. int_readings_daytime joins this back into the main flow.
*/

WITH merged AS (
    SELECT * FROM {{ ref('int_readings_merged') }}
),

metadata AS (
    SELECT system_id, tnoc_celsius
    FROM {{ ref('seed_system_metadata') }}
    WHERE has_temp_sensor = TRUE
),

-- Per-system reverse-TNOC estimate at each timestamp
per_system_estimates AS (
    SELECT
        m.reading_at,
        m.system_id,
        m.temp_module,
        m.irr_cell,
        meta.tnoc_celsius,
        m.temp_module - (meta.tnoc_celsius - 20) * (m.irr_cell / 800.0)
            AS temp_ambient_est

    FROM merged m
    INNER JOIN metadata meta
        ON m.system_id = meta.system_id

    WHERE m.is_ambient_nullified = TRUE
      AND m.temp_module IS NOT NULL
      AND m.irr_cell IS NOT NULL
      AND m.irr_cell > 0
),

-- Average across all eligible systems per timestamp
averaged AS (
    SELECT
        reading_at,
        ROUND(AVG(temp_ambient_est), 4) AS temp_ambient_derived,
        COUNT(*)                         AS num_systems_used
    FROM per_system_estimates
    GROUP BY reading_at
)

SELECT
    reading_at,
    temp_ambient_derived,
    num_systems_used,
    'derived_reverse_tnoc' AS temp_ambient_source
FROM averaged