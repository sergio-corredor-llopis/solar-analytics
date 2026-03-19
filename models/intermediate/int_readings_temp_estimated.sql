{{ config(materialized='view') }}

/*
  int_readings_temp_estimated
  ─────────────────────────────────────────────────────────────────────
  Estimates module temperature for systems without a sensor, or where
  the sensor was nullified, using the NOCT-based formula (IEC 61215):

    temp_module = temp_ambient + (TNOC - 20) × (irr_cell / 800)

  Only applied when:
    - temp_module IS NULL (no sensor, or sensor nullified in seed)
    - temp_ambient IS NOT NULL (formula requires ambient temp)
    - irr_cell IS NOT NULL (formula requires irradiance)

  temp_source tracks provenance:
    'measured'       — original sensor reading
    'interpolated'   — filled by int_readings_interpolated
    'estimated_tnoc' — TNOC estimation (this model)
*/

WITH interpolated AS (
    SELECT * FROM {{ ref('int_readings_interpolated') }}
),

metadata AS (
    SELECT system_id, tnoc_celsius
    FROM {{ ref('seed_system_metadata') }}
)

SELECT
    i.reading_at,
    i.reading_at_raw,
    i.system_id,
    i.inclination_deg,
    i.interval_seconds,
    i.is_dst_corrected,
    i.source_file,
    i.irr_cell,
    i.irr_pyranometer,
    i.irr_data_type,

    -- Module temp: estimate if NULL, keep original otherwise
    CASE
        WHEN i.temp_module IS NOT NULL
            THEN i.temp_module
        WHEN i.temp_ambient IS NOT NULL
            AND i.irr_cell IS NOT NULL
            THEN ROUND(
                i.temp_ambient + (m.tnoc_celsius - 20) * (i.irr_cell / 800.0),
                4)
        ELSE NULL
    END AS temp_module,

    -- Provenance for temp_module
    CASE
        WHEN i.temp_module IS NOT NULL
            THEN 'measured'
        WHEN i.temp_ambient IS NOT NULL
            AND i.irr_cell IS NOT NULL
            THEN 'estimated_tnoc'
        ELSE NULL
    END AS temp_source,

    i.temp_ambient,
    i.temp_ambient_source,
    i.idc_a,
    i.vdc_v,
    i.pac_w,
    i.sunrise_at,
    i.sunset_at,
    i.sun_method,
    i.is_irr_manual_override,
    i.is_irr_reconstruct_needed,
    i.is_temp_manual_override,
    i.is_ambient_nullified,
    i.is_range_filtered

FROM interpolated i
LEFT JOIN metadata m
    ON i.system_id = m.system_id