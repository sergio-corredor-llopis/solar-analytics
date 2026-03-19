{{ config(materialized='table') }}

/*
  int_readings_complete
  ─────────────────────────────────────────────────────────────────────
  Final pre-calculation table. Adds two boolean flags:

  is_complete: all metrics needed for PR calculation are present.
    Required: irr_cell, temp_module, idc_a, vdc_v, pac_w
    (temp_ambient not required directly — it was needed upstream
     for TNOC estimation but PR formulas use temp_module directly)

  is_system_available: system was actively generating power.
    pac_w > 0 — same logic as Python code's "Sistema disponible"

  ALL downstream mart models filter to is_complete = TRUE.
  is_system_available drives availability metrics and loss decomposition.
*/

WITH temp_estimated AS (
    SELECT * FROM {{ ref('int_readings_temp_estimated') }}
)

SELECT
    reading_at,
    reading_at_raw,
    system_id,
    inclination_deg,
    interval_seconds,
    is_dst_corrected,
    source_file,
    irr_cell,
    irr_pyranometer,
    irr_data_type,
    temp_module,
    temp_source,
    temp_ambient,
    temp_ambient_source,
    idc_a,
    vdc_v,
    pac_w,
    sunrise_at,
    sunset_at,
    sun_method,
    is_irr_manual_override,
    is_irr_reconstruct_needed,
    is_temp_manual_override,
    is_ambient_nullified,
    is_range_filtered,

    -- Core completeness flag: all metrics needed for PR calculation present
    (
        irr_cell   IS NOT NULL
        AND temp_module IS NOT NULL
        AND idc_a  IS NOT NULL
        AND vdc_v  IS NOT NULL
        AND pac_w  IS NOT NULL
    ) AS is_complete,

    -- System availability flag: actively generating power
    COALESCE(pac_w > 0, FALSE) AS is_system_available

FROM temp_estimated