{{ config(materialized='view') }}

/*
  int_readings_merged
  ─────────────────────────────────────────────────────────────────────
  Merges irradiance reconstruction back into the main data flow.

  Priority (COALESCE order):
    1. Original irr_cell from int_readings_cleaned
       (NULL during dead sensor periods — so reconstruction kicks in)
    2. Pyranometer regression (30° dead cell windows)
    3. Donor day substitution (5°/10° dead cell periods, Phase 5)
    4. NULL — gap too large or no donor selected yet

  irr_data_type tracks provenance for every row:
    'original'               — unmodified sensor reading
    'reconstructed_regression' — pyranometer regression
    'reconstructed_donor'    — donor day substitution
    NULL                     — irr_cell is NULL (nighttime, gap, no donor)
*/

WITH cleaned AS (
    SELECT * FROM {{ ref('int_readings_cleaned') }}
),

regression AS (
    SELECT * FROM {{ ref('int_irr_30deg_reconstructed') }}
),

donor AS (
    SELECT * FROM {{ ref('int_irr_donor_substituted') }}
)

SELECT
    c.reading_at,
    c.reading_at_raw,
    c.system_id,
    c.inclination_deg,
    c.interval_seconds,
    c.is_dst_corrected,
    c.source_file,

    -- Irradiance: reconstruction takes priority over original when flagged
    CASE
        WHEN c.is_irr_reconstruct_needed = TRUE AND r.irr_cell_reconstructed IS NOT NULL
            THEN r.irr_cell_reconstructed
        WHEN c.is_irr_reconstruct_needed = TRUE AND r.irr_cell_reconstructed IS NULL
            THEN NULL  -- sensor dead, no pyranometer available → NULL not dead value
        WHEN c.irr_cell IS NOT NULL
            THEN c.irr_cell
        WHEN d.irr_cell_donor IS NOT NULL
            THEN d.irr_cell_donor
        ELSE NULL
    END AS irr_cell,

    -- Pyranometer: pass through unchanged (no reconstruction needed)
    c.irr_pyranometer,

    -- Provenance
    CASE
        WHEN c.is_irr_reconstruct_needed = TRUE AND r.irr_cell_reconstructed IS NOT NULL
            THEN 'reconstructed_regression'
        WHEN c.is_irr_reconstruct_needed = TRUE AND r.irr_cell_reconstructed IS NULL
            THEN NULL  -- matches irr_cell = NULL above
        WHEN c.irr_cell IS NOT NULL
            THEN 'original'
        WHEN d.irr_cell_donor IS NOT NULL
            THEN 'reconstructed_donor'
        ELSE NULL
    END AS irr_data_type,

    -- All other columns pass through unchanged
    c.temp_module,
    c.temp_ambient,
    c.idc_a,
    c.vdc_v,
    c.pac_w,

    -- All flags from int_readings_cleaned pass through
    c.is_irr_manual_override,
    c.is_irr_reconstruct_needed,
    c.is_temp_manual_override,
    c.is_ambient_nullified,
    c.is_range_filtered

FROM cleaned c

LEFT JOIN regression r
    ON  c.system_id   = r.system_id
    AND c.reading_at  = r.reading_at

LEFT JOIN donor d
    ON  c.system_id   = d.system_id
    AND c.reading_at  = d.reading_at