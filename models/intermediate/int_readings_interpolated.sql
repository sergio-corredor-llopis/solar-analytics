{{ config(materialized='view') }}

/*
  int_readings_interpolated
  ─────────────────────────────────────────────────────────────────────
  Linear interpolation for gaps <= 1 hour in key metrics.
  Gaps > 1 hour are left as NULL and excluded from calculations.

  Metrics interpolated: irr_cell, irr_pyranometer, temp_module,
                        temp_ambient, idc_a, vdc_v, pac_w

  IMPORTANT: temp_ambient interpolation happens here so that
  int_readings_temp_estimated can use interpolated ambient values
  for TNOC estimation on systems without temp sensors.

  BigQuery approach:
  - LAST_VALUE(IGNORE NULLS) finds the previous non-NULL value + timestamp
  - FIRST_VALUE(IGNORE NULLS) finds the next non-NULL value + timestamp
  - Gap duration = TIMESTAMP_DIFF(next_at, prev_at, SECOND)
  - Weight = position in gap / gap size
  - interpolated = prev + (next - prev) * weight
*/

WITH daytime AS (
    SELECT * FROM {{ ref('int_readings_daytime') }}
),

-- ─── FIND BOUNDING VALUES FOR EACH METRIC ────────────────────────────────────
-- One window pass per metric pair (value + timestamp).
-- ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW = look backwards
-- ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING = look forwards

bounded AS (
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
        temp_module,
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
        irr_data_type,

        -- IRR CELL
        LAST_VALUE(CASE WHEN irr_cell IS NOT NULL THEN irr_cell END IGNORE NULLS)
            OVER (PARTITION BY system_id ORDER BY reading_at
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)       AS irr_cell_prev,
        LAST_VALUE(CASE WHEN irr_cell IS NOT NULL THEN reading_at END IGNORE NULLS)
            OVER (PARTITION BY system_id ORDER BY reading_at
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)       AS irr_cell_prev_at,
        FIRST_VALUE(CASE WHEN irr_cell IS NOT NULL THEN irr_cell END IGNORE NULLS)
            OVER (PARTITION BY system_id ORDER BY reading_at
                  ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)       AS irr_cell_next,
        FIRST_VALUE(CASE WHEN irr_cell IS NOT NULL THEN reading_at END IGNORE NULLS)
            OVER (PARTITION BY system_id ORDER BY reading_at
                  ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)       AS irr_cell_next_at,

        -- IRR PYRANOMETER
        LAST_VALUE(CASE WHEN irr_pyranometer IS NOT NULL THEN irr_pyranometer END IGNORE NULLS)
            OVER (PARTITION BY system_id ORDER BY reading_at
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)       AS irr_pyr_prev,
        LAST_VALUE(CASE WHEN irr_pyranometer IS NOT NULL THEN reading_at END IGNORE NULLS)
            OVER (PARTITION BY system_id ORDER BY reading_at
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)       AS irr_pyr_prev_at,
        FIRST_VALUE(CASE WHEN irr_pyranometer IS NOT NULL THEN irr_pyranometer END IGNORE NULLS)
            OVER (PARTITION BY system_id ORDER BY reading_at
                  ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)       AS irr_pyr_next,
        FIRST_VALUE(CASE WHEN irr_pyranometer IS NOT NULL THEN reading_at END IGNORE NULLS)
            OVER (PARTITION BY system_id ORDER BY reading_at
                  ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)       AS irr_pyr_next_at,

        -- TEMP MODULE
        LAST_VALUE(CASE WHEN temp_module IS NOT NULL THEN temp_module END IGNORE NULLS)
            OVER (PARTITION BY system_id ORDER BY reading_at
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)       AS temp_mod_prev,
        LAST_VALUE(CASE WHEN temp_module IS NOT NULL THEN reading_at END IGNORE NULLS)
            OVER (PARTITION BY system_id ORDER BY reading_at
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)       AS temp_mod_prev_at,
        FIRST_VALUE(CASE WHEN temp_module IS NOT NULL THEN temp_module END IGNORE NULLS)
            OVER (PARTITION BY system_id ORDER BY reading_at
                  ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)       AS temp_mod_next,
        FIRST_VALUE(CASE WHEN temp_module IS NOT NULL THEN reading_at END IGNORE NULLS)
            OVER (PARTITION BY system_id ORDER BY reading_at
                  ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)       AS temp_mod_next_at,

        -- TEMP AMBIENT
        LAST_VALUE(CASE WHEN temp_ambient IS NOT NULL THEN temp_ambient END IGNORE NULLS)
            OVER (PARTITION BY system_id ORDER BY reading_at
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)       AS temp_amb_prev,
        LAST_VALUE(CASE WHEN temp_ambient IS NOT NULL THEN reading_at END IGNORE NULLS)
            OVER (PARTITION BY system_id ORDER BY reading_at
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)       AS temp_amb_prev_at,
        FIRST_VALUE(CASE WHEN temp_ambient IS NOT NULL THEN temp_ambient END IGNORE NULLS)
            OVER (PARTITION BY system_id ORDER BY reading_at
                  ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)       AS temp_amb_next,
        FIRST_VALUE(CASE WHEN temp_ambient IS NOT NULL THEN reading_at END IGNORE NULLS)
            OVER (PARTITION BY system_id ORDER BY reading_at
                  ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)       AS temp_amb_next_at,

        -- IDC
        LAST_VALUE(CASE WHEN idc_a IS NOT NULL THEN idc_a END IGNORE NULLS)
            OVER (PARTITION BY system_id ORDER BY reading_at
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)       AS idc_prev,
        LAST_VALUE(CASE WHEN idc_a IS NOT NULL THEN reading_at END IGNORE NULLS)
            OVER (PARTITION BY system_id ORDER BY reading_at
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)       AS idc_prev_at,
        FIRST_VALUE(CASE WHEN idc_a IS NOT NULL THEN idc_a END IGNORE NULLS)
            OVER (PARTITION BY system_id ORDER BY reading_at
                  ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)       AS idc_next,
        FIRST_VALUE(CASE WHEN idc_a IS NOT NULL THEN reading_at END IGNORE NULLS)
            OVER (PARTITION BY system_id ORDER BY reading_at
                  ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)       AS idc_next_at,

        -- VDC
        LAST_VALUE(CASE WHEN vdc_v IS NOT NULL THEN vdc_v END IGNORE NULLS)
            OVER (PARTITION BY system_id ORDER BY reading_at
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)       AS vdc_prev,
        LAST_VALUE(CASE WHEN vdc_v IS NOT NULL THEN reading_at END IGNORE NULLS)
            OVER (PARTITION BY system_id ORDER BY reading_at
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)       AS vdc_prev_at,
        FIRST_VALUE(CASE WHEN vdc_v IS NOT NULL THEN vdc_v END IGNORE NULLS)
            OVER (PARTITION BY system_id ORDER BY reading_at
                  ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)       AS vdc_next,
        FIRST_VALUE(CASE WHEN vdc_v IS NOT NULL THEN reading_at END IGNORE NULLS)
            OVER (PARTITION BY system_id ORDER BY reading_at
                  ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)       AS vdc_next_at,

        -- PAC
        LAST_VALUE(CASE WHEN pac_w IS NOT NULL THEN pac_w END IGNORE NULLS)
            OVER (PARTITION BY system_id ORDER BY reading_at
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)       AS pac_prev,
        LAST_VALUE(CASE WHEN pac_w IS NOT NULL THEN reading_at END IGNORE NULLS)
            OVER (PARTITION BY system_id ORDER BY reading_at
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)       AS pac_prev_at,
        FIRST_VALUE(CASE WHEN pac_w IS NOT NULL THEN pac_w END IGNORE NULLS)
            OVER (PARTITION BY system_id ORDER BY reading_at
                  ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)       AS pac_next,
        FIRST_VALUE(CASE WHEN pac_w IS NOT NULL THEN reading_at END IGNORE NULLS)
            OVER (PARTITION BY system_id ORDER BY reading_at
                  ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)       AS pac_next_at

    FROM daytime
),

-- ─── CALCULATE GAP DURATIONS AND INTERPOLATION WEIGHTS ───────────────────────
with_weights AS (
    SELECT
        *,
        TIMESTAMP_DIFF(irr_cell_next_at, irr_cell_prev_at, SECOND)  AS irr_cell_gap_s,
        TIMESTAMP_DIFF(reading_at,       irr_cell_prev_at, SECOND)  AS irr_cell_pos_s,
        TIMESTAMP_DIFF(irr_pyr_next_at,  irr_pyr_prev_at,  SECOND)  AS irr_pyr_gap_s,
        TIMESTAMP_DIFF(reading_at,       irr_pyr_prev_at,  SECOND)  AS irr_pyr_pos_s,
        TIMESTAMP_DIFF(temp_mod_next_at, temp_mod_prev_at, SECOND)  AS temp_mod_gap_s,
        TIMESTAMP_DIFF(reading_at,       temp_mod_prev_at, SECOND)  AS temp_mod_pos_s,
        TIMESTAMP_DIFF(temp_amb_next_at, temp_amb_prev_at, SECOND)  AS temp_amb_gap_s,
        TIMESTAMP_DIFF(reading_at,       temp_amb_prev_at, SECOND)  AS temp_amb_pos_s,
        TIMESTAMP_DIFF(idc_next_at,      idc_prev_at,      SECOND)  AS idc_gap_s,
        TIMESTAMP_DIFF(reading_at,       idc_prev_at,      SECOND)  AS idc_pos_s,
        TIMESTAMP_DIFF(vdc_next_at,      vdc_prev_at,      SECOND)  AS vdc_gap_s,
        TIMESTAMP_DIFF(reading_at,       vdc_prev_at,      SECOND)  AS vdc_pos_s,
        TIMESTAMP_DIFF(pac_next_at,      pac_prev_at,      SECOND)  AS pac_gap_s,
        TIMESTAMP_DIFF(reading_at,       pac_prev_at,      SECOND)  AS pac_pos_s
    FROM bounded
)

-- ─── APPLY INTERPOLATION ─────────────────────────────────────────────────────
SELECT
    reading_at,
    reading_at_raw,
    system_id,
    inclination_deg,
    interval_seconds,
    is_dst_corrected,
    source_file,
    sunrise_at,
    sunset_at,
    sun_method,
    is_irr_manual_override,
    is_irr_reconstruct_needed,
    is_temp_manual_override,
    is_ambient_nullified,
    is_range_filtered,

    -- IRR CELL
    CASE
        WHEN irr_cell IS NOT NULL THEN irr_cell
        WHEN irr_cell_gap_s <= 3600 AND irr_cell_prev IS NOT NULL AND irr_cell_next IS NOT NULL
            THEN ROUND(irr_cell_prev + (irr_cell_next - irr_cell_prev)
                       * SAFE_DIVIDE(irr_cell_pos_s, irr_cell_gap_s), 4)
        ELSE NULL
    END AS irr_cell,

    -- IRR PYRANOMETER
    CASE
        WHEN irr_pyranometer IS NOT NULL THEN irr_pyranometer
        WHEN irr_pyr_gap_s <= 3600 AND irr_pyr_prev IS NOT NULL AND irr_pyr_next IS NOT NULL
            THEN ROUND(irr_pyr_prev + (irr_pyr_next - irr_pyr_prev)
                       * SAFE_DIVIDE(irr_pyr_pos_s, irr_pyr_gap_s), 4)
        ELSE NULL
    END AS irr_pyranometer,

    -- TEMP MODULE
    CASE
        WHEN temp_module IS NOT NULL THEN temp_module
        WHEN temp_mod_gap_s <= 3600 AND temp_mod_prev IS NOT NULL AND temp_mod_next IS NOT NULL
            THEN ROUND(temp_mod_prev + (temp_mod_next - temp_mod_prev)
                       * SAFE_DIVIDE(temp_mod_pos_s, temp_mod_gap_s), 4)
        ELSE NULL
    END AS temp_module,

    -- TEMP AMBIENT
    CASE
        WHEN temp_ambient IS NOT NULL THEN temp_ambient
        WHEN temp_amb_gap_s <= 3600 AND temp_amb_prev IS NOT NULL AND temp_amb_next IS NOT NULL
            THEN ROUND(temp_amb_prev + (temp_amb_next - temp_amb_prev)
                       * SAFE_DIVIDE(temp_amb_pos_s, temp_amb_gap_s), 4)
        ELSE NULL
    END AS temp_ambient,

    -- Keep ambient source, mark interpolated rows
    CASE
        WHEN temp_ambient IS NOT NULL
            THEN temp_ambient_source
        WHEN temp_amb_gap_s <= 3600 AND temp_amb_prev IS NOT NULL AND temp_amb_next IS NOT NULL
            THEN 'interpolated'
        ELSE temp_ambient_source
    END AS temp_ambient_source,

    -- IDC
    CASE
        WHEN idc_a IS NOT NULL THEN idc_a
        WHEN idc_gap_s <= 3600 AND idc_prev IS NOT NULL AND idc_next IS NOT NULL
            THEN ROUND(idc_prev + (idc_next - idc_prev)
                       * SAFE_DIVIDE(idc_pos_s, idc_gap_s), 4)
        ELSE NULL
    END AS idc_a,

    -- VDC
    CASE
        WHEN vdc_v IS NOT NULL THEN vdc_v
        WHEN vdc_gap_s <= 3600 AND vdc_prev IS NOT NULL AND vdc_next IS NOT NULL
            THEN ROUND(vdc_prev + (vdc_next - vdc_prev)
                       * SAFE_DIVIDE(vdc_pos_s, vdc_gap_s), 4)
        ELSE NULL
    END AS vdc_v,

    -- PAC
    CASE
        WHEN pac_w IS NOT NULL THEN pac_w
        WHEN pac_gap_s <= 3600 AND pac_prev IS NOT NULL AND pac_next IS NOT NULL
            THEN ROUND(pac_prev + (pac_next - pac_prev)
                       * SAFE_DIVIDE(pac_pos_s, pac_gap_s), 4)
        ELSE NULL
    END AS pac_w,

    -- IRR DATA TYPE: preserve original, mark interpolated
    CASE
        WHEN irr_cell IS NOT NULL
            THEN irr_data_type
        WHEN irr_cell_gap_s <= 3600 AND irr_cell_prev IS NOT NULL AND irr_cell_next IS NOT NULL
            THEN 'interpolated'
        ELSE irr_data_type
    END AS irr_data_type

FROM with_weights