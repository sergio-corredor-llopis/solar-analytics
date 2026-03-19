{{ config(materialized='table') }}

/*
  int_readings_cleaned
  ─────────────────────────────────────────────────────────────────────
  Layer 1 — Manual overrides (seed_manual_overrides):
    - "nullify"     → set column to NULL, set override flag
    - "reconstruct" → keep value, set is_irr_reconstruct_needed flag
                      (replacement happens in int_irr_30deg_reconstructed)

  Layer 2 — Per-system range bounds (seed_per_system_bounds):
    - Any value outside [min_value, max_value] → NULL, set is_range_filtered flag

  No rows are dropped. All 14.3M rows pass through with flags attached.

  SEED METRIC / SYSTEM_ID CONVENTIONS (from seed_manual_overrides.csv):
    irr_cell_30 / irr_cell_10 / irr_cell_5 → system_id = 'all'
    temp_module                             → system_id = '2', '10', or 'all_temp_sensors'
    temp_ambient                            → system_id = 'all'
*/

WITH base AS (
    SELECT * FROM {{ ref('int_readings_unpivoted') }}
),

-- ─── OVERRIDE CTEs ────────────────────────────────────────────────────────────

-- Irradiance: translate metric name → inclination_deg for the join
irr_cell_overrides AS (
    SELECT
        CAST(start_at AS TIMESTAMP) AS start_at,
        CAST(end_at   AS TIMESTAMP) AS end_at,
        override_type,
        CASE metric
            WHEN 'irr_cell_30' THEN 30
            WHEN 'irr_cell_10' THEN 10
            WHEN 'irr_cell_5'  THEN 5
        END AS inclination_deg
    FROM {{ ref('seed_manual_overrides') }}
    WHERE metric IN ('irr_cell_30', 'irr_cell_10', 'irr_cell_5')
),

-- Ambient: single shared sensor
ambient_overrides AS (
    SELECT
        CAST(start_at AS TIMESTAMP) AS start_at,
        CAST(end_at   AS TIMESTAMP) AS end_at,
        override_type
    FROM {{ ref('seed_manual_overrides') }}
    WHERE metric = 'temp_ambient'
),

-- Module temp: system_id is either a numeric string ('2', '10')
--              or 'all_temp_sensors' for simultaneous multi-system failures.
-- We parse both cases. is_all_systems drives a looser join condition below.
temp_mod_overrides AS (
    SELECT
        CAST(start_at AS TIMESTAMP) AS start_at,
        CAST(end_at   AS TIMESTAMP) AS end_at,
        override_type,
        CASE
            WHEN system_id = 'all_temp_sensors' THEN NULL           -- joins to all systems
            ELSE SAFE_CAST(system_id AS INT64)                      -- joins to one system
        END AS system_id_num,
        (system_id = 'all_temp_sensors') AS is_all_systems
    FROM {{ ref('seed_manual_overrides') }}
    WHERE metric = 'temp_module'
),

-- ─── BOUNDS CTE ───────────────────────────────────────────────────────────────
-- Pivot long → wide (one row per system_id).
-- NULL min/max = no bound on that side. BigQuery NULL comparisons handle this:
-- (value < NULL) → NULL → falsy in CASE WHEN, so no unintended filtering.

bounds AS (
    SELECT
        system_id,
        MAX(CASE WHEN metric = 'irr_cell'    THEN min_value END) AS irr_min,
        MAX(CASE WHEN metric = 'irr_cell'    THEN max_value END) AS irr_max,
        MAX(CASE WHEN metric = 'temp_module' THEN min_value END) AS temp_mod_min,
        MAX(CASE WHEN metric = 'temp_module' THEN max_value END) AS temp_mod_max,
        MAX(CASE WHEN metric = 'idc_a'         THEN min_value END) AS idc_min,
        MAX(CASE WHEN metric = 'idc_a'         THEN max_value END) AS idc_max,
        MAX(CASE WHEN metric = 'vdc_v'         THEN min_value END) AS vdc_min,
        MAX(CASE WHEN metric = 'vdc_v'         THEN max_value END) AS vdc_max,
        MAX(CASE WHEN metric = 'pac_w'         THEN min_value END) AS pac_min,
        MAX(CASE WHEN metric = 'pac_w'         THEN max_value END) AS pac_max
    FROM {{ ref('seed_per_system_bounds') }}
    GROUP BY system_id
),

-- ─── JOIN + COLLAPSE FLAGS ────────────────────────────────────────────────────
-- Multiple override rows can match a single base row (e.g. two spike nullifies
-- at the same timestamp, or a date-range row + a point row). GROUP BY + MAX
-- collapses them into one flag per base row safely.

joined AS (
    SELECT
        b.reading_at,
        b.reading_at_raw,
        b.system_id,
        b.inclination_deg,
        b.interval_seconds,
        b.irr_cell,
        b.irr_pyranometer,
        b.temp_module,
        b.temp_ambient,
        b.idc_a,
        b.vdc_v,
        b.pac_w,
        b.is_dst_corrected,
        b.source_file,

        MAX(CASE WHEN ico.override_type = 'nullify'     THEN 1 ELSE 0 END) AS irr_nullify,
        MAX(CASE WHEN ico.override_type = 'reconstruct' THEN 1 ELSE 0 END) AS irr_reconstruct,
        MAX(CASE WHEN ao.override_type  = 'nullify'     THEN 1 ELSE 0 END) AS ambient_nullify,
        MAX(CASE WHEN tmo.override_type = 'nullify'     THEN 1 ELSE 0 END) AS temp_mod_nullify,

        MIN(bd.irr_min)      AS irr_min,
        MIN(bd.irr_max)      AS irr_max,
        MIN(bd.temp_mod_min) AS temp_mod_min,
        MIN(bd.temp_mod_max) AS temp_mod_max,
        MIN(bd.idc_min)      AS idc_min,
        MIN(bd.idc_max)      AS idc_max,
        MIN(bd.vdc_min)      AS vdc_min,
        MIN(bd.vdc_max)      AS vdc_max,
        MIN(bd.pac_min)      AS pac_min,
        MIN(bd.pac_max)      AS pac_max

    FROM base b

    -- Irradiance: join on inclination_deg (shared per inclination group)
    LEFT JOIN irr_cell_overrides ico
        ON  b.inclination_deg = ico.inclination_deg
        AND b.reading_at BETWEEN ico.start_at AND ico.end_at

    -- Ambient: timestamp only
    LEFT JOIN ambient_overrides ao
        ON b.reading_at BETWEEN ao.start_at AND ao.end_at

    -- Module temp: matches if system_id matches OR if it's an all_temp_sensors row
    LEFT JOIN temp_mod_overrides tmo
        ON  (tmo.is_all_systems = TRUE OR tmo.system_id_num = b.system_id)
        AND b.reading_at BETWEEN tmo.start_at AND tmo.end_at

    -- Bounds: one row per system
    LEFT JOIN bounds bd
        ON b.system_id = bd.system_id

    GROUP BY
        b.reading_at, b.reading_at_raw, b.system_id, b.inclination_deg,
        b.interval_seconds, b.irr_cell, b.irr_pyranometer, b.temp_module,
        b.temp_ambient, b.idc_a, b.vdc_v, b.pac_w, b.is_dst_corrected, b.source_file
)

-- ─── APPLY OVERRIDES + BOUNDS ─────────────────────────────────────────────────

SELECT
    -- Identity (never modified)
    reading_at,
    reading_at_raw,
    system_id,
    inclination_deg,
    interval_seconds,
    is_dst_corrected,
    source_file,

    -- IRR CELL
    -- "reconstruct" rows KEEP their value; is_irr_reconstruct_needed flags them
    -- for int_irr_30deg_reconstructed to replace via pyranometer regression.
    CASE
        WHEN irr_nullify = 1                           THEN NULL
        WHEN irr_cell < irr_min OR irr_cell > irr_max  THEN NULL
        ELSE irr_cell
    END AS irr_cell,

    -- IRR PYRANOMETER
    -- No manual overrides. Range-filtered using same irr bounds (both W/m²).
    CASE
        WHEN irr_pyranometer < irr_min OR irr_pyranometer > irr_max THEN NULL
        ELSE irr_pyranometer
    END AS irr_pyranometer,

    -- TEMP MODULE
    CASE
        WHEN temp_mod_nullify = 1                                      THEN NULL
        WHEN temp_module < temp_mod_min OR temp_module > temp_mod_max  THEN NULL
        ELSE temp_module
    END AS temp_module,

    -- TEMP AMBIENT
    -- Nullified for Feb 2013 – Mar 29 2016 (sensor reads +5-6°C high).
    -- int_ambient_temp_derived fills those NULLs via reverse-TNOC formula.
    CASE
        WHEN ambient_nullify = 1 THEN NULL
        ELSE temp_ambient
    END AS temp_ambient,

    -- ELECTRICAL (range filter only — no manual overrides for electrical)
    CASE WHEN idc_a < idc_min OR idc_a > idc_max THEN NULL ELSE idc_a END AS idc_a,
    CASE WHEN vdc_v < vdc_min OR vdc_v > vdc_max THEN NULL ELSE vdc_v END AS vdc_v,
    CASE WHEN pac_w < pac_min OR pac_w > pac_max THEN NULL ELSE pac_w END AS pac_w,

    -- ── FLAGS ──────────────────────────────────────────────────────────────────
    irr_nullify      = 1 AS is_irr_manual_override,
    irr_reconstruct  = 1 AS is_irr_reconstruct_needed,
    temp_mod_nullify = 1 AS is_temp_manual_override,
    ambient_nullify  = 1 AS is_ambient_nullified,

    -- True if ANY metric was nullified by range bounds (not by manual override).
    -- Checked against RAW values from joined CTE (before CASE applies).
    (
           (irr_cell        IS NOT NULL AND irr_nullify      = 0 AND (irr_cell        < irr_min      OR irr_cell        > irr_max))
        OR (irr_pyranometer IS NOT NULL                          AND (irr_pyranometer  < irr_min      OR irr_pyranometer > irr_max))
        OR (temp_module     IS NOT NULL AND temp_mod_nullify = 0 AND (temp_module      < temp_mod_min OR temp_module     > temp_mod_max))
        OR (idc_a           IS NOT NULL                          AND (idc_a            < idc_min      OR idc_a           > idc_max))
        OR (vdc_v           IS NOT NULL                          AND (vdc_v            < vdc_min      OR vdc_v           > vdc_max))
        OR (pac_w           IS NOT NULL                          AND (pac_w            < pac_min      OR pac_w           > pac_max))
    ) AS is_range_filtered

FROM joined