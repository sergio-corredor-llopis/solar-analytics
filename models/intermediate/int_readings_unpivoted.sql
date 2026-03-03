{{
    config(
        materialized='view'
    )
}}


WITH source AS (
    SELECT * FROM {{ ref('stg_solar_readings') }}
),

intervals AS (
    SELECT * FROM {{ ref('seed_interval_definitions') }}
),

/*
  Step 1 — Unpivot: 111-column wide format → per-system rows.
  One UNION ALL block per system. Column mapping from §0d of dbt_Architecture.md.
  Irradiance sensors are shared per inclination, so all systems at the same
  inclination will have identical irr_cell values — this is correct and expected.
*/
unpivoted AS (

    -- System 1: 10°, cell=g_m3, no pyranometer, no module temp sensor
    SELECT
        reading_at        AS reading_at_raw,
        1                 AS system_id,
        10                AS inclination_deg,
        g_m3              AS irr_cell,
        CAST(NULL AS FLOAT64) AS irr_pyranometer,
        CAST(NULL AS FLOAT64) AS temp_module,
        t_ambient         AS temp_ambient,
        i_dc_1            AS idc_a,
        u_dc_1            AS vdc_v,
        p_ac_1            AS pac_w,
        source_file
    FROM source

    UNION ALL

    -- System 2: 30°, cell=g_m1, pyranometer=g_h1, module temp=t_module_2
    SELECT
        reading_at        AS reading_at_raw,
        2                 AS system_id,
        30                AS inclination_deg,
        g_m1              AS irr_cell,
        g_h1              AS irr_pyranometer,
        t_module_2        AS temp_module,
        t_ambient         AS temp_ambient,
        i_dc_2            AS idc_a,
        u_dc_2            AS vdc_v,
        p_ac_2            AS pac_w,
        source_file
    FROM source

    UNION ALL

    -- System 3: 30°, cell=g_m1, pyranometer=g_h1, module temp=t_module_6
    SELECT
        reading_at        AS reading_at_raw,
        3                 AS system_id,
        30                AS inclination_deg,
        g_m1              AS irr_cell,
        g_h1              AS irr_pyranometer,
        t_module_6        AS temp_module,
        t_ambient         AS temp_ambient,
        i_dc_3            AS idc_a,
        u_dc_3            AS vdc_v,
        p_ac_3            AS pac_w,
        source_file
    FROM source

    UNION ALL

    -- System 4: 10°, cell=g_m3, no pyranometer, no module temp sensor
    SELECT
        reading_at        AS reading_at_raw,
        4                 AS system_id,
        10                AS inclination_deg,
        g_m3              AS irr_cell,
        CAST(NULL AS FLOAT64) AS irr_pyranometer,
        CAST(NULL AS FLOAT64) AS temp_module,
        t_ambient         AS temp_ambient,
        i_dc_4            AS idc_a,
        u_dc_4            AS vdc_v,
        p_ac_4            AS pac_w,
        source_file
    FROM source

    UNION ALL

    -- System 5: 10°, cell=g_m3, no pyranometer, no module temp sensor
    SELECT
        reading_at        AS reading_at_raw,
        5                 AS system_id,
        10                AS inclination_deg,
        g_m3              AS irr_cell,
        CAST(NULL AS FLOAT64) AS irr_pyranometer,
        CAST(NULL AS FLOAT64) AS temp_module,
        t_ambient         AS temp_ambient,
        i_dc_5            AS idc_a,
        u_dc_5            AS vdc_v,
        p_ac_5            AS pac_w,
        source_file
    FROM source

    UNION ALL

    -- System 6: 10°, cell=g_m3, no pyranometer, no module temp sensor
    SELECT
        reading_at        AS reading_at_raw,
        6                 AS system_id,
        10                AS inclination_deg,
        g_m3              AS irr_cell,
        CAST(NULL AS FLOAT64) AS irr_pyranometer,
        CAST(NULL AS FLOAT64) AS temp_module,
        t_ambient         AS temp_ambient,
        i_dc_6            AS idc_a,
        u_dc_6            AS vdc_v,
        p_ac_6            AS pac_w,
        source_file
    FROM source

    UNION ALL

    -- System 7: 10°, cell=g_m3, no pyranometer, no module temp sensor
    SELECT
        reading_at        AS reading_at_raw,
        7                 AS system_id,
        10                AS inclination_deg,
        g_m3              AS irr_cell,
        CAST(NULL AS FLOAT64) AS irr_pyranometer,
        CAST(NULL AS FLOAT64) AS temp_module,
        t_ambient         AS temp_ambient,
        i_dc_7            AS idc_a,
        u_dc_7            AS vdc_v,
        p_ac_7            AS pac_w,
        source_file
    FROM source

    UNION ALL

    -- System 8: 30°, cell=g_m1, pyranometer=g_h1, module temp=t_module_5
    SELECT
        reading_at        AS reading_at_raw,
        8                 AS system_id,
        30                AS inclination_deg,
        g_m1              AS irr_cell,
        g_h1              AS irr_pyranometer,
        t_module_5        AS temp_module,
        t_ambient         AS temp_ambient,
        i_dc_8            AS idc_a,
        u_dc_8            AS vdc_v,
        p_ac_8            AS pac_w,
        source_file
    FROM source

    UNION ALL

    -- System 9: 30°, cell=g_m1, pyranometer=g_h1, module temp=t_module_4
    SELECT
        reading_at        AS reading_at_raw,
        9                 AS system_id,
        30                AS inclination_deg,
        g_m1              AS irr_cell,
        g_h1              AS irr_pyranometer,
        t_module_4        AS temp_module,
        t_ambient         AS temp_ambient,
        i_dc_9            AS idc_a,
        u_dc_9            AS vdc_v,
        p_ac_9            AS pac_w,
        source_file
    FROM source

    UNION ALL

    -- System 10: 30°, cell=g_m1, pyranometer=g_h1, module temp=t_module_3
    SELECT
        reading_at        AS reading_at_raw,
        10                AS system_id,
        30                AS inclination_deg,
        g_m1              AS irr_cell,
        g_h1              AS irr_pyranometer,
        t_module_3        AS temp_module,
        t_ambient         AS temp_ambient,
        i_dc_10           AS idc_a,
        u_dc_10           AS vdc_v,
        p_ac_10           AS pac_w,
        source_file
    FROM source

    UNION ALL

    -- System 11: 30°, cell=g_m1, pyranometer=g_h1, module temp=t_module_1
    SELECT
        reading_at        AS reading_at_raw,
        11                AS system_id,
        30                AS inclination_deg,
        g_m1              AS irr_cell,
        g_h1              AS irr_pyranometer,
        t_module_1        AS temp_module,
        t_ambient         AS temp_ambient,
        i_dc_11           AS idc_a,
        u_dc_11           AS vdc_v,
        p_ac_11           AS pac_w,
        source_file
    FROM source

    UNION ALL

    -- System 12: 30°, cell=g_m1, pyranometer=g_h1, no module temp sensor
    SELECT
        reading_at        AS reading_at_raw,
        12                AS system_id,
        30                AS inclination_deg,
        g_m1              AS irr_cell,
        g_h1              AS irr_pyranometer,
        CAST(NULL AS FLOAT64) AS temp_module,
        t_ambient         AS temp_ambient,
        i_dc_12           AS idc_a,
        u_dc_12           AS vdc_v,
        p_ac_12           AS pac_w,
        source_file
    FROM source

    UNION ALL

    -- System 13: 5°, cell=g_m2, no pyranometer, no module temp sensor
    -- NOTE: uses i_dc_1_detail / u_dc_1_detail / p_ac_1_detail (device 47597)
    -- NOT system 1 — see §0d of dbt_Architecture.md
    SELECT
        reading_at        AS reading_at_raw,
        13                AS system_id,
        5                 AS inclination_deg,
        g_m2              AS irr_cell,
        CAST(NULL AS FLOAT64) AS irr_pyranometer,
        CAST(NULL AS FLOAT64) AS temp_module,
        t_ambient         AS temp_ambient,
        i_dc_13     AS idc_a,
        u_dc_13     AS vdc_v,
        p_ac_13     AS pac_w,
        source_file
    FROM source

),

/*
  Step 2 — 2017 DST correction.
  Meteocontrol did not apply the CET→CEST change in 2017.
  Spain CEST: last Sunday of March (2017-03-26) → last Sunday of October (2017-10-29).
  We add +1 hour to all affected timestamps to get true local solar time.
  All other years are correctly handled by Meteocontrol — no correction needed.
*/
dst_corrected AS (
    SELECT
        CASE
            WHEN EXTRACT(YEAR FROM reading_at_raw) = 2017
                AND reading_at_raw >= TIMESTAMP('2017-03-26 02:00:00')
                AND reading_at_raw <  TIMESTAMP('2017-10-29 03:00:00')
            THEN TIMESTAMP_ADD(reading_at_raw, INTERVAL 1 HOUR)
            ELSE reading_at_raw
        END                                                     AS reading_at,
        reading_at_raw,
        system_id,
        inclination_deg,
        irr_cell,
        irr_pyranometer,
        temp_module,
        temp_ambient,
        idc_a,
        vdc_v,
        pac_w,
        CASE
            WHEN EXTRACT(YEAR FROM reading_at_raw) = 2017
                AND reading_at_raw >= TIMESTAMP('2017-03-26 02:00:00')
                AND reading_at_raw <  TIMESTAMP('2017-10-29 03:00:00')
            THEN TRUE
            ELSE FALSE
        END                                                     AS is_dst_corrected,
        source_file
    FROM unpivoted
)

/*
  Step 3 — Interval join.
  Join uses reading_at_raw (original Meteocontrol timestamp) because the
  interval definitions are in the same reference frame as the raw data.
*/
SELECT
    d.reading_at,
    d.reading_at_raw,
    d.system_id,
    d.inclination_deg,
    d.irr_cell,
    d.irr_pyranometer,
    d.temp_module,
    d.temp_ambient,
    d.idc_a,
    d.vdc_v,
    d.pac_w,
    d.is_dst_corrected,
    i.interval_seconds,
    d.source_file
FROM dst_corrected d
LEFT JOIN intervals i
    ON d.reading_at_raw >= CAST(i.valid_from AS TIMESTAMP)
    AND d.reading_at_raw <= CAST(i.valid_to AS TIMESTAMP)