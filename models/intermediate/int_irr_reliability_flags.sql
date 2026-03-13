{{
    config(
        materialized='table',
        description='Phase 5 irradiance reliability QA. FOR VISUAL REVIEW ONLY — does not modify data.'
    )
}}

-- ═══════════════════════════════════════════════════════════════════════════
-- int_irr_reliability_flags
--
-- Purpose : Statistical QA to identify unreliable irradiance periods.
-- Grain   : (date, inclination_deg) — one row per day per sensor group.
-- Input   : int_readings_unpivoted (raw, before cleaning)
--
-- Tests:
--   1. Cell vs pyranometer ratio drift    (30° only)
--   2. Cross-inclination correlation      (all three inclinations)
--   3. Clear-sky envelope                 (all three inclinations)
--
-- All flags: 1 = concern, 0 = clean, NULL = insufficient data to assess.
-- combined_reliability_score: sum of non-null flags (0-4).
--
-- Known limitation: cannot detect simultaneous proportional drift across
-- all sensors — a failure that makes all three inclinations read equally
-- low will not be caught by cross-inclination tests. Only the clear-sky
-- envelope (Test 3) can partially catch that case.
--
-- g_m3 (10°) dead from Nov 2020: after that date, any test that requires
-- irr_10 will produce NULL flags. This is correct expected behaviour.
-- ═══════════════════════════════════════════════════════════════════════════

WITH

-- ══════════════════════════════════════════════════════════════════════════
-- 0. BASE
-- irr_cell is a shared sensor per inclination — the same value is repeated
-- for every system in that inclination group. Deduplicate to one canonical
-- reading per (timestamp, inclination_deg) before all downstream tests.
-- ══════════════════════════════════════════════════════════════════════════

irr_per_timestep AS (
    SELECT
        reading_at,
        DATE(reading_at)  AS date,
        inclination_deg,
        interval_seconds,
        irr_cell,
        irr_pyranometer   -- non-null for 30° only
    FROM {{ ref('int_readings_unpivoted') }}
    WHERE irr_cell IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY reading_at, inclination_deg
        ORDER BY system_id
    ) = 1
),

-- ══════════════════════════════════════════════════════════════════════════
-- 1. TEST 1: Cell vs pyranometer ratio drift (30° only)
--    Rationale: irr_cell and irr_pyranometer share the same 30° plane.
--    Their ratio should be physically stable (~1.5 from the regression).
--    A persistent daily shift indicates one sensor is drifting or failing.
--    Method: daily median ratio → rolling 7-day median → flag ±15% deviation.
-- ══════════════════════════════════════════════════════════════════════════

ratio_readings AS (
    SELECT
        date,
        irr_cell / irr_pyranometer AS ratio
    FROM irr_per_timestep
    WHERE inclination_deg   = 30
      AND irr_cell          > 25
      AND irr_pyranometer   > 25
      AND irr_pyranometer   IS NOT NULL
),

daily_ratio AS (
    SELECT
        date,
        APPROX_QUANTILES(ratio, 100)[OFFSET(50)] AS daily_median_ratio,
        COUNT(*)                                  AS n_ratio_readings
    FROM ratio_readings
    GROUP BY date
),

-- Rolling 7-day median via self-join.
-- BigQuery PERCENTILE_CONT does not support ORDER BY in window context.
-- For each date d, join all rows in [d-6, d] and take APPROX_QUANTILES median.
test1_rolling AS (
    SELECT
        a.date,
        a.daily_median_ratio,
        a.n_ratio_readings,
        APPROX_QUANTILES(b.daily_median_ratio, 100)[OFFSET(50)] AS rolling_7d_median_ratio
    FROM daily_ratio a
    LEFT JOIN daily_ratio b
        ON b.date BETWEEN DATE_SUB(a.date, INTERVAL 6 DAY) AND a.date
    GROUP BY a.date, a.daily_median_ratio, a.n_ratio_readings
),

test1 AS (
    SELECT
        date,
        daily_median_ratio,
        rolling_7d_median_ratio,
        n_ratio_readings,
        CASE
            WHEN n_ratio_readings < 10     THEN NULL  -- too few qualifying readings
            WHEN rolling_7d_median_ratio IS NULL THEN NULL
            WHEN ABS(daily_median_ratio - rolling_7d_median_ratio)
                 / NULLIF(rolling_7d_median_ratio, 0) > 0.15 THEN 1
            ELSE 0
        END AS flag_cell_pyr_ratio
    FROM test1_rolling
),

-- ══════════════════════════════════════════════════════════════════════════
-- 2. TEST 2: Cross-inclination correlation
--    Two sub-tests per (date, inclination_deg):
--
--    2a. Pearson correlation of intraday 5-min readings between inclination
--        pairs. A low correlation on a day with normal readings from two of
--        the three sensors points at the third as the outlier.
--        Threshold: r < 0.90
--
--    2b. Daily irradiation ratio vs historical monthly median.
--        On any given day the absolute irradiation varies with cloud cover,
--        but the *ratio* between inclinations is historically stable for a
--        given month. A deviation > 40% from the historical median ratio
--        suggests one sensor has an offset or systematic failure.
--        Ratios use filtered irradiation (readings > 25 W/m² only) to avoid
--        false alarms from partial-day coverage or nighttime stray readings.
--        Ratio is set to NULL when either side < 0.5 kWh/m² (not enough sun
--        to produce a stable ratio regardless of sensor health).
-- ══════════════════════════════════════════════════════════════════════════

-- 2a. Pivot to one row per timestamp with irradiance for each inclination
incl_pivot AS (
    SELECT
        reading_at,
        date,
        MAX(CASE WHEN inclination_deg = 30 THEN irr_cell END) AS irr_30,
        MAX(CASE WHEN inclination_deg = 10 THEN irr_cell END) AS irr_10,
        MAX(CASE WHEN inclination_deg = 5  THEN irr_cell END) AS irr_5
    FROM irr_per_timestep
    GROUP BY reading_at, date
),

-- 2a. Daily Pearson correlation for each pair
daily_corr AS (
    SELECT
        date,
        CORR(irr_30, irr_10) AS corr_30_10,
        CORR(irr_30, irr_5)  AS corr_30_5,
        CORR(irr_10, irr_5)  AS corr_10_5,
        COUNT(CASE WHEN irr_30 IS NOT NULL AND irr_10 IS NOT NULL THEN 1 END) AS n_30_10,
        COUNT(CASE WHEN irr_30 IS NOT NULL AND irr_5  IS NOT NULL THEN 1 END) AS n_30_5,
        COUNT(CASE WHEN irr_10 IS NOT NULL AND irr_5  IS NOT NULL THEN 1 END) AS n_10_5
    FROM incl_pivot
    GROUP BY date
),

-- 2b. Daily irradiation per inclination — both raw and filtered (> 25 W/m²)
daily_irr AS (
    SELECT
        date,
        inclination_deg,
        SUM(irr_cell * interval_seconds / 3600000.0)                                         AS irr_daily_kwh,
        SUM(CASE WHEN irr_cell > 25 THEN irr_cell * interval_seconds / 3600000.0 ELSE 0 END) AS irr_daily_kwh_filtered
    FROM irr_per_timestep
    GROUP BY date, inclination_deg
),

-- 2b. Pivot daily irradiation by inclination
daily_irr_pivot AS (
    SELECT
        date,
        MAX(CASE WHEN inclination_deg = 30 THEN irr_daily_kwh          END) AS irr_daily_30,
        MAX(CASE WHEN inclination_deg = 10 THEN irr_daily_kwh          END) AS irr_daily_10,
        MAX(CASE WHEN inclination_deg = 5  THEN irr_daily_kwh          END) AS irr_daily_5,
        MAX(CASE WHEN inclination_deg = 30 THEN irr_daily_kwh_filtered END) AS irr_daily_30_filtered,
        MAX(CASE WHEN inclination_deg = 10 THEN irr_daily_kwh_filtered END) AS irr_daily_10_filtered,
        MAX(CASE WHEN inclination_deg = 5  THEN irr_daily_kwh_filtered END) AS irr_daily_5_filtered
    FROM daily_irr
    GROUP BY date
),

-- 2b. Daily irradiation ratios between inclination pairs (filtered values only).
--     Coverage columns are passed through so irr_ratio_flags can apply
--     minimum guards without an extra join.
daily_irr_ratios AS (
    SELECT
        date,
        EXTRACT(MONTH FROM date)                                         AS month_of_year,
        irr_daily_30_filtered / NULLIF(irr_daily_10_filtered, 0)         AS ratio_30_10,
        irr_daily_30_filtered / NULLIF(irr_daily_5_filtered,  0)         AS ratio_30_5,
        irr_daily_10_filtered / NULLIF(irr_daily_5_filtered,  0)         AS ratio_10_5,
        irr_daily_30_filtered,
        irr_daily_10_filtered,
        irr_daily_5_filtered
    FROM daily_irr_pivot
),

-- 2b. Historical monthly median of each ratio (across all available years)
monthly_median_ratios AS (
    SELECT
        month_of_year,
        APPROX_QUANTILES(ratio_30_10, 100)[OFFSET(50)] AS hist_median_ratio_30_10,
        APPROX_QUANTILES(ratio_30_5,  100)[OFFSET(50)] AS hist_median_ratio_30_5,
        APPROX_QUANTILES(ratio_10_5,  100)[OFFSET(50)] AS hist_median_ratio_10_5
    FROM daily_irr_ratios
    WHERE ratio_30_10 IS NOT NULL
       OR ratio_30_5  IS NOT NULL
       OR ratio_10_5  IS NOT NULL
    GROUP BY month_of_year
),

-- 2b. Join back to daily and compute deviation from historical median.
--     Minimum coverage guard: ratio treated as NULL when either relevant
--     sensor has < 0.5 kWh/m² for the day.
irr_ratio_flags AS (
    SELECT
        r.date,
        r.month_of_year,
        r.ratio_30_10,
        r.ratio_30_5,
        r.ratio_10_5,
        m.hist_median_ratio_30_10,
        m.hist_median_ratio_30_5,
        m.hist_median_ratio_10_5,
        -- flag_ratio_30_10: NULL when 30° or 10° coverage is insufficient
        CASE
            WHEN r.ratio_30_10 IS NULL
              OR m.hist_median_ratio_30_10 IS NULL
              OR r.irr_daily_30_filtered < 0.5
              OR r.irr_daily_10_filtered < 0.5
            THEN NULL
            WHEN ABS(r.ratio_30_10 - m.hist_median_ratio_30_10)
                 / NULLIF(m.hist_median_ratio_30_10, 0) > 0.40 THEN 1
            ELSE 0
        END AS flag_ratio_30_10,
        -- flag_ratio_30_5: NULL when 30° or 5° coverage is insufficient
        CASE
            WHEN r.ratio_30_5 IS NULL
              OR m.hist_median_ratio_30_5 IS NULL
              OR r.irr_daily_30_filtered < 0.5
              OR r.irr_daily_5_filtered  < 0.5
            THEN NULL
            WHEN ABS(r.ratio_30_5 - m.hist_median_ratio_30_5)
                 / NULLIF(m.hist_median_ratio_30_5, 0) > 0.40 THEN 1
            ELSE 0
        END AS flag_ratio_30_5,
        -- flag_ratio_10_5: NULL when 10° or 5° coverage is insufficient
        CASE
            WHEN r.ratio_10_5 IS NULL
              OR m.hist_median_ratio_10_5 IS NULL
              OR r.irr_daily_10_filtered < 0.5
              OR r.irr_daily_5_filtered  < 0.5
            THEN NULL
            WHEN ABS(r.ratio_10_5 - m.hist_median_ratio_10_5)
                 / NULLIF(m.hist_median_ratio_10_5, 0) > 0.40 THEN 1
            ELSE 0
        END AS flag_ratio_10_5
    FROM daily_irr_ratios r
    LEFT JOIN monthly_median_ratios m USING (month_of_year)
),

-- 2. Combine correlation and ratio flags → per inclination_deg rows
-- Logic:
--   30° suspect: corr_30_10 low AND corr_30_5 low
--   10° suspect: corr_30_10 low BUT corr_30_5 fine (or ratio_30_10 off but ratio_30_5 fine)
--   5°  suspect: corr_30_5  low BUT corr_30_10 fine (or ratio_30_5 off but ratio_30_10 fine)
--   When both 10° and 5° correlation tests implicate the same direction as 30°,
--   set 30° suspect regardless.

test2_base AS (
    SELECT
        c.date,
        c.corr_30_10,
        c.corr_30_5,
        c.corr_10_5,
        c.n_30_10,
        c.n_30_5,
        c.n_10_5,
        r.ratio_30_10,
        r.ratio_30_5,
        r.ratio_10_5,
        r.hist_median_ratio_30_10,
        r.hist_median_ratio_30_5,
        r.hist_median_ratio_10_5,
        r.flag_ratio_30_10,
        r.flag_ratio_30_5,
        r.flag_ratio_10_5,

        -- 30° suspect: both corr_30_10 and corr_30_5 are low
        CASE
            WHEN n_30_10 < 20 AND n_30_5 < 20 THEN NULL
            WHEN (n_30_10 >= 20 AND corr_30_10 < 0.90)
             AND (n_30_5  >= 20 AND corr_30_5  < 0.90) THEN 1
            WHEN COALESCE(r.flag_ratio_30_10, 0) = 1
             AND COALESCE(r.flag_ratio_30_5,  0) = 1 THEN 1
            ELSE 0
        END AS corr_flag_30,

        -- 10° suspect: corr_30_10 is low while corr_30_5 is fine
        CASE
            WHEN n_30_10 < 20 THEN NULL
            WHEN (n_30_10 >= 20 AND corr_30_10 < 0.90)
             AND (n_30_5  <  20 OR  corr_30_5  >= 0.90) THEN 1
            WHEN COALESCE(r.flag_ratio_30_10, 0) = 1
             AND COALESCE(r.flag_ratio_30_5,  0) = 0 THEN 1
            ELSE 0
        END AS corr_flag_10,

        -- 5° suspect: corr_30_5 is low while corr_30_10 is fine
        CASE
            WHEN n_30_5 < 20 THEN NULL
            WHEN (n_30_5  >= 20 AND corr_30_5  < 0.90)
             AND (n_30_10 <  20 OR  corr_30_10 >= 0.90) THEN 1
            WHEN COALESCE(r.flag_ratio_30_5,  0) = 1
             AND COALESCE(r.flag_ratio_30_10, 0) = 0 THEN 1
            ELSE 0
        END AS corr_flag_5

    FROM daily_corr c
    LEFT JOIN irr_ratio_flags r USING (date)
),

-- Expand to one row per inclination_deg
test2 AS (
    SELECT date, 30 AS inclination_deg,
        corr_flag_30 AS flag_cross_inclination,
        corr_30_10, corr_30_5, corr_10_5,
        ratio_30_10, ratio_30_5, ratio_10_5,
        hist_median_ratio_30_10, hist_median_ratio_30_5, hist_median_ratio_10_5,
        flag_ratio_30_10, flag_ratio_30_5, flag_ratio_10_5
    FROM test2_base
    UNION ALL
    SELECT date, 10 AS inclination_deg,
        corr_flag_10 AS flag_cross_inclination,
        corr_30_10, corr_30_5, corr_10_5,
        ratio_30_10, ratio_30_5, ratio_10_5,
        hist_median_ratio_30_10, hist_median_ratio_30_5, hist_median_ratio_10_5,
        flag_ratio_30_10, flag_ratio_30_5, flag_ratio_10_5
    FROM test2_base
    UNION ALL
    SELECT date, 5 AS inclination_deg,
        corr_flag_5 AS flag_cross_inclination,
        corr_30_10, corr_30_5, corr_10_5,
        ratio_30_10, ratio_30_5, ratio_10_5,
        hist_median_ratio_30_10, hist_median_ratio_30_5, hist_median_ratio_10_5,
        flag_ratio_30_10, flag_ratio_30_5, flag_ratio_10_5
    FROM test2_base
),

-- ══════════════════════════════════════════════════════════════════════════
-- 3. TEST 3: Clear-sky envelope
--    3a. Spike: any 5-min reading exceeds theoretical clear-sky peak × 1.15
--        (physically impossible; strong indicator of a bad reading or spike)
--    3b. Sustained low: daily total irradiation < clear-sky daily × 0.10
--        on a day where the clear-sky potential is meaningful (> 1.0 kWh/m²).
--        This does not flag cloudy days (those sit between 0.10 and 1.0);
--        it flags days where the sensor appears completely dead or near-zero.
-- ══════════════════════════════════════════════════════════════════════════

daily_agg AS (
    SELECT
        date,
        inclination_deg,
        MAX(irr_cell)                                             AS irr_peak_wm2,
        SUM(irr_cell * interval_seconds / 3600000.0)              AS irr_daily_kwh,
        COUNT(CASE WHEN irr_cell > 25 THEN 1 END)                 AS n_daylight_readings
    FROM irr_per_timestep
    GROUP BY date, inclination_deg
),

test3 AS (
    SELECT
        d.date,
        d.inclination_deg,
        d.irr_peak_wm2,
        d.irr_daily_kwh,
        d.n_daylight_readings,
        cs.clear_sky_peak_wm2,
        cs.clear_sky_daily_kwh_m2,

        -- 3a. Spike: peak reading exceeds theoretical max by 15%
        CASE
            WHEN cs.clear_sky_peak_wm2 IS NULL THEN NULL
            WHEN d.irr_peak_wm2 > cs.clear_sky_peak_wm2 * 1.15 THEN 1
            ELSE 0
        END AS flag_clear_sky_exceeded,

        -- 3b. Sustained low: daily total almost zero on a potentially sunny day
        CASE
            WHEN cs.clear_sky_daily_kwh_m2 IS NULL                  THEN NULL
            WHEN cs.clear_sky_daily_kwh_m2 < 1.0                    THEN NULL  -- short winter days
            WHEN d.n_daylight_readings < 10                          THEN NULL  -- not enough readings
            WHEN d.irr_daily_kwh < cs.clear_sky_daily_kwh_m2 * 0.10 THEN 1
            ELSE 0
        END AS flag_clear_sky_low

    FROM daily_agg d
    LEFT JOIN {{ ref('seed_clear_sky_envelope') }} cs
        ON  d.date            = cs.date
        AND d.inclination_deg = cs.inclination_deg
),

-- ══════════════════════════════════════════════════════════════════════════
-- SPINE + FINAL JOIN
-- ══════════════════════════════════════════════════════════════════════════

spine AS (
    SELECT DISTINCT date, inclination_deg
    FROM daily_agg
)

SELECT
    s.date,
    s.inclination_deg,

    -- ── Test 1 (30° only; NULL for 5° and 10°) ──────────────────────────
    t1.n_ratio_readings,
    ROUND(t1.daily_median_ratio,      4) AS daily_median_ratio,
    ROUND(t1.rolling_7d_median_ratio, 4) AS rolling_7d_median_ratio,
    t1.flag_cell_pyr_ratio,

    -- ── Test 2 ───────────────────────────────────────────────────────────
    ROUND(t2.corr_30_10, 4) AS corr_30_10,
    ROUND(t2.corr_30_5,  4) AS corr_30_5,
    ROUND(t2.corr_10_5,  4) AS corr_10_5,
    -- Irradiation ratio diagnostics (keep raw for visual review)
    ROUND(t2.ratio_30_10, 4)             AS irr_ratio_30_10,
    ROUND(t2.ratio_30_5,  4)             AS irr_ratio_30_5,
    ROUND(t2.ratio_10_5,  4)             AS irr_ratio_10_5,
    ROUND(t2.hist_median_ratio_30_10, 4) AS hist_median_ratio_30_10,
    ROUND(t2.hist_median_ratio_30_5,  4) AS hist_median_ratio_30_5,
    ROUND(t2.hist_median_ratio_10_5,  4) AS hist_median_ratio_10_5,
    t2.flag_ratio_30_10,
    t2.flag_ratio_30_5,
    t2.flag_ratio_10_5,
    t2.flag_cross_inclination,

    -- ── Test 3 ───────────────────────────────────────────────────────────
    ROUND(t3.irr_peak_wm2,           2) AS irr_peak_wm2,
    ROUND(t3.irr_daily_kwh,          4) AS irr_daily_kwh,
    ROUND(t3.clear_sky_peak_wm2,     2) AS clear_sky_peak_wm2,
    ROUND(t3.clear_sky_daily_kwh_m2, 4) AS clear_sky_daily_kwh_m2,
    t3.flag_clear_sky_exceeded,
    t3.flag_clear_sky_low,

    -- ── Combined score ───────────────────────────────────────────────────
    -- Sum of flags that fired (NULL treated as 0 — test could not run).
    -- Range 0-4. Rows with score >= 2 are the strongest candidates for
    -- manual review and potential new seed_manual_overrides entries.
    COALESCE(t1.flag_cell_pyr_ratio,       0)
    + COALESCE(t2.flag_cross_inclination,  0)
    + COALESCE(t3.flag_clear_sky_exceeded, 0)
    + COALESCE(t3.flag_clear_sky_low,      0) AS combined_reliability_score

FROM spine s
LEFT JOIN test1 t1
    ON  s.date = t1.date
    AND s.inclination_deg = 30  -- test1 only has rows for 30°
LEFT JOIN test2 t2
    ON  s.date = t2.date
    AND s.inclination_deg = t2.inclination_deg
LEFT JOIN test3 t3
    ON  s.date = t3.date
    AND s.inclination_deg = t3.inclination_deg
ORDER BY s.date, s.inclination_deg