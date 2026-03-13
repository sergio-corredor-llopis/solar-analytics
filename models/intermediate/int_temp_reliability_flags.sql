{{
    config(
        materialized='table',
        description='Phase 5 module temperature reliability QA. FOR VISUAL REVIEW ONLY — does not modify data.'
    )
}}

-- ═══════════════════════════════════════════════════════════════════════════
-- int_temp_reliability_flags
--
-- Purpose : Statistical QA to identify unreliable module temperature sensors.
-- Grain   : (reading_at, system_id) — 5-min resolution, per system.
-- Input   : int_readings_unpivoted (raw), seed_system_metadata
--
-- Tests:
--   1. Cross-system consistency  — |temp_module - median_across_30deg_systems| > 10°C
--   2. TNOC consistency          — persistent deviation from forward-TNOC expected value
--   3. Physical plausibility     — module < ambient (daytime), or module > ambient + 50°C
--
-- Systems with module temp sensors: 2, 3, 8, 9, 10, 11
--   System 10 (T_M3): unreliable from Jun 2015 — excluded from cross-system median
--                     but still evaluated against its own TNOC expectation.
--   Ambient offset period (Feb 2013–Mar 2016): reverse-TNOC derived ambient used.
--   Systems without module temp: 1, 4, 5, 6, 7, 12, 13 — excluded from all tests.
--
-- All flags: 1 = concern, 0 = clean, NULL = insufficient data / test not applicable.
-- combined_temp_reliability_score: sum of non-null flags (0–3).
-- ═══════════════════════════════════════════════════════════════════════════

WITH

-- ══════════════════════════════════════════════════════════════════════════
-- 0. BASE — raw readings for systems that have a module temp sensor
--    Use int_readings_unpivoted (before cleaning) as per architecture spec.
--    Bring in ambient from int_ambient_temp_derived for the offset period.
-- ══════════════════════════════════════════════════════════════════════════

base AS (
    SELECT
        r.reading_at,
        DATE(r.reading_at)  AS date,
        r.system_id,
        r.inclination_deg,
        r.irr_cell,
        r.temp_module,
        r.temp_ambient,
        -- Use reverse-TNOC derived ambient for the offset period (Feb 2013–Mar 2016)
        COALESCE(a.temp_ambient_derived, r.temp_ambient) AS temp_ambient_final,
        m.tnoc_celsius
    FROM {{ ref('int_readings_unpivoted') }} r
    LEFT JOIN {{ ref('int_ambient_temp_derived') }} a
        ON r.reading_at = a.reading_at
    LEFT JOIN {{ ref('seed_system_metadata') }} m
        ON r.system_id = m.system_id
    WHERE r.temp_module IS NOT NULL
      -- Only systems with physical module temp sensors
      AND r.system_id IN (2, 3, 8, 9, 10, 11)
      -- Only daytime readings with meaningful irradiance for TNOC tests
      AND r.irr_cell > 25
),

-- ══════════════════════════════════════════════════════════════════════════
-- 1. TEST 1: Cross-system consistency (30° systems only)
--    All 30° systems share the same irradiance sensor and similar thermal
--    environment, so their module temps should track closely.
--    Method: per-timestamp median across reliable 30° systems, flag any
--    system deviating by more than 10°C.
--
--    System 10 excluded from median calculation after May 2015
--    (known unreliable) but its own readings are still evaluated against
--    the median computed from the remaining systems.
-- ══════════════════════════════════════════════════════════════════════════

-- Compute per-timestamp median from reliable systems only
median_base AS (
    SELECT
        reading_at,
        APPROX_QUANTILES(temp_module, 100)[OFFSET(50)] AS median_temp_30deg,
        COUNT(*)                                        AS n_systems_in_median
    FROM base
    WHERE inclination_deg = 30
      -- Exclude system 10 from median after May 2015
      AND NOT (system_id = 10 AND reading_at >= '2015-06-01')
    GROUP BY reading_at
),

test1 AS (
    SELECT
        b.reading_at,
        b.system_id,
        b.temp_module,
        m.median_temp_30deg,
        m.n_systems_in_median,
        b.temp_module - m.median_temp_30deg AS deviation_from_median_c,
        CASE
            WHEN b.inclination_deg != 30     THEN NULL  -- test only meaningful for shared-sensor group
            WHEN m.n_systems_in_median < 2   THEN NULL  -- need at least 2 systems to compute a meaningful median
            WHEN m.median_temp_30deg IS NULL THEN NULL
            WHEN ABS(b.temp_module - m.median_temp_30deg) > 10 THEN 1
            ELSE 0
        END AS flag_cross_system
    FROM base b
    LEFT JOIN median_base m USING (reading_at)
),

-- ══════════════════════════════════════════════════════════════════════════
-- 2. TEST 2: TNOC consistency
--    Forward-TNOC: temp_module_expected = temp_ambient + (TNOC-20) × (irr_cell/800)
--    Compare measured vs expected per reading.
--    A single-reading deviation is normal (TNOC is approximate).
--    Flag: rolling 24-hour median absolute deviation > 5°C.
--    In ambient offset period (Feb 2013–Mar 29 2016) for systems
--    whose ambient is derived (less reliable for this test)
--    we have int_ambient_temp_derived for that period, and we
--    joined it in base as temp_ambient_final so we can include it.
--    System 10 after May 2015: include (this test can still detect its failure).
-- ══════════════════════════════════════════════════════════════════════════

tnoc_diff AS (
    SELECT
        b.reading_at,
        b.date,
        b.system_id,
        b.temp_module,
        b.temp_ambient_final,
        b.irr_cell,
        b.tnoc_celsius,
        -- Forward-TNOC expected temp
        b.temp_ambient_final + (b.tnoc_celsius - 20.0) * (b.irr_cell / 800.0)
            AS temp_module_tnoc_expected,
        -- Absolute deviation
        ABS(
            b.temp_module
            - (b.temp_ambient_final + (b.tnoc_celsius - 20.0) * (b.irr_cell / 800.0))
        ) AS abs_deviation_c
    FROM base b
    WHERE b.temp_ambient_final IS NOT NULL
      AND b.tnoc_celsius        IS NOT NULL
),

-- Rolling 24-hour median absolute deviation (self-join on same system, same day ± 12 readings)
-- For 5-min data: 24hr = 288 readings. Self-join on date window to keep cost low.
-- Aggregate to daily median deviation first, then apply threshold.
daily_tnoc_dev AS (
    SELECT
        date,
        system_id,
        APPROX_QUANTILES(abs_deviation_c, 100)[OFFSET(50)] AS daily_median_abs_dev,
        COUNT(*) AS n_tnoc_readings
    FROM tnoc_diff
    GROUP BY date, system_id
),

test2 AS (
    SELECT
        date,
        system_id,
        daily_median_abs_dev,
        n_tnoc_readings,
        CASE
            WHEN n_tnoc_readings < 10        THEN NULL
            WHEN daily_median_abs_dev IS NULL THEN NULL
            WHEN daily_median_abs_dev > 5    THEN 1
            ELSE 0
        END AS flag_tnoc_deviation
    FROM daily_tnoc_dev
),

-- ══════════════════════════════════════════════════════════════════════════
-- 3. TEST 3: Physical plausibility (per 5-min reading)
--    Rule 1: During daytime (irr_cell > 25), temp_module should be >= temp_ambient.
--            A PV cell in sunlight is always warmer than ambient air.
--    Rule 2: temp_module <= temp_ambient + 50°C.
--            The TNOC formula maxes out ~40°C above ambient at 1000 W/m².
--            Exceeding ambient+50°C is physically implausible.
--    No exclusion for ambient offset period — we use temp_ambient_final.
-- ══════════════════════════════════════════════════════════════════════════

test3 AS (
    SELECT
        reading_at,
        system_id,
        temp_module,
        temp_ambient_final,
        CASE
            WHEN temp_ambient_final IS NULL THEN NULL
            WHEN temp_module < temp_ambient_final      THEN 1  -- cooler than air in sunlight
            WHEN temp_module > temp_ambient_final + 50 THEN 1  -- physically implausible high
            ELSE 0
        END AS flag_physical
    FROM base
),

-- ══════════════════════════════════════════════════════════════════════════
-- SPINE + FINAL JOIN
-- Grain: (reading_at, system_id) — one row per 5-min reading per system
-- Tests 1 and 3 are at reading_at level; test 2 is daily → join on date
-- ══════════════════════════════════════════════════════════════════════════

spine AS (
    SELECT DISTINCT reading_at, DATE(reading_at) AS date, system_id
    FROM base
)

SELECT
    s.reading_at,
    s.system_id,

    -- ── Test 1 ───────────────────────────────────────────────────────────
    ROUND(t1.temp_module,              2) AS temp_module,
    ROUND(t1.median_temp_30deg,        2) AS median_temp_30deg,
    ROUND(t1.deviation_from_median_c,  2) AS deviation_from_median_c,
    t1.n_systems_in_median,
    t1.flag_cross_system,

    -- ── Test 2 ───────────────────────────────────────────────────────────
    ROUND(t2.daily_median_abs_dev,     2) AS daily_median_abs_dev_tnoc_c,
    t2.n_tnoc_readings,
    t2.flag_tnoc_deviation,

    -- ── Test 3 ───────────────────────────────────────────────────────────
    ROUND(t3.temp_ambient_final,       2) AS temp_ambient_final,
    t3.flag_physical,

    -- ── Combined score ───────────────────────────────────────────────────
    COALESCE(t1.flag_cross_system,    0)
    + COALESCE(t2.flag_tnoc_deviation, 0)
    + COALESCE(t3.flag_physical,       0) AS combined_temp_reliability_score

FROM spine s
LEFT JOIN test1 t1
    ON  s.reading_at = t1.reading_at
    AND s.system_id  = t1.system_id
LEFT JOIN test2 t2
    ON  s.date      = t2.date
    AND s.system_id = t2.system_id
LEFT JOIN test3 t3
    ON  s.reading_at = t3.reading_at
    AND s.system_id  = t3.system_id
ORDER BY s.reading_at, s.system_id