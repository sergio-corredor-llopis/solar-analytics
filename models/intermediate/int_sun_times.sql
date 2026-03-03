{{ config(materialized='table') }}

WITH merged AS (
    SELECT
        reading_at,
        inclination_deg,
        irr_cell,
        interval_seconds,
        DATE(reading_at) AS reading_date
    FROM {{ ref('int_readings_merged') }}
),

astro AS (
    SELECT
        CAST(date AS DATE)              AS date,
        CAST(sunrise_at AS TIMESTAMP)   AS sunrise_at,
        CAST(sunset_at  AS TIMESTAMP)   AS sunset_at
    FROM {{ ref('seed_astronomical_sun_times') }}
),

irr_counts AS (
    SELECT
        reading_date,
        inclination_deg,
        MIN(interval_seconds)                                   AS interval_seconds,
        COUNTIF(irr_cell >= 25)                                 AS actual_daytime_readings,
        MIN(CASE WHEN irr_cell >= 25 THEN reading_at END)       AS irr_sunrise,
        MAX(CASE WHEN irr_cell >= 25 THEN reading_at END)       AS irr_sunset
    FROM merged
    GROUP BY reading_date, inclination_deg
),

-- One row per (date, inclination) — always, even if no irr data that day
all_dates_inclinations AS (
    SELECT
        a.date,
        inc.inclination_deg,
        a.sunrise_at    AS astro_sunrise,
        a.sunset_at     AS astro_sunset
    FROM astro a
    CROSS JOIN (SELECT DISTINCT inclination_deg FROM merged) inc
),

-- Join irr counts and determine if irradiance-based is valid
with_irr AS (
    SELECT
        adi.date,
        adi.inclination_deg,
        adi.astro_sunrise,
        adi.astro_sunset,
        ic.irr_sunrise,
        ic.irr_sunset,
        COALESCE(ic.actual_daytime_readings, 0)     AS actual_daytime_readings,
        TIMESTAMP_DIFF(adi.astro_sunset, adi.astro_sunrise, SECOND)
            / NULLIF(COALESCE(ic.interval_seconds, 300), 0) AS expected_daytime_readings,

        -- Valid if >= 80% threshold AND within 3hr of astronomical
        CASE
            WHEN COALESCE(ic.actual_daytime_readings, 0) >= 0.8 *
                (TIMESTAMP_DIFF(adi.astro_sunset, adi.astro_sunrise, SECOND)
                    / NULLIF(COALESCE(ic.interval_seconds, 300), 0))
                AND ic.irr_sunrise IS NOT NULL
                AND ic.irr_sunset  IS NOT NULL
                AND ABS(TIMESTAMP_DIFF(ic.irr_sunrise, adi.astro_sunrise, MINUTE)) <= 180
                AND ABS(TIMESTAMP_DIFF(ic.irr_sunset,  adi.astro_sunset,  MINUTE)) <= 180
            THEN TRUE
            ELSE FALSE
        END AS is_irr_valid

    FROM all_dates_inclinations adi
    LEFT JOIN irr_counts ic
        ON  adi.date            = ic.reading_date
        AND adi.inclination_deg = ic.inclination_deg
),

-- For each date, find the best available irradiance-based times
-- from ANY inclination (for cross-inclination borrowing)
best_irr_per_date AS (
    SELECT
        date,
        -- Use the inclination with the most daytime readings as donor
        -- (MAX actual_daytime_readings wins via QUALIFY)
        irr_sunrise     AS donor_sunrise,
        irr_sunset      AS donor_sunset
    FROM with_irr
    WHERE is_irr_valid = TRUE
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY date
        ORDER BY actual_daytime_readings DESC
    ) = 1
)

SELECT
    w.date,
    w.inclination_deg,

    CASE
        WHEN w.is_irr_valid = TRUE              THEN w.irr_sunrise      -- same-inclination
        WHEN b.donor_sunrise IS NOT NULL        THEN b.donor_sunrise    -- cross-inclination
        ELSE w.astro_sunrise                                            -- astronomical
    END AS sunrise_at,

    CASE
        WHEN w.is_irr_valid = TRUE              THEN w.irr_sunset
        WHEN b.donor_sunset IS NOT NULL         THEN b.donor_sunset
        ELSE w.astro_sunset
    END AS sunset_at,

    CASE
        WHEN w.is_irr_valid = TRUE              THEN 'irradiance_based'
        WHEN b.donor_sunrise IS NOT NULL        THEN 'irradiance_borrowed'
        ELSE 'astronomical'
    END AS sun_method

FROM with_irr w
LEFT JOIN best_irr_per_date b
    ON w.date = b.date