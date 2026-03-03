{{ config(materialized='view') }}

/*
  int_readings_daytime
  ─────────────────────────────────────────────────────────────────────
  Two things happen here:

  1. Filter to daytime only: keep readings where
     reading_at >= sunrise_at AND reading_at <= sunset_at
     (per date and inclination_deg from int_sun_times)

  2. Merge derived ambient temperature: for the faulty sensor period
     (Feb 2013 - Mar 2016), replace NULL temp_ambient with the
     reverse-TNOC derived value from int_ambient_temp_derived.

  All downstream models work with daytime rows only.
*/

WITH merged AS (
    SELECT * FROM {{ ref('int_readings_merged') }}
),

sun_times AS (
    SELECT
        date,
        inclination_deg,
        sunrise_at,
        sunset_at,
        sun_method
    FROM {{ ref('int_sun_times') }}
),

ambient_derived AS (
    SELECT
        reading_at,
        temp_ambient_derived,
        temp_ambient_source
    FROM {{ ref('int_ambient_temp_derived') }}
)

SELECT
    m.reading_at,
    m.reading_at_raw,
    m.system_id,
    m.inclination_deg,
    m.interval_seconds,
    m.is_dst_corrected,
    m.source_file,

    -- Irradiance
    m.irr_cell,
    m.irr_pyranometer,
    m.irr_data_type,

    -- Module temp: pass through unchanged
    -- (int_readings_temp_estimated fills NULLs downstream via TNOC)
    m.temp_module,

    -- Ambient temp: use derived value during faulty period, measured otherwise
    CASE
        WHEN m.is_ambient_nullified = TRUE
            THEN a.temp_ambient_derived
        ELSE m.temp_ambient
    END AS temp_ambient,

    CASE
        WHEN m.is_ambient_nullified = TRUE
            THEN COALESCE(a.temp_ambient_source, 'derived_reverse_tnoc')
        ELSE 'measured'
    END AS temp_ambient_source,

    -- Electrical
    m.idc_a,
    m.vdc_v,
    m.pac_w,

    -- Sun times (carried through for mart calculations)
    s.sunrise_at,
    s.sunset_at,
    s.sun_method,

    -- Flags from upstream
    m.is_irr_manual_override,
    m.is_irr_reconstruct_needed,
    m.is_temp_manual_override,
    m.is_ambient_nullified,
    m.is_range_filtered

FROM merged m

INNER JOIN sun_times s
    ON  DATE(m.reading_at) = s.date
    AND m.inclination_deg  = s.inclination_deg
    AND m.reading_at >= s.sunrise_at
    AND m.reading_at <= s.sunset_at

LEFT JOIN ambient_derived a
    ON m.reading_at = a.reading_at