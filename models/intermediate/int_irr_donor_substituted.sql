{{ config(materialized='view') }}

/*
  int_irr_donor_substituted
  ─────────────────────────────────────────────────────────────────────
  Replaces irr_cell for 5° and 10° systems during dead/unreliable cell
  sensor periods using manually selected donor days.

  Donor day selection is manual: Sergio visually picks a day with similar
  weather at the same inclination and records it in seed_irr_donor_days.

  Currently returns zero rows — seed_irr_donor_days is an empty stub.
  Will be populated in Phase 5 after int_irr_reliability_flags analysis.

  Logic: for each target (date, inclination_deg) in the seed, pull irr_cell
  from the donor_date at the same time-of-day offset within the day.

  Output is narrow: (reading_at, system_id, irr_cell_donor, irr_data_type)
  joined back by int_readings_merged via COALESCE.
*/

WITH donor_days AS (
    SELECT
        PARSE_DATE('%Y-%m-%d', CAST(target_date AS STRING)) AS target_date,
        inclination_deg,
        PARSE_DATE('%Y-%m-%d', CAST(donor_date  AS STRING)) AS donor_date
    FROM {{ ref('seed_irr_donor_days') }}
),

-- All cleaned readings — needed twice: once as target, once as donor
cleaned AS (
    SELECT
        reading_at,
        system_id,
        inclination_deg,
        irr_cell,
        DATE(reading_at)                                            AS reading_date,
        TIME(reading_at)                                            AS reading_time
    FROM {{ ref('int_readings_cleaned') }}
),

-- Target rows: dates that need substitution
targets AS (
    SELECT
        c.reading_at,
        c.system_id,
        c.inclination_deg,
        c.reading_time,
        d.donor_date
    FROM cleaned c
    INNER JOIN donor_days d
        ON  DATE(c.reading_at)  = d.target_date
        AND c.inclination_deg   = d.inclination_deg
),

-- Donor rows: same time-of-day on the donor date, same inclination
-- Use one representative system per inclination to pull irr_cell
-- (all systems at same inclination have identical irr_cell values)
donors AS (
    SELECT DISTINCT
        reading_date,
        inclination_deg,
        reading_time,
        irr_cell AS irr_cell_donor
    FROM cleaned
    WHERE irr_cell IS NOT NULL
)

SELECT
    t.reading_at,
    t.system_id,
    d.irr_cell_donor,
    'reconstructed_donor' AS irr_data_type

FROM targets t
LEFT JOIN donors d
    ON  d.reading_date    = t.donor_date
    AND d.inclination_deg = t.inclination_deg
    AND d.reading_time    = t.reading_time