{{ config(materialized='view') }}

/*
  int_irr_30deg_reconstructed
  ─────────────────────────────────────────────────────────────────────
  Reconstructs irr_cell for 30° systems during dead cell sensor periods,
  using linear regression against the 30° pyranometer.

  Regression (derived from overlapping good data in Python code):
    irr_cell = 1.5058 × irr_pyranometer − 9.3033

  Matches Python behaviour: regression applied first, range bounds applied
  downstream (irr_cell < 25 and > 1500 removed by int_readings_cleaned
  bounds logic when this result is merged back in int_readings_merged).

  Only fires for rows flagged is_irr_reconstruct_needed = TRUE.
  Only 30° systems have a pyranometer — this model is 30° only by design.

  Output is a narrow table: (reading_at, system_id, irr_cell_reconstructed).
  int_readings_merged COALESCEs this back into the main flow.
*/

SELECT
    reading_at,
    system_id,
    ROUND(1.5058 * irr_pyranometer - 9.3033, 4) AS irr_cell_reconstructed,
    'reconstructed_regression'                   AS irr_data_type

FROM {{ ref('int_readings_cleaned') }}

WHERE is_irr_reconstruct_needed = TRUE
  AND irr_pyranometer IS NOT NULL
  AND inclination_deg = 30