{{
  config(
    materialized='table',
    cluster_by=['system_id']
  )
}}

WITH readings AS (
  SELECT
    r.reading_at,
    r.system_id,
    r.interval_seconds,
    r.irr_cell,
    r.temp_module,
    r.temp_ambient,
    r.idc_a,
    r.vdc_v,
    r.pac_w,
    r.is_complete,
    r.is_system_available,
    r.irr_data_type,
    r.is_ambient_nullified,
    m.area_m2,
    m.power_wp,
    m.gamma_pct,
    m.module_efficiency,
    m.eta_a_g_200,
    m.tnoc_celsius
  FROM {{ ref('int_readings_complete') }} r
  JOIN {{ ref('seed_system_metadata') }} m USING (system_id)
),

-- Per-reading intermediate calculations
reading_calcs AS (
  SELECT
    *,
    DATE(reading_at) AS date,

    -- Energy increments (Wh)
    CASE WHEN is_complete THEN idc_a * vdc_v * interval_seconds / 3600.0 END AS e_dc_wh,
    CASE WHEN is_complete THEN pac_w * interval_seconds / 3600.0 END AS e_ac_wh,
    CASE WHEN is_complete THEN irr_cell * interval_seconds / 3600.0 END AS irr_wh_m2,

    -- Temperature correction factor
    CASE WHEN is_complete AND is_system_available
      THEN 1.0 + (gamma_pct / 100.0) * (temp_module - 25.0)
    END AS c_k,

    -- Irradiance correction factor (only where system available)
    CASE WHEN is_complete AND is_system_available AND irr_cell > 0
      THEN 1.0 + ((eta_a_g_200 / module_efficiency) - 1.0)
           / LN(0.2) * LN(irr_cell / 1000.0)
    END AS f_k,

    -- Reference yield increment (only when system available, for d_t)
    CASE WHEN is_complete AND is_system_available
      THEN irr_cell * interval_seconds / 3600.0
    END AS irr_wh_m2_available

  FROM readings
),

-- Daily aggregation
daily AS (
  SELECT
    system_id,
    date,
    power_wp,
    area_m2,

    -- Group A — Core energy
    SUM(irr_wh_m2) / 1000.0                          AS h_i_kwh_m2,
    SUM(irr_wh_m2) * area_m2 / 1000.0                AS h_it_kwh,
    SUM(e_dc_wh) / 1000.0                             AS e_dc_kwh,
    SUM(e_ac_wh) / 1000.0                             AS e_ac_kwh,

    -- For Group E d_t
    SUM(irr_wh_m2_available) / 1000.0                AS h_i_available_kwh_m2,

    -- Corrected energy sums (for Groups F, G, H)
    SUM(CASE WHEN is_complete AND is_system_available AND c_k > 0
          THEN idc_a * vdc_v * interval_seconds / 3600.0 / c_k
        END) / 1000.0                                 AS e_dc_temp_corrected_kwh,

    SUM(CASE WHEN is_complete AND is_system_available AND c_k > 0 AND f_k IS NOT NULL
          THEN idc_a * vdc_v * interval_seconds / 3600.0 / c_k / f_k
        END) / 1000.0                                 AS e_dc_temp_irr_corrected_kwh,

    SUM(CASE WHEN is_complete AND is_system_available AND c_k > 0 AND f_k IS NOT NULL
          AND irr_cell >= 300
          THEN idc_a * vdc_v * interval_seconds / 3600.0 / c_k / f_k
        END) / 1000.0                                 AS e_dc_high_irr_corrected_kwh,

    SUM(CASE WHEN is_complete AND is_system_available AND c_k > 0 AND f_k IS NOT NULL
          AND irr_cell >= 300
          THEN irr_cell * interval_seconds / 3600.0
        END) / 1000.0                                 AS h_i_high_irr_kwh_m2,

    -- Group J — Data quality counts
    COUNTIF(is_complete)                              AS num_valid_readings,
    COUNT(*)                                          AS num_total_readings,
    SUM(CASE WHEN is_complete
          AND irr_data_type IN ('reconstructed_regression','reconstructed_tnoc','donor')
          THEN 1 ELSE 0 END)                          AS num_reconstructed,
    0                                                 AS num_interpolated,
    0                                                 AS num_temp_estimated,
    SUM(CASE WHEN is_complete AND is_ambient_nullified
          THEN 1 ELSE 0 END)                          AS num_ambient_derived,

    -- Daytime hours (all daytime readings regardless of completeness)
    COUNT(*) * MAX(interval_seconds) / 3600.0         AS daytime_hours

  FROM reading_calcs
  GROUP BY system_id, date, power_wp, area_m2
),

-- Final metrics derived from daily aggregates
final AS (
  SELECT
    system_id,
    date,

    -- Group A
    h_i_kwh_m2,
    h_it_kwh,
    e_dc_kwh,
    e_ac_kwh,

    -- Group B — Normalized yields
    h_i_kwh_m2                                        AS y_r,
    SAFE_DIVIDE(e_dc_kwh, power_wp / 1000.0)          AS y_a,
    SAFE_DIVIDE(e_ac_kwh, power_wp / 1000.0)          AS y_f,

    -- Group C — Losses
    h_i_kwh_m2 - SAFE_DIVIDE(e_dc_kwh, power_wp/1000.0)               AS l_c,
    SAFE_DIVIDE(e_dc_kwh, power_wp/1000.0)
      - SAFE_DIVIDE(e_ac_kwh, power_wp/1000.0)                         AS l_bos,

    -- Group D — Efficiencies
    SAFE_DIVIDE(e_dc_kwh, h_it_kwh) * 100             AS eta_array_pct,
    SAFE_DIVIDE(e_ac_kwh, e_dc_kwh) * 100             AS eta_bos_pct,
    SAFE_DIVIDE(e_ac_kwh, h_it_kwh) * 100             AS eta_system_pct,

    -- Group E — Performance ratios (null if daily irradiance < 0.5 kWh/m²)
    CASE WHEN h_i_kwh_m2 >= 0.5 THEN
      NULLIF(LEAST(
        SAFE_DIVIDE(e_ac_kwh, power_wp/1000.0) / NULLIF(h_i_kwh_m2, 0) * 100
      , 110.0), 110.0)
    END                                                AS pr_pct,
    h_i_available_kwh_m2                              AS y_r_sp,
    CASE WHEN h_i_kwh_m2 >= 0.5 THEN
      SAFE_DIVIDE(h_i_available_kwh_m2, h_i_kwh_m2) * 100
    END                                                AS d_t_pct,
    CASE WHEN h_i_kwh_m2 >= 0.5 THEN
      NULLIF(LEAST(
        SAFE_DIVIDE(
          SAFE_DIVIDE(e_ac_kwh, power_wp/1000.0),
          NULLIF(h_i_available_kwh_m2, 0)
        ) / NULLIF(SAFE_DIVIDE(h_i_available_kwh_m2, h_i_kwh_m2), 0) * 100
      , 110.0), 110.0)
    END                                                AS pr_sp_pct,

    -- Group F — "sp" = sin paradas: denominator is h_i_available (y_r_sp), not h_i (y_r)
    CASE WHEN h_i_kwh_m2 >= 0.5 AND h_i_available_kwh_m2 > 0 THEN
      NULLIF(LEAST(
        SAFE_DIVIDE(e_dc_temp_corrected_kwh, power_wp/1000.0) / NULLIF(h_i_available_kwh_m2, 0) * 100
      , 110.0), 110.0)
    END                                                AS pr_dc_sp_theta_pct,

    -- Group G
    CASE WHEN h_i_kwh_m2 >= 0.5 AND h_i_available_kwh_m2 > 0 THEN
      NULLIF(LEAST(
        SAFE_DIVIDE(e_dc_temp_irr_corrected_kwh, power_wp/1000.0) / NULLIF(h_i_available_kwh_m2, 0) * 100
      , 110.0), 110.0)
    END                                                AS pr_dc_sp_theta_g_pct,

    -- Group H — high-irr subset: denominator is h_i at >=300 W/m² when available
    CASE WHEN h_i_high_irr_kwh_m2 >= 0.3 THEN
      NULLIF(LEAST(
        SAFE_DIVIDE(e_dc_high_irr_corrected_kwh, power_wp/1000.0) / NULLIF(h_i_high_irr_kwh_m2, 0) * 100
      , 115.0), 115.0)
    END                                                AS pr_dc_sp_theta_g_high_pct,

    -- Group J — Data quality
    num_valid_readings,
    num_total_readings,
    SAFE_DIVIDE(num_valid_readings, num_total_readings) * 100  AS data_availability_pct,
    daytime_hours,
    SAFE_DIVIDE(num_reconstructed, num_valid_readings) * 100   AS pct_reconstructed,
    SAFE_DIVIDE(num_interpolated, num_valid_readings) * 100    AS pct_interpolated,
    SAFE_DIVIDE(num_temp_estimated, num_valid_readings) * 100  AS pct_temp_estimated,
    SAFE_DIVIDE(num_ambient_derived, num_valid_readings) * 100 AS pct_ambient_derived,

    -- Keep for loss decomposition
    power_wp,
    area_m2,
    h_i_available_kwh_m2,
    e_dc_temp_corrected_kwh,
    e_dc_temp_irr_corrected_kwh,
    -- FIX (Bug 2): expose high-irr terms so e_loss_irr_low can be computed correctly
    h_i_high_irr_kwh_m2,
    e_dc_high_irr_corrected_kwh

  FROM daily
)

SELECT
  system_id,
  date,
  h_i_kwh_m2, h_it_kwh, e_dc_kwh, e_ac_kwh,
  y_r, y_a, y_f,
  l_c, l_bos,
  eta_array_pct, eta_bos_pct, eta_system_pct,
  pr_pct, y_r_sp, d_t_pct, pr_sp_pct,
  pr_dc_sp_theta_pct,
  pr_dc_sp_theta_g_pct,
  pr_dc_sp_theta_g_high_pct,

  -- Group I — Loss decomposition (kWh)
  -- Downtime: energy that would have been generated during unavailable periods
  (h_i_kwh_m2 - h_i_available_kwh_m2)
    * (power_wp / 1000.0)
    * SAFE_DIVIDE(e_ac_kwh, NULLIF(h_i_available_kwh_m2 * power_wp/1000.0, 0))
                                                      AS e_loss_downtime,

  -- AC/BOS losses
  e_dc_kwh - e_ac_kwh                                AS e_loss_ac,

  -- Temperature losses
  e_dc_temp_corrected_kwh - e_dc_kwh                 AS e_loss_temp,

  -- Total irradiance non-linearity losses
  e_dc_temp_irr_corrected_kwh - e_dc_temp_corrected_kwh AS e_loss_irr,

  -- Low-irradiance specific losses
  -- Python formula: power_kWp × y_r_sp × (pr_high − pr_irr_corrected)
  -- Expanded: e_high_corrected × (h_i_available / h_i_high) − e_irr_corrected
  -- Can be legitimately negative when high-irr correction overshoots average.
  -- NULL on days with no high-irr readings (h_i_high = 0).
  -- Note: monthly mart overrides this with a period-level recalculation from
  -- the readings CTE, which is the authoritative value matching Python.
  CASE WHEN h_i_high_irr_kwh_m2 > 0 THEN
    SAFE_DIVIDE(
      e_dc_high_irr_corrected_kwh * h_i_available_kwh_m2,
      h_i_high_irr_kwh_m2
    ) - e_dc_temp_irr_corrected_kwh
  END                                               AS e_loss_irr_low,

  num_valid_readings, num_total_readings,
  data_availability_pct, daytime_hours,
  pct_reconstructed, pct_interpolated,
  pct_temp_estimated, pct_ambient_derived

FROM final