{{
  config(
    materialized='table',
    cluster_by=['system_id']
  )
}}

WITH monthly AS (
  SELECT *
  FROM {{ ref('mart_monthly_performance') }}
),

annual_agg AS (
  SELECT
    m.system_id,
    EXTRACT(YEAR FROM m.month)                        AS year,
    MAX(s.power_wp)                                   AS power_wp,
    MAX(s.area_m2)                                    AS area_m2,

    -- Summable
    SUM(h_i_kwh_m2)                                   AS h_i_kwh_m2,
    SUM(h_it_kwh)                                     AS h_it_kwh,
    SUM(e_dc_kwh)                                     AS e_dc_kwh,
    SUM(e_ac_kwh)                                     AS e_ac_kwh,
    SUM(e_loss_downtime)                              AS e_loss_downtime,
    SUM(e_loss_ac)                                    AS e_loss_ac,
    SUM(e_loss_temp)                                  AS e_loss_temp,
    SUM(e_loss_irr)                                   AS e_loss_irr,
    SUM(e_loss_irr_low)                               AS e_loss_irr_low,
    SUM(num_valid_readings)                           AS num_valid_readings,
    SUM(num_total_readings)                           AS num_total_readings,
    SUM(daytime_hours)                                AS daytime_hours,

    -- Days-weighted yields (architecture §4.3)
    SUM(y_r * days_in_month) / SUM(days_in_month)     AS y_r,
    SUM(y_a * days_in_month) / SUM(days_in_month)     AS y_a,
    SUM(y_f * days_in_month) / SUM(days_in_month)     AS y_f,

    -- FIX (Bug 1 + §4.3): y_r_sp as days-weighted daily average.
    -- After the monthly fix, y_r_sp there is already a daily average;
    -- weight by days_in_month to get the correct annual daily average.
    SUM(y_r_sp * days_in_month) / NULLIF(SUM(days_in_month), 0)             AS y_r_sp,

    -- Days totals for data quality
    SUM(days_in_month)                                AS days_in_year,
    SUM(days_with_data)                               AS months_with_data,

    -- FIX (§4.3): All ratio metrics must be days-weighted from monthly values,
    -- NOT recomputed from annual energy sums. Python uses this same approach.
    -- pr_pct / pr_sp_pct: exclude NULL months (< 0.5 kWh/m² threshold months)
    SUM(CASE WHEN pr_pct IS NOT NULL
          THEN pr_pct * days_in_month END)
      / NULLIF(SUM(CASE WHEN pr_pct IS NOT NULL
          THEN days_in_month END), 0)                 AS pr_pct,

    SUM(CASE WHEN pr_sp_pct IS NOT NULL
          THEN pr_sp_pct * days_in_month END)
      / NULLIF(SUM(CASE WHEN pr_sp_pct IS NOT NULL
          THEN days_in_month END), 0)                 AS pr_sp_pct,

    -- eta_* and d_t_pct: all months have values, simple weighted avg
    SUM(eta_array_pct * days_in_month) / NULLIF(SUM(days_in_month), 0)      AS eta_array_pct,
    SUM(eta_bos_pct * days_in_month) / NULLIF(SUM(days_in_month), 0)        AS eta_bos_pct,
    SUM(eta_system_pct * days_in_month) / NULLIF(SUM(days_in_month), 0)     AS eta_system_pct,
    SUM(d_t_pct * days_in_month) / NULLIF(SUM(days_in_month), 0)            AS d_t_pct,

    -- Corrected PRs: days-weighted, exclude NULL months
    SUM(CASE WHEN pr_dc_sp_theta_pct IS NOT NULL
          THEN pr_dc_sp_theta_pct * days_in_month END)
      / NULLIF(SUM(CASE WHEN pr_dc_sp_theta_pct IS NOT NULL
          THEN days_in_month END), 0)                 AS pr_dc_sp_theta_pct,

    SUM(CASE WHEN pr_dc_sp_theta_g_pct IS NOT NULL
          THEN pr_dc_sp_theta_g_pct * days_in_month END)
      / NULLIF(SUM(CASE WHEN pr_dc_sp_theta_g_pct IS NOT NULL
          THEN days_in_month END), 0)                 AS pr_dc_sp_theta_g_pct,

    SUM(CASE WHEN pr_dc_sp_theta_g_high_pct IS NOT NULL
          THEN pr_dc_sp_theta_g_high_pct * days_in_month END)
      / NULLIF(SUM(CASE WHEN pr_dc_sp_theta_g_high_pct IS NOT NULL
          THEN days_in_month END), 0)                 AS pr_dc_sp_theta_g_high_pct

  FROM monthly m
  JOIN {{ ref('seed_system_metadata') }} s USING (system_id)
  GROUP BY 1, 2
)

SELECT
  system_id,
  year,
  days_in_year,
  months_with_data,

  -- Group A
  h_i_kwh_m2,
  h_it_kwh,
  e_dc_kwh,
  e_ac_kwh,

  -- Group B
  y_r,
  y_a,
  y_f,

  -- Group C
  y_r - y_a                                          AS l_c,
  y_a - y_f                                          AS l_bos,

  -- Groups D & E — all pre-computed as days-weighted avgs in annual_agg
  eta_array_pct,
  eta_bos_pct,
  eta_system_pct,
  pr_pct,
  y_r_sp,
  d_t_pct,
  pr_sp_pct,

  -- Groups F, G, H — days-weighted from monthly
  pr_dc_sp_theta_pct,
  pr_dc_sp_theta_g_pct,
  pr_dc_sp_theta_g_high_pct,

  -- Group I
  e_loss_downtime,
  e_loss_ac,
  e_loss_temp,
  e_loss_irr,
  e_loss_irr_low,

  -- Group J
  num_valid_readings,
  num_total_readings,
  SAFE_DIVIDE(num_valid_readings, num_total_readings) * 100  AS data_availability_pct,
  daytime_hours

FROM annual_agg