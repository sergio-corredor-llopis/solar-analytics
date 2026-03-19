{{
  config(
    materialized='table',
    cluster_by=['system_id']
  )
}}

WITH daily AS (
  SELECT *
  FROM {{ ref('mart_daily_performance') }}
),

-- Re-aggregate corrected energy from readings for accurate monthly PRs
readings AS (
  SELECT
    r.system_id,
    DATE_TRUNC(DATE(r.reading_at), MONTH)             AS month,
    m.power_wp,
    m.area_m2,
    m.gamma_pct,
    m.module_efficiency,
    m.eta_a_g_200,

    -- Corrected energy terms (must re-aggregate from readings, not avg daily)
    SUM(CASE WHEN r.is_complete AND r.is_system_available
          AND (1.0 + (m.gamma_pct/100.0) * (r.temp_module - 25.0)) > 0
        THEN r.idc_a * r.vdc_v * r.interval_seconds / 3600.0
          / (1.0 + (m.gamma_pct/100.0) * (r.temp_module - 25.0))
        END) / 1000.0                                 AS e_dc_temp_corrected_kwh,

    SUM(CASE WHEN r.is_complete AND r.is_system_available
          AND (1.0 + (m.gamma_pct/100.0) * (r.temp_module - 25.0)) > 0
          AND r.irr_cell > 0
        THEN r.idc_a * r.vdc_v * r.interval_seconds / 3600.0
          / (1.0 + (m.gamma_pct/100.0) * (r.temp_module - 25.0))
          / (1.0 + ((m.eta_a_g_200/m.module_efficiency) - 1.0)
             / LN(0.2) * LN(r.irr_cell / 1000.0))
        END) / 1000.0                                 AS e_dc_temp_irr_corrected_kwh,

    SUM(CASE WHEN r.is_complete AND r.is_system_available
          AND r.irr_cell >= 300
          AND (1.0 + (m.gamma_pct/100.0) * (r.temp_module - 25.0)) > 0
          AND r.irr_cell > 0
        THEN r.idc_a * r.vdc_v * r.interval_seconds / 3600.0
          / (1.0 + (m.gamma_pct/100.0) * (r.temp_module - 25.0))
          / (1.0 + ((m.eta_a_g_200/m.module_efficiency) - 1.0)
             / LN(0.2) * LN(r.irr_cell / 1000.0))
        END) / 1000.0                                 AS e_dc_high_irr_corrected_kwh,

    SUM(CASE WHEN r.is_complete AND r.is_system_available
          AND r.irr_cell >= 300
        THEN r.irr_cell * r.interval_seconds / 3600.0
        END) / 1000.0                                 AS h_i_high_irr_kwh_m2

  FROM {{ ref('int_readings_complete') }} r
  JOIN {{ ref('seed_system_metadata') }} m USING (system_id)
  GROUP BY 1, 2, 3, 4, 5, 6, 7
),

monthly_agg AS (
  SELECT
    d.system_id,
    DATE_TRUNC(d.date, MONTH)                         AS month,
    MAX(m.power_wp)                                   AS power_wp,
    MAX(m.area_m2)                                    AS area_m2,

    -- Summable
    SUM(d.h_i_kwh_m2)                                 AS h_i_kwh_m2,
    SUM(d.h_it_kwh)                                   AS h_it_kwh,
    SUM(d.e_dc_kwh)                                   AS e_dc_kwh,
    SUM(d.e_ac_kwh)                                   AS e_ac_kwh,
    SUM(d.e_loss_downtime)                            AS e_loss_downtime,
    SUM(d.e_loss_ac)                                  AS e_loss_ac,
    SUM(d.e_loss_temp)                                AS e_loss_temp,
    SUM(d.e_loss_irr)                                 AS e_loss_irr,
    SUM(d.e_loss_irr_low)                             AS e_loss_irr_low,
    SUM(d.num_valid_readings)                         AS num_valid_readings,
    SUM(d.num_total_readings)                         AS num_total_readings,
    SUM(d.daytime_hours)                              AS daytime_hours,

    -- Daily-average yields
    SUM(d.h_i_kwh_m2) / COUNT(d.date)                AS y_r,
    SUM(d.e_dc_kwh) / (MAX(m.power_wp)/1000.0)
      / COUNT(d.date)                                 AS y_a,
    SUM(d.e_ac_kwh) / (MAX(m.power_wp)/1000.0)
      / COUNT(d.date)                                 AS y_f,
    -- h_i_available_kwh_m2 kept as monthly total for d_t_pct ratio;
    -- y_r_sp (daily avg) is derived in final SELECT by dividing by days_in_month
    SUM(COALESCE(d.y_r_sp, 0))                        AS h_i_available_kwh_m2,

    COUNT(d.date)                                     AS days_in_month,
    COUNTIF(d.num_valid_readings > 0)                 AS days_with_data

  FROM daily d
  JOIN {{ ref('seed_system_metadata') }} m USING (system_id)
  GROUP BY 1, 2
),

final AS (
  SELECT
    m.system_id,
    m.month,
    m.days_in_month,
    m.days_with_data,

    -- Group A
    m.h_i_kwh_m2,
    m.h_it_kwh,
    m.e_dc_kwh,
    m.e_ac_kwh,

    -- Group B
    m.y_r,
    m.y_a,
    m.y_f,

    -- Group C
    m.y_r - m.y_a                                    AS l_c,
    m.y_a - m.y_f                                    AS l_bos,

    -- Group D — recalculated from monthly sums
    SAFE_DIVIDE(m.e_dc_kwh, m.h_it_kwh) * 100        AS eta_array_pct,
    SAFE_DIVIDE(m.e_ac_kwh, m.e_dc_kwh) * 100        AS eta_bos_pct,
    SAFE_DIVIDE(m.e_ac_kwh, m.h_it_kwh) * 100        AS eta_system_pct,

    -- Group E — recalculated from monthly sums
    CASE WHEN m.h_i_kwh_m2 > 0 THEN
      NULLIF(LEAST(
        SAFE_DIVIDE(m.e_ac_kwh, m.power_wp/1000.0)
          / NULLIF(m.h_i_kwh_m2, 0) * 100
      , 110.0), 110.0)
    END                                               AS pr_pct,
    -- FIX (Bug 1): y_r_sp is the daily-average reference yield when available,
    -- matching the same normalisation as y_r. Divide monthly total by days_in_month.
    SAFE_DIVIDE(m.h_i_available_kwh_m2, m.days_in_month) AS y_r_sp,
    SAFE_DIVIDE(m.h_i_available_kwh_m2, m.h_i_kwh_m2) * 100  AS d_t_pct,
    CASE WHEN m.h_i_kwh_m2 > 0 THEN
      NULLIF(LEAST(
        SAFE_DIVIDE(
          SAFE_DIVIDE(m.e_ac_kwh, m.power_wp/1000.0),
          NULLIF(m.h_i_available_kwh_m2, 0)
        ) / NULLIF(SAFE_DIVIDE(m.h_i_available_kwh_m2, m.h_i_kwh_m2), 0) * 100
      , 110.0), 110.0)
    END                                               AS pr_sp_pct,

    -- Group F — "sp" = sin paradas: numerator uses available-period energy only,
    -- so denominator must also use h_i_available (y_r_sp), not h_i_kwh_m2 (y_r).
    CASE WHEN m.h_i_available_kwh_m2 > 0 THEN
      NULLIF(LEAST(
        SAFE_DIVIDE(r.e_dc_temp_corrected_kwh, m.power_wp/1000.0)
          / NULLIF(m.h_i_available_kwh_m2, 0) * 100
      , 110.0), 110.0)
    END                                               AS pr_dc_sp_theta_pct,

    -- Group G
    CASE WHEN m.h_i_available_kwh_m2 > 0 THEN
      NULLIF(LEAST(
        SAFE_DIVIDE(r.e_dc_temp_irr_corrected_kwh, m.power_wp/1000.0)
          / NULLIF(m.h_i_available_kwh_m2, 0) * 100
      , 110.0), 110.0)
    END                                               AS pr_dc_sp_theta_g_pct,

    -- Group H — high-irr subset: denominator is h_i at >=300 W/m² when available
    CASE WHEN r.h_i_high_irr_kwh_m2 > 0 THEN
      NULLIF(LEAST(
        SAFE_DIVIDE(r.e_dc_high_irr_corrected_kwh, m.power_wp/1000.0)
          / NULLIF(r.h_i_high_irr_kwh_m2, 0) * 100
      , 115.0), 115.0)
    END                                               AS pr_dc_sp_theta_g_high_pct,

    -- Group I
    -- FIX: e_loss_downtime must be recomputed from monthly totals.
    -- Python applies this formula at period level; summing daily results
    -- gives a different answer when the downtime ratio varies day to day.
    -- Formula: e_ac × (h_i_total - h_i_available) / h_i_available
    SAFE_DIVIDE(
      m.e_ac_kwh * (m.h_i_kwh_m2 - m.h_i_available_kwh_m2),
      NULLIF(m.h_i_available_kwh_m2, 0)
    )                                                 AS e_loss_downtime,
    m.e_loss_ac,
    m.e_loss_temp,
    m.e_loss_irr,
    -- FIX: e_loss_irr_low must be recomputed from monthly totals using readings CTE.
    -- Python formula: power_kWp × y_r_sp × (pr_high − pr_irr_corrected)
    -- Expanded: e_high_corrected × (h_i_available / h_i_high) − e_irr_corrected
    -- Can be legitimately negative when high-irr correction overshoots.
    -- NULL when no high-irr readings exist in the month.
    CASE WHEN r.h_i_high_irr_kwh_m2 > 0 THEN
      SAFE_DIVIDE(
        r.e_dc_high_irr_corrected_kwh * m.h_i_available_kwh_m2,
        r.h_i_high_irr_kwh_m2
      ) - r.e_dc_temp_irr_corrected_kwh
    END                                               AS e_loss_irr_low,

    -- Group J
    m.num_valid_readings,
    m.num_total_readings,
    SAFE_DIVIDE(m.num_valid_readings, m.num_total_readings) * 100  AS data_availability_pct,
    m.daytime_hours

  FROM monthly_agg m
  JOIN readings r USING (system_id, month)
)

SELECT * FROM final
