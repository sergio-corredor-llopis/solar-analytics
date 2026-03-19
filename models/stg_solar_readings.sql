with source as (
    select * from {{ source('solar_raw', 'raw_solar_readings') }}
),

renamed as (
    select

        -- Timestamps & partitioning
        timestamp_micros(cast(timestamp / 1000 as INT64))  as reading_at,
        Fecha                                              as fecha_int,
        Hora                                               as hora_int,
        year                                               as year,
        month                                              as month,
        source_file                                        as source_file,

        -- -----------------------------------------------------------------------
        -- Grid / system flag
        -- VFG = Verfügbarkeit (availability flag) — keep raw for now
        -- -----------------------------------------------------------------------
        VFG                                      as vfg,

        -- -----------------------------------------------------------------------
        -- Irradiance sensors [W/m²]
        -- G_H = horizontal plane | G_M = module plane
        -- G_M3 is the dead irradiance sensor (confirmed in validation)
        -- -----------------------------------------------------------------------
        G_H1__47596__                            as g_h1,
        G_H2__47597__                            as g_h2,
        G_M1__47596__                            as g_m1,
        G_M2__47596__                            as g_m2,
        G_M3__47596__                            as g_m3,      -- dead sensor ⚠️

        -- -----------------------------------------------------------------------
        -- Temperature sensors [°C]
        -- T_U0 = ambient | T_M = module | T_WR = inverter
        -- T_M2 failed August 2021 | T_M3 dead throughout
        -- -----------------------------------------------------------------------
        T_U0__47596__                            as t_ambient,
        T_M1__47596__                            as t_module_1,
        T_M2__47596__                            as t_module_2,   -- failed Aug 2021 ⚠️
        T_M3__47596__                            as t_module_3,   -- dead sensor ⚠️
        T_M4__47596__                            as t_module_4,
        T_M5__47597__                            as t_module_5,
        T_M6__47597__                            as t_module_6,

        -- -----------------------------------------------------------------------
        -- Wind speed [m/s]
        -- -----------------------------------------------------------------------
        F_L0__47596__                            as wind_speed,

        -- -----------------------------------------------------------------------
        -- DC current [A] — all 12 inverters, device 47596
        -- -----------------------------------------------------------------------
        I_DC_1_47596__                           as i_dc_1,
        I_DC_2_47596__                           as i_dc_2,
        I_DC_3_47596__                           as i_dc_3,
        I_DC_4_47596__                           as i_dc_4,
        I_DC_5_47596__                           as i_dc_5,
        I_DC_6_47596__                           as i_dc_6,
        I_DC_7_47596__                           as i_dc_7,
        I_DC_8_47596__                           as i_dc_8,
        I_DC_9_47596__                           as i_dc_9,
        I_DC_10_47596__                          as i_dc_10,
        I_DC_11_47596__                          as i_dc_11,
        I_DC_12_47596__                          as i_dc_12,

        -- -----------------------------------------------------------------------
        -- DC voltage [V] — all 12 inverters, device 47596
        -- -----------------------------------------------------------------------
        U_DC_1_47596__                           as u_dc_1,
        U_DC_2_47596__                           as u_dc_2,
        U_DC_3_47596__                           as u_dc_3,
        U_DC_4_47596__                           as u_dc_4,
        U_DC_5_47596__                           as u_dc_5,
        U_DC_6_47596__                           as u_dc_6,
        U_DC_7_47596__                           as u_dc_7,
        U_DC_8_47596__                           as u_dc_8,
        U_DC_9_47596__                           as u_dc_9,
        U_DC_10_47596__                          as u_dc_10,
        U_DC_11_47596__                          as u_dc_11,
        U_DC_12_47596__                          as u_dc_12,

        -- -----------------------------------------------------------------------
        -- AC current [A] — all 12 inverters, device 47596
        -- -----------------------------------------------------------------------
        I_AC_1_47596__                           as i_ac_1,
        I_AC_2_47596__                           as i_ac_2,
        I_AC_3_47596__                           as i_ac_3,
        I_AC_4_47596__                           as i_ac_4,
        I_AC_5_47596__                           as i_ac_5,
        I_AC_6_47596__                           as i_ac_6,
        I_AC_7_47596__                           as i_ac_7,
        I_AC_8_47596__                           as i_ac_8,
        I_AC_9_47596__                           as i_ac_9,
        I_AC_10_47596__                          as i_ac_10,
        I_AC_11_47596__                          as i_ac_11,
        I_AC_12_47596__                          as i_ac_12,

        -- -----------------------------------------------------------------------
        -- AC voltage [V] — all 12 inverters, device 47596
        -- -----------------------------------------------------------------------
        U_AC_1_47596__                           as u_ac_1,
        U_AC_2_47596__                           as u_ac_2,
        U_AC_3_47596__                           as u_ac_3,
        U_AC_4_47596__                           as u_ac_4,
        U_AC_5_47596__                           as u_ac_5,
        U_AC_6_47596__                           as u_ac_6,
        U_AC_7_47596__                           as u_ac_7,
        U_AC_8_47596__                           as u_ac_8,
        U_AC_9_47596__                           as u_ac_9,
        U_AC_10_47596__                          as u_ac_10,
        U_AC_11_47596__                          as u_ac_11,
        U_AC_12_47596__                          as u_ac_12,

        -- -----------------------------------------------------------------------
        -- AC power [W] — all 12 inverters, device 47596
        -- -----------------------------------------------------------------------
        P_AC_1_47596__                           as p_ac_1,
        P_AC_2_47596__                           as p_ac_2,
        P_AC_3_47596__                           as p_ac_3,
        P_AC_4_47596__                           as p_ac_4,
        P_AC_5_47596__                           as p_ac_5,
        P_AC_6_47596__                           as p_ac_6,
        P_AC_7_47596__                           as p_ac_7,
        P_AC_8_47596__                           as p_ac_8,
        P_AC_9_47596__                           as p_ac_9,
        P_AC_10_47596__                          as p_ac_10,
        P_AC_11_47596__                          as p_ac_11,
        P_AC_12_47596__                          as p_ac_12,

        -- -----------------------------------------------------------------------
        -- Inverter temperature [°C] — all 12 inverters, device 47596
        -- T_WR_11 confirmed hottest inverter in validation
        -- -----------------------------------------------------------------------
        T_WR_1_47596__                           as t_wr_1,
        T_WR_2_47596__                           as t_wr_2,
        T_WR_3_47596__                           as t_wr_3,
        T_WR_4_47596__                           as t_wr_4,
        T_WR_5_47596__                           as t_wr_5,
        T_WR_6_47596__                           as t_wr_6,
        T_WR_7_47596__                           as t_wr_7,
        T_WR_8_47596__                           as t_wr_8,
        T_WR_9_47596__                           as t_wr_9,
        T_WR_10_47596__                          as t_wr_10,
        T_WR_11_47596__                          as t_wr_11,   -- hottest ⚠️
        T_WR_12_47596__                          as t_wr_12,

        -- -----------------------------------------------------------------------
        -- Cumulative energy [Wh] — all 12 inverters, device 47596
        -- -----------------------------------------------------------------------
        E_Total_1_47596__                        as e_total_1,
        E_Total_2_47596__                        as e_total_2,
        E_Total_3_47596__                        as e_total_3,
        E_Total_4_47596__                        as e_total_4,
        E_Total_5_47596__                        as e_total_5,
        E_Total_6_47596__                        as e_total_6,
        E_Total_7_47596__                        as e_total_7,
        E_Total_8_47596__                        as e_total_8,
        E_Total_9_47596__                        as e_total_9,
        E_Total_10_47596__                       as e_total_10,
        E_Total_11_47596__                       as e_total_11,
        E_Total_12_47596__                       as e_total_12,

        -- -----------------------------------------------------------------------
        -- Device 47597 — System 13 (university numbering), 5° inclination
        -- This is a SEPARATE system, NOT a duplicate of system 1 from device 47596
        -- See dbt_Architecture.md §0d for complete column mapping
        -- -----------------------------------------------------------------------
        I_DC_1_47597__                           as i_dc_13,
        U_DC_1_47597__                           as u_dc_13,
        I_AC_1_47597__                           as i_ac_13,
        U_AC_1_47597__                           as u_ac_13,
        P_AC_1_47597__                           as p_ac_13,
        T_WR_1_47597__                           as t_wr_13,
        E_Total_1_47597__                        as e_total_13

    from source
    where timestamp is not null  -- exclude empty trailing rows from CSV export
)

select * from renamed