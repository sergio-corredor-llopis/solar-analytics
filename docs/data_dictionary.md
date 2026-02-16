# Solar Data Dictionary

## Source Files
- **Count:** 131 monthly CSVs + 2 auxiliary CSVs + 13 auxiliary XLSX
- **Date range:** February 2013 → December 2023
- **Naming pattern:** `YYYY MM [Spanish Month] Todos los Inversores.csv`

## File Format
| Property | Value |
|----------|-------|
| Encoding | UTF-16 LE |
| Separator | Tab |
| Decimal | European (comma) |
| Date format | DD.MM.YYYY |
| Time format | H:MM:SS |
| Missing values | `-` |
| Header row | 6 (skip rows 1-5) |
| Data starts | Row 10 (skip rows 7-9 for units) |

## Column Categories
| Prefix | Meaning | Example |
|--------|---------|---------|
| Fecha | Date | 01.02.2013 |
| Hora | Time | 0:15:00 |
| G_H, G_M | Irradiance (Global Horizontal/Module) | G_H1, G_M1 |
| T_U, T_M | Temperature (Ambient/Module) | T_U0, T_M1 |
| I_DC | DC Current (per inverter) | I_DC(1(47596)) |
| U_DC | DC Voltage (per inverter) | U_DC(1(47596)) |
| I_AC | AC Current (per inverter) | I_AC(1(47596)) |
| U_AC | AC Voltage (per inverter) | U_AC(1(47596)) |
| P_AC | AC Power (per inverter) | P_AC(1(47596)) |
| T_WR | Inverter temperature | T_WR(1(47596)) |
| E_Total | Total Energy (per inverter) | E_Total(1(47596)) |

## Column Summary
- Total columns: 108 (verified across all 131 files)
- Fecha (date) + Hora (time) = 2 identifier columns
- 106 measurement columns across categories:
  - G_H, G_M: Irradiance
  - T_U, T_M: Temperature
  - I_DC, U_DC: DC current/voltage (per inverter)
  - I_AC, U_AC: AC current/voltage (per inverter)
  - P_AC: AC power (per inverter)
  - T_WR: Inverter temperature
  - E_Total: Cumulative energy (per inverter)
  - F_L0: [check what this is — possibly frequency?]
  - VFG: [check what this is]

## Processing Notes
- 107 columns total (not all used)
- Multiple inverters (identified by ID in parentheses)
- First 5 files (~Feb 2013 - Jun 2013?): 15-minute intervals, ~4 MB each
- Remaining 126 files: 5-minute intervals, ~10 MB each
- Total estimated size: (5 × 4MB) + (126 × 10MB) ≈ 1.28 GB of CSVs

## Measurement Intervals
|
 Period 
|
 Interval 
|
 Source 
|
|
--------
|
----------
|
--------
|
|
 01/01/2013 00:00:00 → 02/07/2013 21:44:59 
|
 15 minutes 
|
 Auxiliary file 
|
|
 02/07/2013 21:45:00 → 31/12/2023 
|
 5 minutes 
|
 Auxiliary file 
|

⚠️ IMPORTANT: The transition happens MID-FILE in July 2013.
The July 2013 CSV contains BOTH 15-min and 5-min intervals.

## Conversion Results
- Total files converted: 131/131
- Total rows: [1,098,114]
- No anomalies

## Unused/Unknown Columns
|
 Column 
|
 Units Row 
|
 Data Present 
|
 Used in Analysis 
|
 Notes 
|
|
--------
|
-----------
|
-------------
|
-----------------
|
-------
|
|
 VFG    
|
 Verfügbarkeit (Availability)
|
 ❌ Empty in all files
|
 ❌ No
|
|
 F_L0   
|
 Likely line frequency
|
 ✅ Has data 
|
 ❌ No 
|
 Unknown parameter, not used in original UPM analysis 
|

## Notes
- "valor medio" = mean value (Spanish)
- Both columns preserved in Parquet for completeness
- Can be dropped in the dbt staging layer (Week 9-10)