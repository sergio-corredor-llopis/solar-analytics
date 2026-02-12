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

## Processing Notes
- 107 columns total (not all used)
- 15-minute intervals
- Multiple inverters (identified by ID in parentheses)