"""
generate_clear_sky_envelope.py
==============================
Generates seed_clear_sky_envelope.csv for the dbt solar analytics pipeline.

Purpose:
    Compute theoretical clear-sky irradiance on tilted surfaces for each
    (date, inclination_deg) pair across the full 10-year dataset.
    Used by int_irr_reliability_flags to:
      - Flag spikes: irr_cell > clear_sky_peak_wm2 × 1.15
      - Flag sustained lows: daily irr_cell_kwh < clear_sky_daily_kwh_m2 × 0.10

Output columns:
    date                  DATE
    inclination_deg       INT (5, 10, 30)
    clear_sky_peak_wm2    FLOAT  -- max 5-min POA irradiance for the day
    clear_sky_daily_kwh_m2 FLOAT -- sum of 5-min POA values × (5/60) / 1000

Location:
    ETSIDI, UPM Madrid
    Latitude:  40.3993° N
    Longitude: -3.7149° W (negative = West)
    Altitude:  660 m
    Timezone:  Europe/Madrid (CET/CEST — matches dataset)

Model:
    Ineichen clear-sky model (pvlib default, well-validated for Europe).
    Decomposition: Disc model → DNI from GHI.
    POA: isotropic sky model (simple, appropriate for seed/QA use).

Install:
    pip install pvlib pandas numpy

Usage:
    python generate_clear_sky_envelope.py
    → outputs: seed_clear_sky_envelope.csv  (in same directory)

Runtime: ~30 seconds.
"""

import pandas as pd
import numpy as np
import pvlib
from pvlib.location import Location
from pvlib.irradiance import get_total_irradiance

# ─── CONFIGURATION ────────────────────────────────────────────────────────────

LATITUDE  = 40.4056   # degrees North
LONGITUDE = -3.7000   # degrees East (negative = West)
ALTITUDE  = 660       # meters above sea level
TIMEZONE  = "Europe/Madrid"

# Date range matching the dataset
DATE_START = "2013-02-01"
DATE_END   = "2023-12-31"

# Inclination angles matching the three sensor groups
INCLINATIONS = [5, 10, 30]

# Azimuth: 0° = North (pvlib convention)
SURFACE_AZIMUTH = 160  # degrees (East)

# Resolution for intraday computation (aggregated to daily output)
FREQ = "5min"

OUTPUT_FILE = "seed_clear_sky_envelope.csv"

# ─── SETUP ────────────────────────────────────────────────────────────────────

site = Location(
    latitude=LATITUDE,
    longitude=LONGITUDE,
    tz=TIMEZONE,
    altitude=ALTITUDE,
    name="ETSIDI-Madrid"
)

print(f"Site: {site.name} | {LATITUDE}°N, {LONGITUDE}°E | {ALTITUDE}m")
print(f"Date range: {DATE_START} → {DATE_END}")
print(f"Inclinations: {INCLINATIONS}°")
print(f"Resolution: {FREQ}")
print()

# ─── GENERATE TIMESTAMPS ──────────────────────────────────────────────────────
# Use tz-aware timestamps in Europe/Madrid (handles CET/CEST automatically).
# pvlib handles DST transitions correctly via pytz/dateutil under the hood.

times = pd.date_range(
    start=DATE_START,
    end=DATE_END + " 23:55",
    freq=FREQ,
    tz=TIMEZONE
)

print(f"Total 5-min timestamps: {len(times):,}")

# ─── SOLAR POSITION ───────────────────────────────────────────────────────────

print("Computing solar positions...")
solar_pos = site.get_solarposition(times)
# solar_pos columns: apparent_zenith, zenith, apparent_elevation, elevation,
#                    azimuth, equation_of_time

# ─── CLEAR-SKY GHI ────────────────────────────────────────────────────────────

print("Computing clear-sky GHI (Ineichen model)...")
clearsky = site.get_clearsky(times, model="ineichen")
# clearsky columns: ghi, dni, dhi

# ─── POA IRRADIANCE PER INCLINATION ───────────────────────────────────────────

results = []

for tilt in INCLINATIONS:
    print(f"  Computing POA for {tilt}° tilt...")

    poa = get_total_irradiance(
        surface_tilt=tilt,
        surface_azimuth=SURFACE_AZIMUTH,
        solar_zenith=solar_pos["apparent_zenith"],
        solar_azimuth=solar_pos["azimuth"],
        dni=clearsky["dni"],
        ghi=clearsky["ghi"],
        dhi=clearsky["dhi"],
        model="isotropic"
    )
    # poa_global = beam + sky_diffuse + ground_diffuse

    poa_global = poa["poa_global"].clip(lower=0)  # no negative irradiance

    # Attach date for groupby
    df = pd.DataFrame({
        "poa_global": poa_global.values,
        "date": times.date
    })

    # Aggregate to daily
    # Peak: max 5-min reading of the day (W/m²)
    # Total: sum of 5-min readings × (5min / 60min/hr) / 1000 → kWh/m²
    daily = df.groupby("date").agg(
        clear_sky_peak_wm2=("poa_global", "max"),
        _sum_wm2=("poa_global", "sum")
    )
    daily["clear_sky_daily_kwh_m2"] = daily["_sum_wm2"] * (5 / 60) / 1000
    daily = daily.drop(columns=["_sum_wm2"])
    daily["inclination_deg"] = tilt
    daily.index.name = "date"
    daily = daily.reset_index()

    results.append(daily)

# ─── COMBINE AND EXPORT ───────────────────────────────────────────────────────

print("\nCombining results...")
output = pd.concat(results, ignore_index=True)

# Round to 3 decimal places (sub-watt precision is meaningless)
output["clear_sky_peak_wm2"]     = output["clear_sky_peak_wm2"].round(3)
output["clear_sky_daily_kwh_m2"] = output["clear_sky_daily_kwh_m2"].round(5)

# Sort for readability
output = output.sort_values(["inclination_deg", "date"]).reset_index(drop=True)

# Reorder columns to match dbt seed convention
output = output[["date", "inclination_deg", "clear_sky_peak_wm2", "clear_sky_daily_kwh_m2"]]

days_in_range = (pd.Timestamp(DATE_END) - pd.Timestamp(DATE_START)).days + 1
print(f"Output rows: {len(output):,}  (expected ~{len(INCLINATIONS) * days_in_range:,})")
print()
print("Sample (first 5 rows, 30° tilt):")
print(output[output["inclination_deg"] == 30].head().to_string(index=False))
print()
print("Sample stats (30° tilt):")
subset_30 = output[output["inclination_deg"] == 30]
print(f"  Peak W/m²:  min={subset_30['clear_sky_peak_wm2'].min():.1f}  "
      f"max={subset_30['clear_sky_peak_wm2'].max():.1f}  "
      f"mean={subset_30['clear_sky_peak_wm2'].mean():.1f}")
print(f"  Daily kWh:  min={subset_30['clear_sky_daily_kwh_m2'].min():.3f}  "
      f"max={subset_30['clear_sky_daily_kwh_m2'].max():.3f}  "
      f"mean={subset_30['clear_sky_daily_kwh_m2'].mean():.3f}")
print()

output.to_csv(OUTPUT_FILE, index=False)
print(f"✅ Written to: {OUTPUT_FILE}")
print(f"   File size estimate: ~{len(output) * 50 / 1024:.0f} KB")
