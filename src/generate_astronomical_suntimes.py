from astral import LocationInfo
from astral.sun import sun
import pandas as pd
from datetime import date, timedelta

# UPM campus, Madrid
location = LocationInfo(
    name="UPM Madrid",
    region="Spain",
    timezone="Europe/Madrid",
    latitude=40.4069,
    longitude=-3.7003
)

rows = []
d = date(2013, 1, 1)
end = date(2023, 12, 31)

while d <= end:
    s = sun(location.observer, date=d, tzinfo=location.timezone)
    rows.append({
        "date": d.isoformat(),
        "sunrise_at": s["sunrise"].strftime("%Y-%m-%d %H:%M:%S"),
        "sunset_at":  s["sunset"].strftime("%Y-%m-%d %H:%M:%S"),
    })
    d += timedelta(days=1)

df = pd.DataFrame(rows)
df.to_csv("seed_astronomical_sun_times.csv", index=False)
print(f"Done: {len(df)} rows")
print(df.head(3))
print(df.tail(3))