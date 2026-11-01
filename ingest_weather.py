"""
Step 1 of the weather pipeline: fetch hourly weather JSON for Madrid and
land it UNCHANGED in MongoDB (raw zone).

- One request per month, matching the monthly traffic files.
- Each API response is stored as one document, with ingestion metadata.
- A unique index on (source, month) makes the script idempotent:
  re-running it never creates duplicates.

Requires: pip install requests pymongo
"""

import calendar
import logging
from datetime import date, datetime, timezone

import requests
from pymongo import MongoClient, ASCENDING

# ---------------------------------------------------------------- config
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "madrid_traffic_raw"
COLLECTION = "weather_raw"

API_URL = "https://archive-api.open-meteo.com/v1/archive"
LATITUDE = 40.4168     # central Madrid (Puerta del Sol)
LONGITUDE = -3.7038
HOURLY_VARS = "temperature_2m,precipitation"

START = (2025, 1)      # first month (year, month)
END = (2026, 8)        # last month, inclusive

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("weather_ingest")


def months(start, end):
    """Yield (year, month) tuples from start to end, inclusive."""
    y, m = start
    while (y, m) <= end:
        yield y, m
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def fetch_month(year, month, retries=3):
    """Call the API for one month. Retries on network errors or 5xx responses."""
    last_day = calendar.monthrange(year, month)[1]
    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "start_date": date(year, month, 1).isoformat(),
        "end_date": date(year, month, last_day).isoformat(),
        "hourly": HOURLY_VARS,
        "timezone": "Europe/Madrid",
    }
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(API_URL, params=params, timeout=60)
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            log.warning("Attempt %d/%d failed for %d-%02d: %s", attempt, retries, year, month, e)
    raise RuntimeError(f"Could not fetch weather for {year}-{month:02d}")


def main():
    client = MongoClient(MONGO_URI)
    coll = client[DB_NAME][COLLECTION]

    # Idempotency: one document per source per month.
    coll.create_index([("source", ASCENDING), ("month", ASCENDING)], unique=True)

    for year, month in months(START, END):
        month_key = f"{year}-{month:02d}"

        if coll.count_documents({"source": "open-meteo", "month": month_key}, limit=1):
            log.info("Skipping %s (already ingested)", month_key)
            continue

        payload = fetch_month(year, month)

        # Basic sanity check before landing: the hourly arrays must exist
        # and have the same length.
        hourly = payload.get("hourly", {})
        lengths = {k: len(v) for k, v in hourly.items()}
        if not hourly or len(set(lengths.values())) != 1:
            log.error("Malformed payload for %s: %s", month_key, lengths)
            continue

        coll.insert_one({
            "source": "open-meteo",
            "month": month_key,
            "ingested_at": datetime.now(timezone.utc),
            "request": {"lat": LATITUDE, "lon": LONGITUDE, "hourly": HOURLY_VARS},
            "payload": payload,          # raw API response, untouched
        })
        log.info("Landed %s (%d hourly records)", month_key, len(hourly["time"]))

    log.info("Done. %d monthly documents in %s.%s",
             coll.count_documents({}), DB_NAME, COLLECTION)


if __name__ == "__main__":
    main()