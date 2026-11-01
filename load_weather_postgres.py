"""
Step 2 of the weather pipeline: read the raw JSON documents from MongoDB,
flatten them into one row per hour, and load them into the PostgreSQL
`weather` table (serving layer).

- ON CONFLICT DO NOTHING on the primary key makes reruns safe.
- Rows are validated against the data dictionary before loading.

Requires: pip install pymongo psycopg2-binary
"""

import logging
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values
from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017"
MONGO_DB, MONGO_COLL = "madrid_traffic_raw", "weather_raw"

PG_DSN = "host=localhost port=5432 dbname=madrid_traffic user=traffic password=traffic"

DDL = """
CREATE TABLE IF NOT EXISTS weather (
    "timestamp"  TIMESTAMP PRIMARY KEY,           -- hourly, Europe/Madrid local time
    temperature  NUMERIC(4,1),                    -- degrees Celsius at 2 m
    rain         NUMERIC(5,1) CHECK (rain >= 0)   -- precipitation in mm over the hour
);
"""

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("weather_load")


def flatten(doc):
    """Turn one monthly document (column arrays) into a list of hourly rows."""
    h = doc["payload"]["hourly"]
    rows, rejected = [], 0
    for ts, temp, rain in zip(h["time"], h["temperature_2m"], h["precipitation"]):
        # Validation rules from the data dictionary
        if temp is not None and not (-20 <= temp <= 50):
            rejected += 1
            continue
        if rain is not None and rain < 0:
            rejected += 1
            continue
        rows.append((datetime.fromisoformat(ts), temp, rain))
    return rows, rejected


def main():
    mongo = MongoClient(MONGO_URI)[MONGO_DB][MONGO_COLL]
    conn = psycopg2.connect(PG_DSN)

    with conn, conn.cursor() as cur:
        cur.execute(DDL)
        total_rows = total_rejected = 0

        for doc in mongo.find({"source": "open-meteo"}).sort("month", 1):
            rows, rejected = flatten(doc)
            execute_values(
                cur,
                'INSERT INTO weather ("timestamp", temperature, rain) VALUES %s '
                'ON CONFLICT ("timestamp") DO NOTHING',
                rows,
            )
            total_rows += len(rows)
            total_rejected += rejected
            log.info("%s: %d rows prepared, %d rejected", doc["month"], len(rows), rejected)

        cur.execute("SELECT COUNT(*), MIN(\"timestamp\"), MAX(\"timestamp\") FROM weather")
        count, first, last = cur.fetchone()

    log.info("Prepared %d rows, rejected %d. Table now has %d rows (%s to %s).",
             total_rows, total_rejected, count, first, last)


if __name__ == "__main__":
    main()