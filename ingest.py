# ingest.py
# Loads the Madrid traffic data (Sept 2026) into our Postgres database.
# Order: tables -> calendar -> sensors -> readings -> weather
#
# run with: python3 ingest.py
# for the small sample: python3 ingest.py raw_data/<sample file>

import io
import json
import logging
import os
import sys
import time
from pathlib import Path

import pandas as pd
import psycopg2

BASE = Path(__file__).resolve().parent
RAW = BASE / "raw_data"
SQL = BASE / "sql"

# full traffic file is on the Google Drive (too big for github)
if len(sys.argv) > 1:
    TRAFFIC_FILE = Path(sys.argv[1])
else:
    TRAFFIC_FILE = RAW / "traffic_readings_2026_09.csv"
SENSORS_FILE = RAW / "sensor_locations_2026_09.csv"
WEATHER_FILE = RAW / "weather_2026_09.json"

START = "2026-09-01"
END = "2026-09-30"
CHUNK = 500000  # the traffic csv is huge so we read it in pieces

# db login - set PGPASSWORD etc in terminal if yours is different
DB = {
    "host": os.getenv("PGHOST", "localhost"),
    "port": os.getenv("PGPORT", "5432"),
    "dbname": os.getenv("PGDATABASE", "madrid_traffic"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", ""),
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler(BASE / "ingest.log", mode="w"), logging.StreamHandler()],
)
log = logging.getLogger()


def to_num(df, col):
    # madrid uses commas for decimals sometimes, bad values -> NaN
    if col not in df.columns:
        return pd.Series(float("nan"), index=df.index)
    return pd.to_numeric(df[col].astype(str).str.replace(",", "."), errors="coerce")


def find_header(path):
    # the sensors file has a random title line before the actual header
    with open(path, encoding="utf-8", errors="replace") as f:
        for i, line in enumerate(f):
            if "id" in line.lower().replace('"', "").split(";"):
                return i
            if i > 10:
                return 0
    return 0


def read_madrid_csv(path, **kwargs):
    df = pd.read_csv(path, sep=";", dtype=str, encoding="utf-8",
                     encoding_errors="replace", skiprows=find_header(path), **kwargs)
    return df


def fix_cols(df):
    df.columns = [c.strip().strip('"').lower() for c in df.columns]
    return df


def copy_df(cur, table, df):
    # COPY is way faster than inserting row by row
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)
    cur.copy_expert(f"COPY {table} ({', '.join(df.columns)}) FROM STDIN WITH CSV", buf)


def make_tables(conn):
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS readings, weather, calendar, sensors CASCADE")
    cur.execute((SQL / "create_tables.sql").read_text())
    conn.commit()
    log.info("tables created")


def make_calendar(conn):
    cur = conn.cursor()
    cur.execute((SQL / "generate_calendar.sql").read_text())
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM calendar")
    log.info(f"calendar: {cur.fetchone()[0]} days")


def load_sensors(conn):
    raw = fix_cols(read_madrid_csv(SENSORS_FILE))
    total = len(raw)

    sensors = pd.DataFrame({
        "id": to_num(raw, "id"),
        "name": raw["nombre"],
        "district": raw["distrito"],
        "latitude": to_num(raw, "latitud"),
        "longitude": to_num(raw, "longitud"),
    })
    sensors = sensors.dropna(subset=["id"]).drop_duplicates("id")
    sensors["id"] = sensors["id"].astype(int)

    copy_df(conn.cursor(), "sensors", sensors)
    conn.commit()
    log.info(f"sensors: {len(sensors)} loaded out of {total}")
    return set(sensors["id"])


def load_readings(conn, sensor_ids):
    cur = conn.cursor()
    # temp table so we can skip duplicates with ON CONFLICT
    cur.execute("CREATE TEMP TABLE tmp (LIKE readings) ON COMMIT DELETE ROWS")

    start = pd.Timestamp(START)
    end = pd.Timestamp(END) + pd.Timedelta(days=1)
    counts = {"read": 0, "loaded": 0, "bad_row": 0, "unknown_sensor": 0,
              "wrong_month": 0, "duplicate": 0, "error_flag": 0}

    for n, raw in enumerate(read_madrid_csv(TRAFFIC_FILE, chunksize=CHUNK), 1):
        raw = fix_cols(raw)
        counts["read"] += len(raw)

        df = pd.DataFrame({
            "sensor_id": to_num(raw, "id"),
            "timestamp": pd.to_datetime(raw["fecha"], errors="coerce"),
            "intensity": to_num(raw, "intensidad"),
            "occupancy": to_num(raw, "ocupacion"),
            "load": to_num(raw, "carga"),
            "speed": to_num(raw, "vmed"),
            "error": raw["error"].str.strip() if "error" in raw else None,
        })

        # rows with no id or a broken date are useless
        bad = df["sensor_id"].isna() | df["timestamp"].isna()
        counts["bad_row"] += bad.sum()
        df = df[~bad]

        # sensor has to exist in the sensors table (foreign key)
        unknown = ~df["sensor_id"].astype(int).isin(sensor_ids)
        counts["unknown_sensor"] += unknown.sum()
        df = df[~unknown]

        # only keep september
        outside = (df["timestamp"] < start) | (df["timestamp"] >= end)
        counts["wrong_month"] += outside.sum()
        df = df[~outside].copy()

        # negative counts / occupancy over 100% can't be real, so blank them
        # (rest of the row is still fine so we keep it)
        df.loc[df["intensity"] < 0, "intensity"] = None
        df.loc[df["speed"] < 0, "speed"] = None
        df.loc[(df["occupancy"] < 0) | (df["occupancy"] > 100), "occupancy"] = None
        df.loc[(df["load"] < 0) | (df["load"] > 100), "load"] = None

        # we keep rows where the sensor flagged an error, analysts can filter error = 'N'
        counts["error_flag"] += (df["error"].fillna("N") != "N").sum()

        df["sensor_id"] = df["sensor_id"].astype(int)
        copy_df(cur, "tmp", df)
        cur.execute("INSERT INTO readings SELECT * FROM tmp ON CONFLICT DO NOTHING")
        counts["loaded"] += cur.rowcount
        counts["duplicate"] += len(df) - cur.rowcount
        conn.commit()
        log.info(f"chunk {n} done - {counts['loaded']:,} readings so far")

    dropped = counts["bad_row"] + counts["unknown_sensor"] + counts["wrong_month"] + counts["duplicate"]
    log.info(f"readings: read {counts['read']:,}, loaded {counts['loaded']:,}, dropped {dropped:,}")
    log.info(f"  dropped because: bad id/date {counts['bad_row']:,}, unknown sensor {counts['unknown_sensor']:,}, "
             f"not september {counts['wrong_month']:,}, duplicate {counts['duplicate']:,}")
    log.info(f"  rows with an error flag (kept): {counts['error_flag']:,}")

    # sanity check that every row is accounted for
    if counts["read"] != counts["loaded"] + dropped:
        log.warning("row counts don't add up!")


def load_weather(conn):
    with open(WEATHER_FILE) as f:
        data = json.load(f)["hourly"]

    weather = pd.DataFrame({
        "timestamp": pd.to_datetime(data["time"]),
        "temperature": data["temperature_2m"],
        "rain": data["rain"],
    }).drop_duplicates("timestamp")

    copy_df(conn.cursor(), "weather", weather)
    conn.commit()
    log.info(f"weather: {len(weather)} hours loaded")


def run(name, func, conn, *args):
    # if one step breaks we log it and move on instead of crashing everything
    try:
        return func(conn, *args)
    except FileNotFoundError as e:
        log.error(f"{name}: can't find {e.filename}")
        if name == "readings":
            log.error("get traffic_readings_2026_09.csv from the drive and put it in raw_data/")
    except Exception as e:
        log.exception(f"{name} failed: {e}")
    conn.rollback()
    return False


def main():
    t = time.time()
    try:
        conn = psycopg2.connect(**DB)
    except Exception as e:
        log.error(f"couldn't connect to postgres: {e}")
        sys.exit(1)

    failed = []
    if run("tables", make_tables, conn) is False:
        sys.exit(1)
    if run("calendar", make_calendar, conn) is False:
        failed.append("calendar")

    sensor_ids = run("sensors", load_sensors, conn)
    if sensor_ids is False:
        failed += ["sensors", "readings"]
        log.error("skipping readings since sensors didn't load")
    elif run("readings", load_readings, conn, sensor_ids) is False:
        failed.append("readings")

    if run("weather", load_weather, conn) is False:
        failed.append("weather")

    cur = conn.cursor()
    log.info("final counts:")
    for table in ["sensors", "calendar", "weather", "readings"]:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        log.info(f"  {table}: {cur.fetchone()[0]:,}")
    conn.close()

    log.info(f"done in {time.time() - t:.1f}s" + (f", failed: {failed}" if failed else ""))


if __name__ == "__main__":
    main()
