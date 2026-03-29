#!/usr/bin/env python3
import pathlib
import csv
import re
import logging
import sqlite3
import requests
from collections import Counter
from timezonefinder import TimezoneFinder

OURAIRPORTS_URL = "https://davidmegginson.github.io/ourairports-data/"
PWD = pathlib.Path(__file__).resolve().parent
AIRPORT_DB_FILE = PWD / "airports.sqb"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

icao_pattern = re.compile(r"^[A-Z]{2}[A-Z0-9]{2}$")


def _fetch_csv(session: requests.Session, url: str) -> list[dict]:
    response = session.get(url)
    response.encoding = "utf-8"
    return list(csv.DictReader(response.text.splitlines(), delimiter=","))


def _ensure_schema(db_connection: sqlite3.Connection) -> None:
    cursor = db_connection.cursor()
    cursor.execute(
        "SELECT count(name) FROM sqlite_master "
        "WHERE type='table' AND name='airports'"
    )
    if cursor.fetchone()[0] == 0:
        with open(PWD / "airports.sql", encoding="utf-8") as f:
            db_connection.executescript(f.read())
    cursor.close()


def _iter_valid_airports(
    airports: list[dict],
    duplicate_icaos: set[str],
    tf: TimezoneFinder,
    countries: dict[str, str],
):
    for _row in airports:
        if _row["type"] == "closed":
            continue

        _iata = _row["iata_code"]
        if len(_iata) not in (0, 3):
            # OurAirports uses "0" as a placeholder for a missing IATA code
            if _iata == "0":
                _row["iata_code"] = ""
            else:
                continue

        if (
            _row["gps_code"] in duplicate_icaos
            and _row["ident"] != _row["gps_code"]
        ):
            logger.info(
                f"ignoring duplicate entry {_row['ident']} for "
                f"{_row['gps_code']} / {_row['iata_code']}."
            )
            continue

        _longitude = float(_row["longitude_deg"])
        _latitude = float(_row["latitude_deg"])
        _timezone = tf.timezone_at(lng=_longitude, lat=_latitude)

        if _timezone is None:
            logger.warning(f"timezone info unknown: {_row['gps_code']}")

        yield (
            _row["name"],
            _row["municipality"],
            countries[_row["iso_country"]],
            _row["iata_code"],
            _row["gps_code"],
            _latitude,
            _longitude,
            _row["elevation_ft"],
            _timezone,
        )


def main() -> None:
    with requests.Session() as session:
        countries = {
            _row["code"]: _row["name"]
            for _row in _fetch_csv(session, f"{OURAIRPORTS_URL}/countries.csv")
        }
        raw_airports = _fetch_csv(session, f"{OURAIRPORTS_URL}/airports.csv")

    airports = [
        _row for _row in raw_airports if icao_pattern.match(_row["gps_code"])
    ]

    icao_count = Counter(_row["gps_code"] for _row in airports)
    duplicate_icaos = {icao for icao, count in icao_count.items() if count > 1}

    tf = TimezoneFinder()

    with sqlite3.connect(AIRPORT_DB_FILE) as db_connection:
        _ensure_schema(db_connection)
        db_connection.executemany(
            "REPLACE INTO airports(Name, City, Country, IATA, ICAO, Latitude, "
            "Longitude, Altitude, Timezone) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
            _iter_valid_airports(airports, duplicate_icaos, tf, countries),
        )
        db_connection.commit()
        db_connection.execute("VACUUM")


if __name__ == "__main__":
    main()
