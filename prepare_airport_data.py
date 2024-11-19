#!/usr/bin/env python
# encoding: utf-8
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

countries = {}
icao_pattern = re.compile("^[A-Z]{4}$")

with requests.Session() as s:
    _response = s.get(f"{OURAIRPORTS_URL}/countries.csv")
    _response.encoding = "utf-8"
for _row in csv.DictReader(_response.text.splitlines(), delimiter=","):
    countries[_row["code"]] = _row["name"]

with sqlite3.connect(AIRPORT_DB_FILE) as db_connection:
    _cursor = db_connection.cursor()
    _cursor.execute(
        "SELECT count(name) FROM sqlite_master "
        "WHERE type='table' AND name='airports'"
    )
    if _cursor.fetchone()[0] == 0:
        with open(PWD / "airports.sql", encoding="utf-8") as f:
            db_connection.executescript(f.read())

    with requests.Session() as s:
        _response = s.get(f"{OURAIRPORTS_URL}/airports.csv")
        _response.encoding = "utf-8"
    _reader = csv.DictReader(_response.text.splitlines(), delimiter=",")
    airports = [
        row
        for row in _reader
        if icao_pattern.match(row["gps_code"]) is not None
    ]

    icao_count = Counter(row["gps_code"] for row in airports)
    duplicate_icaos = {icao for icao, count in icao_count.items() if count > 1}

    tf = TimezoneFinder()
    for _row in airports:
        if not len(_row["iata_code"]) in (0, 3):
            if _row["iata_code"] == "0":
                _row["iata_code"] = ""
            else:
                continue
        if _row["type"] == "closed":
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
        _country = countries[_row["iso_country"]]
        if _timezone is None:
            logger.warning("timezone info unknown: {}".format(_row["gps_code"]))
        _cursor.execute(
            "REPLACE INTO airports(Name, City, Country, IATA, ICAO, Latitude, "
            "Longitude, Altitude, Timezone) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                _row["name"],
                _row["municipality"],
                _country,
                _row["iata_code"],
                _row["gps_code"],
                _latitude,
                _longitude,
                _row["elevation_ft"],
                _timezone,
            ),
        )
    db_connection.commit()

with sqlite3.connect(AIRPORT_DB_FILE) as db_connection:
    cursor = db_connection.cursor()
    cursor.execute("VACUUM")
