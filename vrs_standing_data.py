#!/usr/bin/env python3
import pathlib
import glob
import csv
import sqlite3
from opensky_utils import validated_callsign

PWD = pathlib.Path(__file__).resolve().parent
ROUTES_DB_FILE = PWD / "vrs_routes.sqb"


def refresh_database():
    with sqlite3.connect(ROUTES_DB_FILE) as db_connection:
        _cursor = db_connection.cursor()
        _cursor.execute(
            "SELECT count(name) FROM sqlite_master "
            "WHERE type='table' AND name='flight_routes'"
        )
        if _cursor.fetchone()[0] == 0:
            with open(PWD / "vrs_routes.sql", encoding="utf-8") as f:
                db_connection.executescript(f.read())
        for _file_name in glob.glob(
            "../standing-data/routes/schema-01/*/*.csv"
        ):
            with open(_file_name, encoding="utf-8-sig") as csv_file:
                reader = csv.DictReader(csv_file)
                for _row in reader:
                    _callsign_info = validated_callsign(_row["Callsign"])
                    if _callsign_info is None:
                        continue
                    _cursor.execute(
                        "REPLACE INTO flight_routes("
                        "Callsign, OperatorIcao, Route) VALUES(?, ?, ?)",
                        (
                            _callsign_info["callsign"],
                            _row["AirlineCode"],
                            _row["AirportCodes"],
                        ),
                    )
        _cursor.close()
        db_connection.commit()

    with sqlite3.connect(ROUTES_DB_FILE) as db_connection:
        db_connection.execute("VACUUM")


def get_flight_route(callsign: str) -> dict | None:
    with sqlite3.connect(ROUTES_DB_FILE) as connection:
        _cursor = connection.cursor()
        _cursor.execute(
            "SELECT Route from flight_routes WHERE Callsign=?", (callsign,)
        )
        result = _cursor.fetchone()
        _cursor.close()
    if result is not None:
        return result[0]


def get_airline_routes(operator_icao: str) -> list[str]:
    with sqlite3.connect(ROUTES_DB_FILE) as connection:
        _cursor = connection.cursor()
        _cursor.execute(
            "SELECT DISTINCT Route FROM flight_routes WHERE OperatorIcao=?",
            (operator_icao,),
        )
        result = _cursor.fetchall()
        _cursor.close()
    if result:
        return [row[0] for row in result]
    return []


if __name__ == "__main__":
    refresh_database()
