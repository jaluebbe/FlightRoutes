import os
import time
import sqlite3
from config import OUTDATED

PWD = os.path.dirname(os.path.abspath(__file__))
ROUTES_DB_FILE = f"{PWD}/flight_routes.sqb"

db_connection = sqlite3.connect(ROUTES_DB_FILE)
_cursor = db_connection.cursor()
_cursor.execute(
    "SELECT count(name) FROM sqlite_master "
    "WHERE type='table' AND name='flight_routes'"
)
if _cursor.fetchone()[0] == 0:
    with open(os.path.join(PWD, "flight_routes.sql"), encoding="utf-8") as f:
        db_connection.executescript(f.read())
_cursor.close()
db_connection.commit()
db_connection.close()

_sql_key_translation = {
    "Callsign": "callsign",
    "Route": "route",
    "Source": "source",
    "UpdateTime": "update_time",
    "OperatorIcao": "operator_icao",
    "OperatorIata": "operator_iata",
    "FlightNumber": "flight_number",
    "Quality": "quality",
    "Errors": "errors",
    "ValidFrom": "valid_from",
}


def set_checked_flightroute(
    flight: dict, quality: int = 0, reset_errors: bool = False
) -> bool:
    utc = time.time()
    old_flight = get_checked_flightroute(flight["callsign"], flight["route"])
    valid_from = int(utc)
    if old_flight is not None:
        if old_flight["quality"] < quality:
            pass
        elif old_flight["update_time"] < OUTDATED:
            pass
        elif old_flight["errors"] > 10:
            # old record will be overwritten
            pass
        elif old_flight["quality"] > quality:
            return False
        if (
            old_flight["flight_number"] == flight["flight_number"]
            and old_flight["valid_from"] is not None
        ):
            valid_from = old_flight["valid_from"]
    if reset_errors:
        _reset_error_count(flight["callsign"], flight["route"])
    connection = sqlite3.connect(ROUTES_DB_FILE)
    connection.row_factory = sqlite3.Row
    _cursor = connection.cursor()
    _cursor.execute(
        "REPLACE INTO flight_routes (Callsign, Route, Source, OperatorIcao, "
        "OperatorIata, FlightNumber, Quality, UpdateTime, ValidFrom) VALUES "
        "(?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            flight["callsign"],
            flight["route"],
            flight["source"],
            flight["airline_icao"],
            flight["airline_iata"],
            flight["flight_number"],
            quality,
            int(utc),
            valid_from,
        ),
    )
    _cursor.close()
    connection.commit()
    connection.close()
    return True


def get_checked_flightroute(callsign: str, route: str) -> dict:
    connection = sqlite3.connect(ROUTES_DB_FILE)
    connection.row_factory = sqlite3.Row
    _cursor = connection.cursor()
    _cursor.execute(
        "SELECT * from flight_routes WHERE Callsign=? AND Route=?",
        (callsign, route),
    )
    result = _cursor.fetchone()
    _cursor.close()
    connection.close()
    if result is None:
        return None
    result = dict(result)
    for old_key, new_key in _sql_key_translation.items():
        result[new_key] = result.pop(old_key)
    return result


def _reset_error_count(callsign: str, route: str) -> None:
    connection = sqlite3.connect(ROUTES_DB_FILE)
    connection.row_factory = sqlite3.Row
    _cursor = connection.cursor()
    _cursor.execute(
        "UPDATE flight_routes SET Errors = 0 WHERE Callsign=? AND Route=?",
        (callsign, route),
    )
    result = _cursor.fetchone()
    _cursor.close()
    connection.close()


def get_recent_callsigns(min_quality: int = 1, hours: float = 48) -> list:
    connection = sqlite3.connect(ROUTES_DB_FILE)
    _cursor = connection.cursor()
    _cursor.execute(
        "SELECT DISTINCT Callsign FROM flight_routes "
        "WHERE Quality >= ? AND UpdateTime > STRFTIME('%s') - ?*3600",
        (min_quality, hours),
    )
    results = _cursor.fetchall()
    _cursor.close()
    connection.close()
    return [_row[0] for _row in results]
