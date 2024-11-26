import os
import time
import sqlite3
import logging
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
    errors = 0
    if old_flight is not None:
        if old_flight["quality"] < quality:
            # allow sources of better quality to overwrite
            pass
        elif old_flight["update_time"] < OUTDATED:
            pass
        elif old_flight["errors"] > 10:
            # old record will be overwritten
            pass
        elif old_flight["quality"] > quality:
            # Deny sources of lower quality to overwrite except for
            # many errors or outdated data.
            return False
        if (
            old_flight["flight_number"] == flight["flight_number"]
            and old_flight["operator_iata"] == flight["airline_iata"]
            and old_flight["valid_from"] is not None
        ):
            valid_from = old_flight["valid_from"]
            if not reset_errors:
                errors = old_flight["errors"]
        logging.debug(
            "updating flight in database: {} {}".format(
                flight["callsign"], flight["route"]
            )
        )
    else:
        logging.info(
            "added flight to database: {} {}".format(
                flight["callsign"], flight["route"]
            )
        )
    connection = sqlite3.connect(ROUTES_DB_FILE)
    connection.row_factory = sqlite3.Row
    _cursor = connection.cursor()
    _cursor.execute(
        "REPLACE INTO flight_routes (Callsign, Route, Source, OperatorIcao, "
        "OperatorIata, FlightNumber, Quality, Errors, UpdateTime, ValidFrom) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            flight["callsign"],
            flight["route"],
            flight["source"],
            flight["airline_icao"],
            flight["airline_iata"],
            flight["flight_number"],
            quality,
            errors,
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


def get_flights_by_number(operator_iata: str, flight_number: int) -> list[dict]:
    connection = sqlite3.connect(ROUTES_DB_FILE)
    connection.row_factory = sqlite3.Row
    _cursor = connection.cursor()
    _cursor.execute(
        "SELECT * from flight_routes WHERE OperatorIata=? AND FlightNumber=?",
        (operator_iata, flight_number),
    )
    results = _cursor.fetchall()
    _cursor.close()
    connection.close()
    if results is None:
        return []
    results = [dict(_row) for _row in results]
    for _result in results:
        for old_key, new_key in _sql_key_translation.items():
            _result[new_key] = _result.pop(old_key)
    return results


def reset_error_count(callsign: str, route: str) -> None:
    connection = sqlite3.connect(ROUTES_DB_FILE)
    connection.row_factory = sqlite3.Row
    _cursor = connection.cursor()
    _cursor.execute(
        "UPDATE flight_routes SET Errors = 0 WHERE Callsign=? AND Route=?",
        (callsign, route),
    )
    _cursor.close()
    connection.commit()
    connection.close()


def increase_error_count(callsign: str, route: str) -> None:
    connection = sqlite3.connect(ROUTES_DB_FILE)
    connection.row_factory = sqlite3.Row
    _cursor = connection.cursor()
    _cursor.execute(
        "UPDATE flight_routes SET Errors = Errors + 1 "
        "WHERE Callsign=? AND Route=?",
        (callsign, route),
    )
    _cursor.close()
    connection.commit()
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


if __name__ == "__main__":
    with sqlite3.connect(ROUTES_DB_FILE) as db_connection:
        db_connection.execute("VACUUM")
