import os
import sqlite3
from opensky_utils import validated_callsign

PWD = os.path.dirname(os.path.abspath(__file__))
ROUTES_DB_FILE = f"{PWD}/flightroute-icao.sqb"


def get_flight_route(callsign: str) -> dict:
    connection = sqlite3.connect(ROUTES_DB_FILE)
    _cursor = connection.cursor()
    _cursor.execute(
        "SELECT route from FlightRoute WHERE flight=?",
        (callsign,),
    )
    result = _cursor.fetchone()
    _cursor.close()
    connection.close()
    if result is None:
        return None
    return result[0]
