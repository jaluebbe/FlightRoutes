import pathlib
import math
import logging
import sqlite3

PWD = pathlib.Path(__file__).resolve().parent
URI = f"file:{PWD}/airports.sqb?mode=ro"


def get_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    if None in (lat1, lon1, lat2, lon2):
        return float("nan")
    degRad = 2 * math.pi / 360
    distance = 6.370e6 * math.acos(
        math.sin(lat1 * degRad) * math.sin(lat2 * degRad)
        + math.cos(lat1 * degRad)
        * math.cos(lat2 * degRad)
        * math.cos((lon2 - lon1) * degRad)
    )
    return distance


def get_closest_airports(
    latitude: float, longitude: float, iata_only: bool = False
) -> list[dict]:
    with sqlite3.connect(URI, uri=True) as connection:
        connection.create_function("DistanceBetween", 4, get_distance)
        connection.row_factory = sqlite3.Row
        cursor = connection.cursor()
        # 1deg latitude is approximately 111km.
        location_query = (
            "SELECT * FROM Airports WHERE Latitude > {0:f} - 0.5 AND "
            "Latitude < {0:f} + 0.5 "
            "ORDER BY DistanceBetween({0:f}, {1:f}, Latitude, Longitude) "
            "ASC LIMIT 10"
        )
        # limit results to airports with IATA designator
        location_query_iata_only = (
            "SELECT * FROM Airports WHERE Latitude > {0:f} - 0.5 AND "
            "Latitude < {0:f} + 0.5 AND LENGTH(IATA)=3 "
            "ORDER BY DistanceBetween({0:f}, {1:f}, Latitude, Longitude) "
            "ASC LIMIT 5"
        )
        if iata_only:
            cursor.execute(location_query_iata_only.format(latitude, longitude))
        else:
            cursor.execute(location_query.format(latitude, longitude))
        results = cursor.fetchall()
        cursor.close()
    return [dict(_row) for _row in results]


def get_closest_airport(
    latitude: float, longitude: float, iata_only: bool = False
) -> dict:
    return get_closest_airports(latitude, longitude, iata_only)[0]


def get_airport_info(icao: str) -> dict | None:
    assert icao is not None
    assert len(icao) == 4
    with sqlite3.connect(URI, uri=True) as connection:
        connection.row_factory = sqlite3.Row
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM Airports WHERE ICAO=?", (icao,))
        result = cursor.fetchone()
        cursor.close()
    if result is not None:
        airport = dict(result)
        airport["Latitude"] = float(airport["Latitude"])
        airport["Longitude"] = float(airport["Longitude"])
        return airport


def get_airport_label(icao: str) -> str:
    result = get_airport_info(icao)
    if result is None:
        return f"{icao} is unknown to database."
    return f"{result['Name']} ({result['ICAO']}), {result['Country']}"


def get_airport_iata(icao: str) -> str | None:
    result = get_airport_info(icao)
    if result is not None:
        return result["IATA"]


def get_airport_icao(iata: str) -> str | None:
    assert iata is not None
    assert len(iata) == 3
    with sqlite3.connect(URI, uri=True) as connection:
        connection.row_factory = sqlite3.Row
        cursor = connection.cursor()
        cursor.execute("SELECT ICAO from Airports WHERE IATA=?", (iata,))
        result = cursor.fetchone()
        cursor.close()
    if result is None:
        logging.warning(f"{iata} is unknown to database and may be a station.")
        return None
    return result["ICAO"]
