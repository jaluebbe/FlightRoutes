import pathlib
import math
import logging
import sqlite3

PWD = pathlib.Path(__file__).resolve().parent
URI = f"file:{PWD}/airports.sqb?mode=ro"

logger = logging.getLogger(__name__)


def get_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    if None in (lat1, lon1, lat2, lon2):
        return float("nan")
    deg_rad = 2 * math.pi / 360
    dot_product = math.sin(lat1 * deg_rad) * math.sin(
        lat2 * deg_rad
    ) + math.cos(lat1 * deg_rad) * math.cos(lat2 * deg_rad) * math.cos(
        (lon2 - lon1) * deg_rad
    )
    return 6.370e6 * math.acos(max(-1.0, min(1.0, dot_product)))


# 1 degree of latitude is approximately 111 km. The latitude window
# is a fast pre-filter to avoid calling get_distance on every row.
_LOCATION_QUERY = (
    "SELECT *, CAST(DistanceBetween(?, ?, Latitude, Longitude) AS INTEGER)"
    " AS Distance "
    "FROM Airports "
    "WHERE Latitude > ? - 1 AND Latitude < ? + 1{iata_filter} "
    "ORDER BY Distance ASC LIMIT {limit}"
)


def get_closest_airports(
    latitude: float, longitude: float, iata_only: bool = False
) -> list[dict]:
    iata_filter = " AND LENGTH(IATA)=3" if iata_only else ""
    limit = 5 if iata_only else 15
    query = _LOCATION_QUERY.format(iata_filter=iata_filter, limit=limit)
    with sqlite3.connect(URI, uri=True) as connection:
        connection.create_function("DistanceBetween", 4, get_distance)
        connection.row_factory = sqlite3.Row
        cursor = connection.cursor()
        cursor.execute(query, (latitude, longitude, latitude, latitude))
        results = cursor.fetchall()
        cursor.close()
    return [dict(_row) for _row in results]


def get_closest_airport(
    latitude: float, longitude: float, iata_only: bool = False
) -> dict | None:
    results = get_closest_airports(latitude, longitude, iata_only)
    return results[0] if results else None


def get_airport_info(icao: str) -> dict | None:
    if icao is None or len(icao) != 4:
        raise ValueError(
            f"ICAO code must be a 4-character string, got: {icao!r}"
        )
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
    if iata is None or len(iata) != 3:
        raise ValueError(
            f"IATA code must be a 3-character string, got: {iata!r}"
        )
    with sqlite3.connect(URI, uri=True) as connection:
        connection.row_factory = sqlite3.Row
        cursor = connection.cursor()
        cursor.execute("SELECT ICAO FROM Airports WHERE IATA=?", (iata,))
        result = cursor.fetchone()
        cursor.close()
    if result is None:
        logger.warning(f"{iata} is unknown to database and may be a station.")
        return None
    return result["ICAO"]
