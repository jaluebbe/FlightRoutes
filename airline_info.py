import os
import logging
import sqlite3
from difflib import SequenceMatcher

PWD = os.path.dirname(os.path.abspath(__file__))
URI = f"file:{PWD}/airlines.sqb?mode=ro"


def _similarity(a: str, b: str) -> float:
    s = SequenceMatcher(a=a.upper(), b=b.upper())
    return round(s.ratio(), 3)


def get_airline_info(icao: str):
    assert icao is not None
    assert len(icao) == 3
    assert icao.isalpha()
    connection = sqlite3.connect(URI, uri=True)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM Airlines WHERE ICAO=?", (icao,))
    result = dict(cursor.fetchone())
    cursor.close()
    connection.close()
    return result


def get_airlines_by_iata(iata: str):
    assert iata is not None
    assert len(iata) == 2
    connection = sqlite3.connect(URI, uri=True)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()
    cursor.execute("SELECT * from Airlines WHERE IATA=?", (iata,))
    results = cursor.fetchall()
    cursor.close()
    connection.close()
    if results is None:
        logging.warning(f"{iata} is unknown to database and may be a station.")
        return None
    else:
        return [dict(_row) for _row in results]


def get_airline_by_iata(iata: str, name: str = None, flight_number: int = None):
    if flight_number is not None:
        if iata == "LH":
            # https://de.wikipedia.org/wiki/Lufthansa#Flugnummernsystem
            if 8000 <= flight_number <= 8515:
                name = "Lufthansa Cargo"
            else:
                name = "Lufthansa"
        elif iata == "LX":
            name = "SWISS"
    results = get_airlines_by_iata(iata)
    if results is None:
        return None
    elif len(results) == 1:
        return results[0]
    elif name is None:
        return None
    ordered_results = sorted(
        [(_similarity(_row["Name"], name), _row) for _row in results]
    )
    logging.info(ordered_results)
    return ordered_results[-1][1]


def get_airline_iata(icao: str):
    result = get_airline_info(icao)
    if result is not None:
        return result["IATA"]


def get_airline_icao(iata: str, name: str = None, flight_number: int = None):
    result = get_airline_by_iata(iata, name, flight_number)
    if result is not None:
        return result["ICAO"]


def get_airline_icaos(iata: str):
    results = get_airlines_by_iata(iata)
    if results is not None:
        return [_row["ICAO"] for _row in results]
