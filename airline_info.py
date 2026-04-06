import json
import pathlib
import logging
import sqlite3
from functools import lru_cache
from difflib import SequenceMatcher

PWD = pathlib.Path(__file__).resolve().parent
URI = f"file:{PWD}/airlines.sqb?mode=ro"

logger = logging.getLogger(__name__)

_LOW_SIMILARITY_THRESHOLD = 0.3


def _similarity(a: str, b: str) -> float:
    return round(SequenceMatcher(None, a.upper(), b.upper()).ratio(), 3)


@lru_cache(maxsize=4096)
def _query_airline_by_icao(icao: str) -> tuple | None:
    with sqlite3.connect(URI, uri=True) as connection:
        cursor = connection.cursor()
        cursor.execute(
            "SELECT ICAO, IATA, Name FROM Airlines WHERE ICAO=?", (icao,)
        )
        result = cursor.fetchone()
        cursor.close()
    return result


@lru_cache(maxsize=4096)
def _query_airlines_by_iata(iata: str) -> tuple[tuple, ...]:
    with sqlite3.connect(URI, uri=True) as connection:
        cursor = connection.cursor()
        cursor.execute(
            "SELECT ICAO, IATA, Name FROM Airlines WHERE IATA=?", (iata,)
        )
        results = cursor.fetchall()
        cursor.close()
    return tuple(results)


def get_airline_info(icao: str) -> dict | None:
    if icao is None or len(icao) != 3 or not icao.isalpha():
        raise ValueError(
            f"ICAO airline code must be a 3-letter string, got: {icao!r}"
        )
    result = _query_airline_by_icao(icao)
    if result is not None:
        return {"ICAO": result[0], "IATA": result[1], "Name": result[2]}


def get_airlines_by_iata(iata: str) -> list[dict]:
    if iata is None or len(iata) != 2:
        raise ValueError(
            f"IATA airline code must be a 2-character string, got: {iata!r}"
        )
    results = _query_airlines_by_iata(iata)
    if not results:
        logger.debug(f"{iata} is unknown to database and may be a station.")
    return [{"ICAO": _r[0], "IATA": _r[1], "Name": _r[2]} for _r in results]


def get_airline_by_iata(
    iata: str, name: str = None, flight_number: int = None
) -> dict | None:
    if flight_number is not None:
        name = _resolve_name_by_flight_number(iata, flight_number, name)
    results = get_airlines_by_iata(iata)
    if not results:
        return None
    if len(results) == 1:
        return results[0]
    if name is None:
        return None
    ordered_results = sorted(
        [(_similarity(_row["Name"], name), _row) for _row in results],
        key=lambda x: x[0],
    )
    best_score, best_match = ordered_results[-1]
    logger.debug(
        f"best name match for {iata!r} with hint {name!r}: "
        f"{best_match['Name']!r} (score {best_score})"
    )
    if best_score < _LOW_SIMILARITY_THRESHOLD:
        logger.warning(
            f"low similarity ({best_score}) for {iata!r} / {name!r} — "
            f"best match was {best_match['Name']!r}"
        )
    return best_match


def _load_flight_number_ranges() -> dict:
    _ranges_file = PWD / "airline_flight_number_ranges.json"
    if not _ranges_file.exists():
        return {}
    with open(_ranges_file, encoding="utf-8") as _f:
        return json.load(_f)


_flight_number_ranges = _load_flight_number_ranges()


def _resolve_name_by_flight_number(
    iata: str, flight_number: int, name: str | None
) -> str | None:
    """Return a disambiguating name for airlines whose sub-brands operate
    under different flight number ranges. Falls back to the existing name
    if no range matches."""
    for _entry in _flight_number_ranges.get(iata, []):
        if _entry["min"] <= flight_number <= _entry["max"]:
            return _entry["name"]
    return name


def get_airline_iata(icao: str) -> str | None:
    result = get_airline_info(icao)
    if result is not None:
        return result["IATA"] or None


def get_airline_icao(
    iata: str, name: str = None, flight_number: int = None
) -> str | None:
    result = get_airline_by_iata(iata, name, flight_number)
    if result is not None:
        return result["ICAO"]


def get_airline_icaos(iata: str) -> list[str]:
    return [_row["ICAO"] for _row in get_airlines_by_iata(iata)]
