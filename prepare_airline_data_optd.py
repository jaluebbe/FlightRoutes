#!/usr/bin/env python3
import csv
import io
import sqlite3
import pathlib
import logging
from collections import Counter
import requests

URL = (
    "https://raw.githubusercontent.com/opentraveldata/opentraveldata/"
    "refs/heads/master/opentraveldata/optd_airlines.csv"
)
PWD = pathlib.Path(__file__).resolve().parent
AIRLINE_DB_FILE = PWD / "airlines.sqb"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _ensure_schema(db_connection: sqlite3.Connection) -> None:
    _cursor = db_connection.cursor()
    _cursor.execute(
        "SELECT count(name) FROM sqlite_master "
        "WHERE type='table' AND name='airlines'"
    )
    if _cursor.fetchone()[0] == 0:
        with open(PWD / "airlines.sql", encoding="utf-8") as f:
            db_connection.executescript(f.read())
    _cursor.close()


def _fetch_airlines() -> list[tuple[str, str, str]]:
    """Fetch the OPTD airlines CSV and return a list of (ICAO, IATA, Name)
    tuples for active airlines that have a valid 3-letter ICAO code.

    Active means: env_id is not '1' (not defunct/historical) and
    validity_to is empty (no end date recorded).
    """
    with requests.Session() as _session:
        _response = _session.get(URL)
        _response.raise_for_status()
        _response.encoding = "utf-8"

    _reader = csv.DictReader(io.StringIO(_response.text), delimiter="^")
    _airlines = []
    for _row in _reader:
        # Skip defunct or historical entries.
        if _row["env_id"] == "1":
            continue
        # Skip entries with a recorded end date.
        if _row["validity_to"]:
            continue
        _icao = _row["3char_code"].strip()
        if len(_icao) != 3:
            continue
        _iata = _row["2char_code"].strip()
        _name = _row["name"].strip()
        _airlines.append((_icao, _iata, _name))
    return _airlines


def main() -> None:
    _airlines = _fetch_airlines()
    logger.info(f"Fetched {len(_airlines)} active airlines from OPTD.")

    _iata_count = Counter(_iata for _, _iata, _ in _airlines if len(_iata) == 2)
    _duplicate_iatas = {
        _iata for _iata, _count in _iata_count.items() if _count > 1
    }
    if _duplicate_iatas:
        logger.info(
            f"Multiple occurrences of the following IATA codes: "
            f"{_duplicate_iatas}"
        )

    with sqlite3.connect(AIRLINE_DB_FILE) as db_connection:
        _ensure_schema(db_connection)
        _cursor = db_connection.cursor()
        _cursor.executemany(
            "REPLACE INTO airlines(ICAO, IATA, Name) VALUES(?, ?, ?)", _airlines
        )
        logger.info(f"Inserted/replaced {_cursor.rowcount} rows.")
        _cursor.close()
        db_connection.commit()
        db_connection.execute("VACUUM")


if __name__ == "__main__":
    main()
