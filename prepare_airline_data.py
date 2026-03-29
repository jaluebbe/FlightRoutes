#!/usr/bin/env python3
import csv
import json
import sqlite3
import pathlib
import logging
from collections import Counter
import requests

URL = (
    "https://raw.githubusercontent.com/vradarserver/standing-data/main/"
    "airlines/schema-01/airlines.csv"
)
PWD = pathlib.Path(__file__).resolve().parent
AIRLINE_DB_FILE = PWD / "airlines.sqb"
PATCH_FILE = PWD / "airline_patches.json"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _load_patches() -> dict:
    with open(PATCH_FILE, encoding="utf-8") as f:
        return json.load(f)


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


def _apply_patches(cursor: sqlite3.Cursor, patches: dict) -> None:
    for _patch in patches.get("insert", []):
        cursor.execute(
            "REPLACE INTO airlines(ICAO, IATA, Name) VALUES(?, ?, ?)",
            (_patch["icao"], _patch["iata"], _patch["name"]),
        )
        logger.info(
            f"patch insert: {_patch['icao']} / {_patch['iata']} "
            f"({_patch['name']})"
        )

    for _patch in patches.get("delete", []):
        cursor.execute(
            "DELETE FROM airlines WHERE ICAO=? AND IATA=?",
            (_patch["icao"], _patch["iata"]),
        )
        if cursor.rowcount > 0:
            logger.info(
                f"patch delete: removed {_patch['icao']} / {_patch['iata']}"
            )
        else:
            logger.warning(
                f"patch delete: no row found for "
                f"{_patch['icao']} / {_patch['iata']} — patch may be stale"
            )

    for _patch in patches.get("clear_iata", []):
        cursor.execute(
            "UPDATE airlines SET IATA='' WHERE ICAO=? AND IATA=?",
            (_patch["icao"], _patch["iata"]),
        )
        if cursor.rowcount > 0:
            logger.info(
                f"patch clear_iata: cleared IATA {_patch['iata']} "
                f"from {_patch['icao']}"
            )
        else:
            logger.warning(
                f"patch clear_iata: no row found for "
                f"{_patch['icao']} / {_patch['iata']} — patch may be stale"
            )

    for _patch in patches.get("set_iata", []):
        cursor.execute(
            "UPDATE airlines SET IATA=? WHERE ICAO=?",
            (_patch["iata"], _patch["icao"]),
        )
        if cursor.rowcount > 0:
            logger.info(
                f"patch set_iata: set IATA {_patch['iata']} "
                f"on {_patch['icao']}"
            )
        else:
            logger.warning(
                f"patch set_iata: no row found for ICAO "
                f"{_patch['icao']} — patch may be stale"
            )


def main() -> None:
    patches = _load_patches()

    with requests.Session() as _session:
        _response = _session.get(URL)
        _response.encoding = "utf-8-sig"

    _reader = csv.DictReader(_response.text.splitlines(), delimiter=",")
    _airlines = [_row for _row in _reader if len(_row["ICAO"]) == 3]

    _iata_count = Counter(
        _row["IATA"] for _row in _airlines if len(_row["IATA"]) == 2
    )
    _duplicate_iatas = {
        iata for iata, count in _iata_count.items() if count > 1
    }
    logger.info(
        f"Multiple occurrence of the following IATA codes:\n{_duplicate_iatas}"
    )

    with sqlite3.connect(AIRLINE_DB_FILE) as db_connection:
        _ensure_schema(db_connection)
        _cursor = db_connection.cursor()

        for _row in _airlines:
            _cursor.execute(
                "REPLACE INTO airlines(ICAO, IATA, Name) VALUES(?, ?, ?)",
                (_row["ICAO"], _row["IATA"], _row["Name"]),
            )

        _apply_patches(_cursor, patches)
        _cursor.close()
        db_connection.commit()
        db_connection.execute("VACUUM")


if __name__ == "__main__":
    main()
