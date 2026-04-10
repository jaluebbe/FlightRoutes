#!venv/bin/python3
import pathlib
import glob
import csv
import sqlite3
import logging
import re
from opensky_utils import validated_callsign

PWD = pathlib.Path(__file__).resolve().parent
ROUTES_DB_FILE = PWD / "vrs_routes.sqb"

logger = logging.getLogger(__name__)

valid_route = re.compile(r"^([A-Z]{2}[A-Z0-9]{2}-){1,}[A-Z]{2}[A-Z0-9]{2}$")


def _ensure_schema(db_connection: sqlite3.Connection) -> None:
    _cursor = db_connection.cursor()
    _cursor.execute(
        "SELECT count(name) FROM sqlite_master "
        "WHERE type='table' AND name='flight_routes'"
    )
    if _cursor.fetchone()[0] == 0:
        with open(PWD / "vrs_routes.sql", encoding="utf-8") as f:
            db_connection.executescript(f.read())
    _cursor.close()


def refresh_database() -> None:
    csv_files = glob.glob(
        str(PWD / "../standing-data/routes/schema-01/*/*.csv")
    )
    if not csv_files:
        logger.warning(
            "No VRS route CSV files found — standing-data checkout missing "
            f"or not at expected path relative to {PWD}."
        )
        return

    logger.info(f"Processing {len(csv_files)} VRS route files.")
    _inserted = 0
    _skipped = 0

    with sqlite3.connect(ROUTES_DB_FILE) as db_connection:
        _ensure_schema(db_connection)
        _cursor = db_connection.cursor()

        for _file_name in csv_files:
            with open(_file_name, encoding="utf-8-sig") as _csv_file:
                _reader = csv.DictReader(_csv_file)
                for _row in _reader:
                    _callsign_info = validated_callsign(_row["Callsign"])
                    if _callsign_info is None:
                        _skipped += 1
                        continue
                    if not valid_route.match(_row["AirportCodes"]):
                        _skipped += 1
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
                    _inserted += 1

        _cursor.close()
        db_connection.commit()
        db_connection.execute("VACUUM")

    logger.info(f"Done: {_inserted} routes inserted, {_skipped} skipped.")


def get_flight_route(callsign: str) -> str | None:
    with sqlite3.connect(
        f"file:{ROUTES_DB_FILE}?mode=ro", uri=True
    ) as connection:
        _cursor = connection.cursor()
        _cursor.execute(
            "SELECT Route FROM flight_routes WHERE Callsign=?", (callsign,)
        )
        result = _cursor.fetchone()
        _cursor.close()
    if result is not None:
        return result[0]


def get_airline_routes(operator_icao: str) -> list[str]:
    with sqlite3.connect(
        f"file:{ROUTES_DB_FILE}?mode=ro", uri=True
    ) as connection:
        _cursor = connection.cursor()
        _cursor.execute(
            "SELECT DISTINCT Route FROM flight_routes WHERE OperatorIcao=?",
            (operator_icao,),
        )
        result = _cursor.fetchall()
        _cursor.close()
    if result:
        return [_row[0] for _row in result]
    return []


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    refresh_database()
