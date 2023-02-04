import os
import glob
import csv
import sqlite3

PWD = os.path.dirname(os.path.abspath(__file__))
ROUTES_DB_FILE = f"{PWD}/vrs_routes.sqb"

_sql_key_translation = {
    "Callsign": "callsign",
    "Route": "route",
    "OperatorIcao": "operator_icao",
}


def refresh_database():
    db_connection = sqlite3.connect(ROUTES_DB_FILE)
    _cursor = db_connection.cursor()
    _cursor.execute(
        "SELECT count(name) FROM sqlite_master "
        "WHERE type='table' AND name='flight_routes'"
    )

    if _cursor.fetchone()[0] == 0:
        with open(os.path.join(PWD, "vrs_routes.sql"), encoding="utf-8") as f:
            db_connection.executescript(f.read())
    for _file_name in glob.glob("../standing-data/routes/schema-01/*/*.csv"):
        with open(_file_name, encoding="utf-8-sig") as csv_file:
            reader = csv.DictReader(csv_file)
            for _row in reader:
                _cursor.execute(
                    "REPLACE INTO flight_routes(Callsign, OperatorIcao, Route)"
                    " VALUES(?, ?, ?)",
                    (
                        _row["Callsign"],
                        _row["AirlineCode"],
                        _row["AirportCodes"],
                    ),
                )
    _cursor.close()
    db_connection.commit()
    db_connection.execute("VACUUM")
    db_connection.close()


def get_flightroute(callsign: str) -> dict:
    connection = sqlite3.connect(ROUTES_DB_FILE)
    connection.row_factory = sqlite3.Row
    _cursor = connection.cursor()
    _cursor.execute(
        "SELECT * from flight_routes WHERE Callsign=?",
        (callsign,),
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


if __name__ == "__main__":
    refresh_database()
