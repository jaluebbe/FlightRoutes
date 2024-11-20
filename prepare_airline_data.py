import csv
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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

iata_count = Counter()
with requests.Session() as s:
    _response = s.get(URL)
    _response.encoding = "utf-8-sig"
_reader = csv.DictReader(_response.text.splitlines(), delimiter=",")
_airlines = [_row for _row in _reader if len(_row["ICAO"]) == 3]
iata_count.update(
    [_row["IATA"] for _row in _airlines if len(_row["IATA"]) == 2]
)
iata_count.subtract(list(iata_count))
duplicate_iatas = set(+iata_count)
logger.info(
    f"Multiple occurence of the following IATA codes:\n{duplicate_iatas}"
)

with sqlite3.connect(AIRLINE_DB_FILE) as db_connection:
    _cursor = db_connection.cursor()
    _cursor.execute(
        "SELECT count(name) FROM sqlite_master "
        "WHERE type='table' AND name='airlines'"
    )
    if _cursor.fetchone()[0] == 0:
        with open(PWD / "airlines.sql", encoding="utf-8") as f:
            db_connection.executescript(f.read())

    for _row in _airlines:
        _cursor.execute(
            "REPLACE INTO airlines(ICAO, IATA, Name) VALUES(?, ?, ?)",
            (_row["ICAO"], _row["IATA"], _row["Name"]),
        )
    for _airline in []:
        _cursor.execute(
            "REPLACE INTO airlines(ICAO, IATA, Name) VALUES(?, ?, ?)",
            _airline,
        )
    for _icao_iata in [
        ("IBZ", "6I"),
    ]:
        _cursor.execute(
            "DELETE FROM airlines WHERE ICAO=? AND IATA=?", _icao_iata
        )
    for _icao_iata in []:
        _cursor.execute(
            "UPDATE airlines SET IATA='' WHERE ICAO=? AND IATA=?", _icao_iata
        )
    _cursor.execute(
        "UPDATE airlines SET Name='Azul Conecta', IATA='2F' WHERE ICAO='ACN'"
    )
    _cursor.close()
    db_connection.commit()

with sqlite3.connect(AIRLINE_DB_FILE) as db_connection:
    db_connection.execute("VACUUM")
