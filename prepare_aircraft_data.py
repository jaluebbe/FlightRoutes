import json
import requests
import csv
from contextlib import closing

url = "https://opensky-network.org/datasets/metadata/aircraftDatabase.csv"
icao24_to_registration = {}

with closing(requests.get(url, stream=True)) as r:
    reader = csv.DictReader(r.iter_lines(decode_unicode='utf-8'), delimiter=',',
        quotechar='"')
    icao24_to_registration = {
        _row["icao24"]: "".join(_row["registration"].split("-"))
        for _row in reader
        if _row["operatoricao"] != "" and _row["registration"] != ""}
json.dump(icao24_to_registration, open("icao24_to_registration_new.json", "w"))
