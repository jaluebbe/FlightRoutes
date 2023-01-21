#!/usr/bin/env python3
import requests
import arrow
import xmltodict
import pymongo
from airport_info import get_airport_icao
from airline_info import get_airline_icao
from route_utils import estimate_max_flight_duration, get_route_length


def _recent_timestamp(flight):
    for _key in ["ATD", "ETD", "STD"]:
        if flight[f"{_key}_DATE"] is None:
            continue
        _date = flight[f"{_key}_DATE"]
        _time = flight[f"{_key}_TIME"]
        return int(
            arrow.get(
                f"{_date} {_time}", "DD.MM.YYYY HH:mm", tzinfo="Europe/Berlin"
            ).timestamp()
        )


myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["airports"]
mycol = mydb["fmo"]

_status_codes = {
    "GTO": "gate_open",
    "BOR": "boarding",
    "GCL": "gate_closed",
    "TXI": "taxiing",
    "DEP": "departed",
    "ARR": "arrived",
    "DLY": "delayed",
}


def update_fmo_data():
    url = "https://service.fmo.de/arrdep1.xml"
    response = requests.get(url)
    response.raise_for_status()
    flights = xmltodict.parse(response.content)["Flights"]["Flight"]
    for _flight in flights:
        _fmo_flight = {}
        if _flight["FTYP"] == "D":
            _origin_icao = "EDDG"
            _destination_icao = get_airport_icao(_flight["CITY3"])
            _fmo_flight["departure"] = _recent_timestamp(_flight)
        else:
            _origin_icao = get_airport_icao(_flight["CITY3"])
            _destination_icao = "EDDG"
            _fmo_flight["arrival"] = _recent_timestamp(_flight)
        _fmo_flight["_id"] = _flight["ID"]
        _airline_iata, _flight_number = _flight["FNR"].split(" ")
        _flight_number = int(_flight_number)
        _airline_icao = get_airline_icao(
            iata=_airline_iata, flight_number=_flight_number
        )
        _fmo_flight["airline_iata"] = _airline_iata
        _fmo_flight["airline_icao"] = _airline_icao
        _fmo_flight["flight_number"] = _flight_number
        _route_items = [_origin_icao]
        if _flight["VIA3"] is not None:
            _route_items.append(get_airport_icao(_flight["VIA3"]))
        _route_items.append(_destination_icao)
        _fmo_flight["route"] = "-".join(_route_items)
        _status_code = _flight["REM_CODE"]
        if _status_code in _status_codes:
            _fmo_flight["status"] = _status_codes[_status_code]
        elif _status_code is not None:
            _fmo_flight["status"] = _status_code
        try:
            mycol.insert_one(_fmo_flight)
        except pymongo.errors.DuplicateKeyError:
            mycol.update_one({"_id": _fmo_flight["_id"]}, {"$set": _fmo_flight})


def _in_bounds(flight, utc):
    _departure = flight.get("departure")
    _arrival = flight.get("arrival")
    _length = get_route_length(flight["route"])
    if _departure is not None:
        return _departure + estimate_max_flight_duration(_length) > utc
    elif _arrival is not None:
        return _arrival - estimate_max_flight_duration(_length) < utc


def get_active_flights(utc=None):
    if utc is None:
        utc = int(arrow.utcnow().timestamp())
    flights = [
        _flight
        for _flight in mycol.find(
            {
                "$or": [
                    {"departure": {"$gt": utc - 24 * 3600, "$lt": utc + 300}},
                    {"arrival": {"$gt": utc - 300, "$lt": utc + 24 * 3600}},
                ]
            }
        )
        if _in_bounds(_flight, utc)
    ]
    return flights


if __name__ == "__main__":
    update_fmo_data()
