#!/usr/bin/env python3
import arrow
import pymongo
import requests
from airport_info import get_airport_icao
from route_utils import estimate_max_flight_duration, get_route_length

from config import HAM_API_KEY


def request_ham_data():
    url = "https://rest.api.hamburg-airport.de/v2/flights/"
    headers = {
        "Cache-Control": "no-cache",
        "Ocp-Apim-Subscription-Key": HAM_API_KEY,
    }
    session = requests.Session()
    session.headers.update(headers)
    arrivals = session.get(url + "arrivals").json()
    departures = session.get(url + "departures").json()
    arriving_flight_numbers = [_row["flightnumber"] for _row in arrivals]
    departing_flight_numbers = [_row["flightnumber"] for _row in departures]
    overlapping_flight_numbers = set(arriving_flight_numbers).intersection(
        departing_flight_numbers
    )
    return {
        "arrivals": arrivals,
        "departures": departures,
        "overlapping_flight_numbers": overlapping_flight_numbers,
    }


myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["airports"]
mycol = mydb["ham"]

# "2023-01-11T14:40:00.000+01:00[EUROPE/BERLIN]"[:29]


def _get_date_and_time(flight):
    for _key in ["plannedDepartureTime", "plannedArrivalTime"]:
        if flight.get(_key) is not None:
            _timestamp = arrow.get(flight[_key][:29])
            _date = _timestamp.format("YYYY-MM-DD")
    for _key in ["expectedDepartureTime", "expectedArrivalTime"]:
        if flight.get(_key) is not None:
            _timestamp = arrow.get(flight[_key][:29])
    return _date, _timestamp.timestamp()


def update_ham_data():
    data = request_ham_data()
    for _flight in data["arrivals"]:
        _airline_iata = _flight["airline2LCode"]
        if _airline_iata is None:
            continue
        _flight_number = int(_flight["flightnumber"][3:])
        _date, _timestamp = _get_date_and_time(_flight)
        _route_items = [get_airport_icao(_flight["originAirport3LCode"])]
        if _flight["viaAirport3LCode"] is not None:
            _route_items.append(get_airport_icao(_flight["viaAirport3LCode"]))
        _route_items.append("EDDH")
        _route = "-".join(_route_items)
        _key = "{}_{}_{}_{}".format(
            _airline_iata, _flight_number, _date, _route
        )
        _ham_flight = {
            "_id": _key,
            "airline_iata": _airline_iata,
            "flight_number": _flight_number,
            "arrival": _timestamp,
            "status": _flight["flightStatusArrival"],
        }
        if _flight["flightnumber"] in data["overlapping_flight_numbers"]:
            _ham_flight["overlap"] = True
        if _flight["cancelled"]:
            _ham_flight["cancelled"] = True
        if _flight["diverted"]:
            _ham_flight["diverted"] = True
        try:
            mycol.insert_one(_ham_flight)
        except pymongo.errors.DuplicateKeyError:
            mycol.update_one({"_id": _ham_flight["_id"]}, {"$set": _ham_flight})
    for _flight in data["departures"]:
        _airline_iata = _flight["airline2LCode"]
        if _airline_iata is None:
            continue
        _flight_number = int(_flight["flightnumber"][3:])
        _date, _timestamp = _get_date_and_time(_flight)
        _route_items = [
            "EDDH",
        ]
        if _flight["viaAirport3LCode"] is not None:
            _route_items.append(get_airport_icao(_flight["viaAirport3LCode"]))
        _route_items.append(
            get_airport_icao(_flight["destinationAirport3LCode"])
        )
        _route = "-".join(_route_items)
        _key = "{}_{}_{}_{}".format(
            _airline_iata, _flight_number, _date, _route
        )
        _ham_flight = {
            "_id": _key,
            "airline_iata": _airline_iata,
            "flight_number": _flight_number,
            "departure": _timestamp,
            "status": _flight["flightStatusDeparture"],
        }
        if _flight["flightnumber"] in data["overlapping_flight_numbers"]:
            _ham_flight["overlap"] = True
        if _flight["cancelled"]:
            _ham_flight["cancelled"] = True
        if _flight["diverted"]:
            _ham_flight["diverted"] = True
        try:
            mycol.insert_one(_ham_flight)
        except pymongo.errors.DuplicateKeyError:
            mycol.update_one({"_id": _ham_flight["_id"]}, {"$set": _ham_flight})


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
    update_ham_data()
