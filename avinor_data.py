#!/usr/bin/env python3
import logging
import pathlib
from itertools import permutations
import arrow
import requests
import xmltodict
from airport_info import get_airport_icao
from airline_info import get_airline_icao, get_airline_iata
import flight_data_source

avinor_airports_iata = (
    "AES",
    "ALF",
    "ANX",
    "BDU",
    "BGO",
    "BJF",
    "BNN",
    "BOO",
    "BVG",
    "EVE",
    "FDE",
    "FRO",
    "HAA",
    "HAU",
    "HFT",
    "HOV",
    "KKN",
    "KRS",
    "KSU",
    "LKL",
    "LKN",
    "LYR",
    "MEH",
    "MJF",
    "MOL",
    "MQN",
    "OSL",
    "OSY",
    "RET",
    "RRS",
    "RVK",
    "SDN",
    "SKN",
    "SOG",
    "SOJ",
    "SSJ",
    "SVG",
    "SVJ",
    "TOS",
    "TOS",
    "TRD",
    "TRF",
    "VAW",
    "VDS",
    "VRY",
)

URL = "https://asrv.avinor.no/XmlFeed/v1.0"
logger = logging.getLogger(pathlib.Path(__file__).name)


def _get_date_and_time(flight):
    _timestamp = arrow.get(flight["schedule_time"])
    _date = _timestamp.format("YYYY-MM-DD")
    if "status" in flight and "@time" in flight["status"]:
        _timestamp = arrow.get(flight["status"]["@time"])
    return _date, _timestamp.timestamp()


_status_codes = {
    "A": "arrived",
    "C": "cancelled",
    "D": "departed",
    "E": "new_time",
    "N": "new_info",
}


def request_airport_data(airport_iata):
    airport_icao = get_airport_icao(airport_iata)
    params = {"airport": airport_iata, "TimeTo": 36, "TimeFrom": 36}
    response = requests.request("GET", URL, params=params, timeout=10)
    data = xmltodict.parse(response.text)
    flight_list = data["airport"]["flights"].get("flight")
    logger.debug(f"{airport_iata}: {len(flight_list)} flights")
    if flight_list is None:
        return
    # flight_list represents the unmodified output of the API data which has
    # been converted from XML to dict.
    for flight in flight_list:
        if not isinstance(flight, dict):
            continue
        airline = flight["airline"]
        if flight["flight_id"][2:].isdigit():
            flight_number = int(flight["flight_id"][2:])
        elif flight["flight_id"][3:].isdigit():
            flight_number = int(flight["flight_id"][3:])
        else:
            continue
        if len(airline) == 3:
            operator_icao = airline
            operator_iata = get_airline_iata(operator_icao)
        elif len(airline) == 2:
            operator_iata = airline
            operator_icao = get_airline_icao(
                airline, flight_number=flight_number
            )
        else:
            continue
        if operator_iata == "QF":
            operator_icao = "QFA"
        elif operator_iata == "BA":
            operator_icao = "BAW"
        elif operator_iata == "NO":
            operator_icao = "NOS"
        elif operator_iata == "C3":
            operator_icao = "TDR"
        elif operator_icao == "WGH":
            continue
        if None in (operator_icao, operator_iata):
            logger.warning(
                "operator information incomplete {}, {}, {}".format(
                    operator_icao, operator_iata, flight["flight_id"]
                )
            )
            continue
        other_airport_iata = flight["airport"]
        other_airport_icao = get_airport_icao(other_airport_iata)
        if other_airport_icao is None:
            logger.warning(
                "missing airport icao for {}".format(other_airport_iata)
            )
        stopovers_iata = flight.get("via_airport")
        stopovers_icao = []
        if stopovers_iata is not None:
            for stopover_iata in stopovers_iata.split(","):
                stopover_icao = get_airport_icao(stopover_iata)
                stopovers_icao.append(stopover_icao)
                if stopover_icao is None:
                    logger.warning(
                        "missing airport icao for {}".format(stopover_iata)
                    )
        direction = flight["arr_dep"]
        route_items = []
        if any(
            _icao is None
            for _icao in stopovers_icao + [other_airport_icao, airport_icao]
        ):
            continue
        response = {
            "airline_iata": operator_iata,
            "airline_icao": operator_icao,
            "flight_number": flight_number,
        }
        if "status" in flight:
            response["status"] = _status_codes[flight["status"]["@code"]]
        if direction == "A":
            route_items = [other_airport_icao] + stopovers_icao + [airport_icao]
            _date, _arrival = _get_date_and_time(flight)
            response["arrival"] = _arrival
        elif direction == "D":
            route_items = [airport_icao] + stopovers_icao + [other_airport_icao]
            _date, _departure = _get_date_and_time(flight)
            response["departure"] = _departure
        _route = "-".join(route_items)
        response["route"] = _route
        response["_id"] = "{}_{}_{}_{}".format(
            operator_iata, flight_number, _date, _route
        )
        yield response


class Airport(flight_data_source.FlightDataSource):
    def __init__(self):
        super().__init__("Avinor")

    def update_data(self):
        all_flights = {}
        for _airport_iata in avinor_airports_iata:
            for _flight in request_airport_data(_airport_iata):
                _key = _flight["_id"].replace(_flight["route"], "")
                all_flights.setdefault(_key, {})
                all_flights[_key].setdefault(_flight["_id"], {})
                all_flights[_key][_flight["_id"]].update(_flight)
        for _flight_set in all_flights.values():
            for _flight1, _flight2 in permutations(_flight_set.values(), 2):
                if _flight2["route"] == _flight1["route"]:
                    pass
                elif _flight2["route"].startswith(_flight1["route"]):
                    _flight1["redundant"] = True
                elif _flight2["route"].endswith(_flight1["route"]):
                    _flight1["redundant"] = True
                elif _flight1["route"] in _flight2["route"]:
                    _flight1["redundant"] = True
            for _flight in _flight_set.values():
                self.update_flight(_flight)


if __name__ == "__main__":
    airport = Airport()
    airport.update_data()
