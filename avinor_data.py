#!/usr/bin/env python3
import logging
import arrow
import requests
import xmltodict
from airport_info import get_airport_icao
from airline_info import get_airline_icao, get_airline_iata
import airport_data

avinor_airports_iata = (
    "AES",
    "ANX",
    "ALF",
    "FDE",
    "BNN",
    "BOO",
    "BGO",
    "KRS",
    "EVE",
    "FRO",
    "OSL",
    "HAU",
    "KKN",
    "MOL",
    "MJF",
    "HOV",
    "MQN",
    "SDN",
    "SVJ",
    "SKN",
    "SSJ",
    "TOS",
    "TRF",
    "TRD",
    "VDS",
    "VRY",
    "SVG",
)

URL = "https://flydata.avinor.no/XmlFeed.asp"

logger = logging.getLogger(__name__)


def request_airport_data(airport_iata):
    airport_icao = get_airport_icao(airport_iata)
    params = {"airport": airport_iata, "TimeTo": 36, "TimeFrom": 36}
    response = requests.request("GET", URL, params=params, timeout=10)
    data = xmltodict.parse(response.text)
    flight_list = data["airport"]["flights"].get("flight")
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
        if None in (operator_icao, operator_iata):
            logger.warning(
                "operator information incomplete {}, {}".format(
                    operator_icao, operator_iata
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
        if direction == "A":
            route_items.append(other_airport_icao)
            route_items += stopovers_icao
            route_items.append(airport_icao)
            _route = "-".join(route_items)
            _timestamp = arrow.get(flight["schedule_time"])
            _date = _timestamp.format("YYYY-MM-DD")
            arrival = int(_timestamp.timestamp())
            _key = "{}_{}_{}_{}".format(
                operator_iata, flight_number, _date, _route
            )
            yield {
                "_id": _key,
                "airline_iata": operator_iata,
                "airline_icao": operator_icao,
                "flight_number": flight_number,
                "arrival": arrival,
                "route": _route,
            }
        elif direction == "D":
            route_items.append(airport_icao)
            route_items += stopovers_icao
            route_items.append(other_airport_icao)
            _route = "-".join(route_items)
            _timestamp = arrow.get(flight["schedule_time"])
            _date = _timestamp.format("YYYY-MM-DD")
            departure = int(_timestamp.timestamp())
            _key = "{}_{}_{}_{}".format(
                operator_iata, flight_number, _date, _route
            )
            yield {
                "_id": _key,
                "airline_iata": operator_iata,
                "airline_icao": operator_icao,
                "flight_number": flight_number,
                "departure": departure,
                "route": _route,
            }


class Airport(airport_data.Airport):
    def __init__(self):
        super().__init__("avinor")

    def update_data(self):
        for _airport_iata in avinor_airports_iata:
            for _flight in request_airport_data(_airport_iata):
                self.mycol.update_one(
                    {"_id": _flight["_id"]}, {"$set": _flight}, upsert=True
                )


if __name__ == "__main__":
    airport = Airport()
    airport.update_data()
