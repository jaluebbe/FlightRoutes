#!/usr/bin/env python3
import time
import logging
import arrow
import requests
from airport_info import get_airport_icao
from airline_info import get_airline_icao
import flight_data_source

from config import HAM_API_KEY

logger = logging.getLogger(__name__)


def request_ham_data():
    url = "https://rest.api.hamburg-airport.de/v2/flights/"
    headers = {
        "Cache-Control": "no-cache",
        "Ocp-Apim-Subscription-Key": HAM_API_KEY,
    }
    session = requests.Session()
    session.headers.update(headers)
    arrivals_response = session.get(url + "arrivals")
    if arrivals_response.status_code != 200:
        # retry once if request was not successful
        time.sleep(5)
        arrivals_response = session.get(url + "arrivals")
    arrivals_response.raise_for_status()
    arrivals = arrivals_response.json()
    departures_response = session.get(url + "departures")
    if departures_response.status_code != 200:
        # retry once if request was not successful
        time.sleep(5)
        departures_response = session.get(url + "departures")
    departures_response.raise_for_status()
    departures = departures_response.json()
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


def _get_date_and_time(flight):
    for _key in ["plannedDepartureTime", "plannedArrivalTime"]:
        if flight.get(_key) is not None:
            _timestamp = arrow.get(flight[_key].split("[")[0])
            _date = _timestamp.format("YYYY-MM-DD")
    for _key in ["expectedDepartureTime", "expectedArrivalTime"]:
        if flight.get(_key) is not None:
            _timestamp = arrow.get(flight[_key].split("[")[0])
    return _date, _timestamp.timestamp()


class Airport(flight_data_source.FlightDataSource):
    def __init__(self):
        super().__init__("HAM")

    def update_data(self):
        data = request_ham_data()
        for _flight in data["arrivals"]:
            _airline_iata = _flight["airline2LCode"]
            if _airline_iata is None:
                continue
            _flight_number = int(_flight["flightnumber"][3:])
            _airline_name = _flight["airlineName"]
            if _airline_name == "SmartLynx Airlines":
                _airline_iata = "6Y"
            _airline_icao = get_airline_icao(
                _airline_iata, _airline_name, _flight_number
            )
            if None in (_airline_icao, _airline_iata):
                logger.warning(
                    "operator information incomplete {}, {}".format(
                        _airline_icao, _airline_iata
                    )
                )
                continue
            _date, _timestamp = _get_date_and_time(_flight)
            _route_items = [get_airport_icao(_flight["originAirport3LCode"])]
            if _flight["viaAirport3LCode"] is not None:
                _route_items.append(
                    get_airport_icao(_flight["viaAirport3LCode"])
                )
            _route_items.append("EDDH")
            _route = "-".join(_route_items)
            _key = "{}_{}_{}_{}".format(
                _airline_iata, _flight_number, _date, _route
            )
            _ham_flight = {
                "_id": _key,
                "airline_iata": _airline_iata,
                "airline_icao": _airline_icao,
                "airline_name": _airline_name,
                "flight_number": _flight_number,
                "arrival": _timestamp,
                "status": _flight["flightStatusArrival"],
                "route": _route,
            }
            if _flight["cancelled"]:
                _ham_flight["cancelled"] = True
            if _flight["diverted"]:
                _ham_flight["diverted"] = True
            if _flight["flightnumber"] in data["overlapping_flight_numbers"]:
                _ham_flight["overlap"] = True
            self.update_flight(_ham_flight)
        for _flight in data["departures"]:
            _airline_iata = _flight["airline2LCode"]
            if _airline_iata is None:
                continue
            _flight_number = int(_flight["flightnumber"][3:])
            _airline_name = _flight["airlineName"]
            if _airline_name == "SmartLynx Airlines":
                _airline_iata = "6Y"
            _airline_icao = get_airline_icao(
                _airline_iata, _airline_name, _flight_number
            )
            if None in (_airline_icao, _airline_iata):
                logger.warning(
                    "operator information incomplete {}, {}".format(
                        _airline_icao, _airline_iata
                    )
                )
                continue
            _date, _timestamp = _get_date_and_time(_flight)
            _route_items = ["EDDH"]
            if _flight["viaAirport3LCode"] is not None:
                _route_items.append(
                    get_airport_icao(_flight["viaAirport3LCode"])
                )
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
                "airline_icao": _airline_icao,
                "airline_name": _airline_name,
                "flight_number": _flight_number,
                "departure": _timestamp,
                "status": _flight["flightStatusDeparture"],
                "route": _route,
            }
            if _flight["cancelled"]:
                _ham_flight["cancelled"] = True
            if _flight["diverted"]:
                _ham_flight["diverted"] = True
            if _flight["flightnumber"] in data["overlapping_flight_numbers"]:
                _ham_flight["overlap"] = True
            self.update_flight(_ham_flight)


if __name__ == "__main__":
    airport = Airport()
    airport.update_data()
