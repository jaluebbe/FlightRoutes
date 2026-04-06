#!/usr/bin/env python3
import time
import logging
import pathlib
import arrow
import requests
from airport_info import get_airport_icao
from airline_info import get_airline_icao
import flight_data_source

from config import HAM_API_KEY

logger = logging.getLogger(pathlib.Path(__file__).name)

_HAM_ICAO = "EDDH"
_HAM_URL = "https://rest.api.hamburg-airport.de/v2/flights/"


def request_ham_data() -> dict:
    headers = {
        "Cache-Control": "no-cache",
        "Ocp-Apim-Subscription-Key": HAM_API_KEY,
    }
    session = requests.Session()
    session.headers.update(headers)

    def _fetch(path: str) -> list:
        response = session.get(_HAM_URL + path)
        if response.status_code != 200:
            time.sleep(5)
            response = session.get(_HAM_URL + path)
        response.raise_for_status()
        return response.json()

    arrivals = _fetch("arrivals")
    departures = _fetch("departures")
    _arriving = {_row["flightnumber"] for _row in arrivals}
    _departing = {_row["flightnumber"] for _row in departures}
    return {
        "arrivals": arrivals,
        "departures": departures,
        "overlapping_flight_numbers": _arriving & _departing,
    }


def _get_date_and_time(
    flight: dict, planned_key: str, expected_key: str
) -> tuple:
    """Return (date_str, unix_timestamp) for a flight.

    Uses the planned time as the base date, then overrides with the expected
    time if available. Returns (None, None) if no planned time is present.
    """
    _planned = flight.get(planned_key)
    if _planned is None:
        return None, None
    _timestamp = arrow.get(_planned.split("[")[0])
    _date = _timestamp.format("YYYY-MM-DD")
    _expected = flight.get(expected_key)
    if _expected is not None:
        _timestamp = arrow.get(_expected.split("[")[0])
    return _date, _timestamp.timestamp()


def _resolve_airline(flight: dict) -> tuple[str | None, str | None]:
    """Return (airline_iata, airline_icao) for a flight, or (None, None)
    if the airline cannot be resolved."""
    _airline_iata = flight["airline2LCode"]
    if _airline_iata is None:
        return None, None
    _flight_number_str = flight["flightnumber"][3:]
    if not _flight_number_str.isdigit():
        return None, None
    _flight_number = int(_flight_number_str)
    _airline_name = flight["airlineName"]
    # SmartLynx Airlines uses a non-standard code in the HAM feed.
    if _airline_name == "SmartLynx Airlines":
        _airline_iata = "6Y"
    _airline_icao = get_airline_icao(
        _airline_iata, _airline_name, _flight_number
    )
    return _airline_iata, _airline_icao


def _process_flight(
    flight: dict,
    direction: str,
    overlapping: set[str],
    unknown_airports: set[str],
) -> dict | None:
    """Process a single HAM API flight entry into a flight dict.

    direction must be 'A' (arrival) or 'D' (departure).
    """
    _airline_iata, _airline_icao = _resolve_airline(flight)
    if _airline_iata is None:
        return None
    if None in (_airline_icao, _airline_iata):
        if _airline_iata != "ZZ":
            logger.warning(
                "operator information incomplete {}, {}, {}".format(
                    _airline_icao, _airline_iata, flight["airlineName"]
                )
            )
        return None

    if direction == "A":
        _date, _timestamp = _get_date_and_time(
            flight, "plannedArrivalTime", "expectedArrivalTime"
        )
        _other_iata = flight["originAirport3LCode"]
        _via_iata = flight.get("viaAirport3LCode")
    else:
        _date, _timestamp = _get_date_and_time(
            flight, "plannedDepartureTime", "expectedDepartureTime"
        )
        _other_iata = flight["destinationAirport3LCode"]
        _via_iata = flight.get("viaAirport3LCode")

    if _date is None:
        return None

    _other_icao = get_airport_icao(_other_iata)
    if _other_icao is None:
        unknown_airports.add(_other_iata)
        return None

    _route_items = []
    if direction == "A":
        _route_items.append(_other_icao)
        if _via_iata is not None:
            _via_icao = get_airport_icao(_via_iata)
            if _via_icao is None:
                unknown_airports.add(_via_iata)
                return None
            _route_items.append(_via_icao)
        _route_items.append(_HAM_ICAO)
    else:
        _route_items.append(_HAM_ICAO)
        if _via_iata is not None:
            _via_icao = get_airport_icao(_via_iata)
            if _via_icao is None:
                unknown_airports.add(_via_iata)
                return None
            _route_items.append(_via_icao)
        _route_items.append(_other_icao)

    _flight_number = int(flight["flightnumber"][3:])
    _route = "-".join(_route_items)
    _result = {
        "_id": f"{_airline_iata}_{_flight_number}_{_date}_{_route}",
        "airline_iata": _airline_iata,
        "airline_icao": _airline_icao,
        "airline_name": flight["airlineName"],
        "flight_number": _flight_number,
        "route": _route,
    }
    if direction == "A":
        _result["arrival"] = _timestamp
        _result["status"] = flight["flightStatusArrival"]
    else:
        _result["departure"] = _timestamp
        _result["status"] = flight["flightStatusDeparture"]

    if flight["cancelled"]:
        _result["cancelled"] = True
    if flight["diverted"]:
        _result["diverted"] = True
    if flight["flightnumber"] in overlapping:
        _result["overlap"] = True

    return _result


class Airport(flight_data_source.FlightDataSource):
    def __init__(self):
        super().__init__("HAM")

    def update_data(self) -> None:
        data = request_ham_data()
        _overlapping = data["overlapping_flight_numbers"]
        _unknown_airports: set[str] = set()
        _stored = 0

        for _flight in data["arrivals"]:
            _result = _process_flight(
                _flight, "A", _overlapping, _unknown_airports
            )
            if _result is None:
                continue
            self.update_flight(_result)
            _stored += 1

        for _flight in data["departures"]:
            _result = _process_flight(
                _flight, "D", _overlapping, _unknown_airports
            )
            if _result is None:
                continue
            self.update_flight(_result)
            _stored += 1

        for _iata in sorted(_unknown_airports):
            logger.warning(f"Unknown airport IATA: {_iata}")

        logger.info(f"HAM: stored {_stored} flights.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    airport = Airport()
    airport.update_data()
