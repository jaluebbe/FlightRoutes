#!/usr/bin/env python3
import logging
import requests
import arrow
import pymongo.errors
from fp.fp import FreeProxy
from airport_info import get_airport_icao
from airline_info import get_airline_icao
import flight_data_source

logger = logging.getLogger(__name__)


def request_lux_data():
    proxies = {"https": FreeProxy(https=True).get()}
    url = (
        "https://www.lux-airport.lu/wp-content/themes/lux-airport/"
        "flightsinfo.php?arrivalsDepartures_action=getArrivalsDepartures&"
        "lang=en"
    )
    response = requests.get(url, timeout=10, proxies=proxies)
    response.raise_for_status()
    return response.json()


airport_names = {
    "Zurich": "ZRH",
    "Porto": "OPO",
    "Frankfurt": "FRA",
    "London-Heathrow": "LHR",
    "Faro": "FAO",
    "Amsterdam": "AMS",
    "Paris - CDG": "CDG",
    "Toulouse Blagnac": "TLS",
    "Podgorica": "TGD",
    "Lisbon": "LIS",
    "Madrid": "MAD",
    "Belgrade": "BEG",
    "Marseille": "MRS",
    "Budapest": "BUD",
    "Venice": "VCE",
    "Munich": "MUC",
    "Lanzarote": "ACE",
    "Gran Canaria": "LPA",
    "Barcelona": "BCN",
    "Tenerife South-Reina": "TFS",
    "London-Stansted": "STN",
    "Athens": "ATH",
    "Milan-Bergamo": "BGY",
    "Istanbul": "IST",
    "Malta": "MLA",
    "Nice": "NCE",
    "Stockholm-Arlanda": "ARN",
    "London-City": "LCY",
    "Dublin": "DUB",
    "Milan-Malpensa": "MXP",
    "Copenhagen": "CPH",
    "Geneva": "GVA",
    "Vienna": "VIE",
    "Warsaw": "WAW",
    "Hamburg": "HAM",
    "Hurghada": "HRG",
    "Rome-Fiumicino": "FCO",
    "Berlin-Brandenburg": "BER",
    "Djerba": "DJE",
    "Palma de Mallorca": "PMI",
    "Malaga": "AGP",
    "Krakow": "KRK",
    "Prague": "PRG",
    "Oslo": "OSL",
    "Bucarest": "OTP",
    "Dubai": "DXB",
    "Bari": "BRI",
    "Bologna": "BLQ",
    "Funchal": "FNC",
    "San Pedro Airport": "VXE",
    "Sal": "SID",
    "Montpellier": "MPL",
    "Marsa-Alam": "RMF",
    "Dakar": "DSS",
    "Boa Vista": "BVC",
    "Innsbruck": "INN",
    "Fuerteventura": "FUE",
    "Agadir": "AGA",
    "Zaventem": "BRU",
    "Luxembourg": "LUX",
}


def _recent_timestamp(flight):
    if len(flight["estimatedTime"]) > 0:
        _timestring = "{}T{}".format(
            flight["scheduledDate"], flight["estimatedTime"]
        )
    else:
        _timestring = "{}T{}".format(
            flight["scheduledDate"], flight["scheduledTime"]
        )
    return int(arrow.get(_timestring, tzinfo="Europe/Luxembourg").timestamp())


class Airport(flight_data_source.FlightDataSource):
    def __init__(self):
        super().__init__("LUX")
        self.status_codes = {
            "1": "closed",
            "2": "delayed",
            "3": "taxiing",
            "4": "scheduled",
            "8": "cancelled",
            "9": "expected",
            "10": "take off",
            "11": "boarding",
            "12": "check-in",
            "13": "arrived",
        }
        self.known_status_codes = set(self.status_codes)
        self.missing_airports = set()

    def _get_status(self, status_code, remarks):
        if status_code not in self.status_codes:
            self.status_codes[status_code] = remarks.lower()
        return self.status_codes[status_code]

    def update_data(self):
        data = request_lux_data()
        arriving_flight_numbers = set()
        departing_flight_numbers = set()
        for _flight in data["arrivals"]:
            _airline_iata = _flight["flightNumber"][0:2]
            _flight_number = int(_flight["flightNumber"][2:])
            _airline_name = _flight["airlineName"]
            arriving_flight_numbers.add(
                "{}_{}".format(_airline_iata, _flight_number)
            )
            _airline_icao = get_airline_icao(
                _airline_iata, _airline_name, _flight_number
            )
            _lux_flight = {
                "status": self._get_status(
                    _flight["statusCode"], _flight["remarks"]
                ),
                "airline_iata": _airline_iata,
                "airline_icao": _airline_icao,
                "flight_number": _flight_number,
                "airline_name": _airline_name,
                "arrival": _recent_timestamp(_flight),
            }
            _lux_flight["_id"] = "{}_{}_{}".format(
                _airline_iata, _flight_number, _flight["scheduledDate"]
            )
            _airport_name = _flight["airportName"]
            _airport_iata = airport_names.get(_airport_name)
            if _airport_iata is None:
                self.missing_airports.add(_airport_name)
                logger.warning(f"{_airport_name} is unknown.")
                continue
            _origin_icao = get_airport_icao(_airport_iata)
            _route_items = [_origin_icao]
            if len(_flight["airportStepover"]) > 0:
                _via_name = _flight["airportStepover"].split(
                    " <span>via</span> "
                )[1]
                _via_iata = airport_names.get(_via_name)
                if _via_iata is None:
                    self.missing_airports.add(_via_name)
                    logger.warning(f"IATA unknown for {_via_name}.")
                    continue
                _route_items.append(get_airport_icao(_via_iata))
            _route_items.append("ELLX")
            _lux_flight["route"] = "-".join(_route_items)
            try:
                self.mycol.insert_one(_lux_flight)
            except pymongo.errors.DuplicateKeyError:
                if _lux_flight["status"] != "scheduled":
                    self.mycol.update_one(
                        {"_id": _lux_flight["_id"]}, {"$set": _lux_flight}
                    )
        for _flight in data["departures"]:
            _airline_iata = _flight["flightNumber"][0:2]
            _flight_number = int(_flight["flightNumber"][2:])
            _airline_name = _flight["airlineName"]
            departing_flight_numbers.add(
                "{}_{}".format(_airline_iata, _flight_number)
            )
            _airline_icao = get_airline_icao(
                _airline_iata, _airline_name, _flight_number
            )
            _lux_flight = {
                "status": self._get_status(
                    _flight["statusCode"], _flight["remarks"]
                ),
                "airline_iata": _airline_iata,
                "airline_icao": _airline_icao,
                "flight_number": _flight_number,
                "airline_name": _airline_name,
                "departure": _recent_timestamp(_flight),
            }
            _lux_flight["_id"] = "{}_{}_{}".format(
                _airline_iata, _flight_number, _flight["scheduledDate"]
            )
            _airport_name = _flight["airportName"]
            _airport_iata = airport_names.get(_airport_name)
            if _airport_iata is None:
                self.missing_airports.add(_airport_name)
                logger.warning(f"{_airport_name} is unknown.")
                continue
            _origin_icao = get_airport_icao(_airport_iata)
            _route_items = ["ELLX"]
            if len(_flight["airportStepover"]) > 0:
                _via_name = _flight["airportStepover"].split(
                    " <span>via</span> "
                )[1]
                _via_iata = airport_names.get(_via_name)
                if _via_iata is None:
                    self.missing_airports.add(_via_name)
                    logger.warning(f"IATA unknown for {_via_name}.")
                    continue
                _route_items.append(get_airport_icao(_via_iata))
            _route_items.append(_origin_icao)
            _lux_flight["route"] = "-".join(_route_items)
            try:
                self.mycol.insert_one(_lux_flight)
            except pymongo.errors.DuplicateKeyError:
                if _lux_flight["status"] != "scheduled":
                    self.mycol.update_one(
                        {"_id": _lux_flight["_id"]}, {"$set": _lux_flight}
                    )
            overlapping_flights = arriving_flight_numbers.intersection(
                departing_flight_numbers
            )
        if len(overlapping_flights) > 0:
            logger.warning(f"overlapping flights: {overlapping_flights}")

    def check_metadata(self):
        if len(self.missing_airports) > 0:
            print(f"missing airports: {list(self.missing_airports)}")
        if (
            len(self.known_status_codes.symmetric_difference(self.status_codes))
            > 0
        ):
            print(f"new status codes discovered: {self.status_codes}")


if __name__ == "__main__":
    airport = Airport()
    airport.update_data()
    airport.check_metadata()
