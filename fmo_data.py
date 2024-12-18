#!/usr/bin/env python3
import logging
import requests
import arrow
import xmltodict
from airport_info import get_airport_icao
from airline_info import get_airline_icao
import flight_data_source


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


_status_codes = {
    "GTO": "gate_open",
    "BOR": "boarding",
    "GCL": "gate_closed",
    "TXI": "taxiing",
    "DEP": "departed",
    "ARR": "arrived",
    "DLY": "delayed",
    "CXXT": "cancelled",
    "CXXO": "cancelled",
}


class Airport(flight_data_source.FlightDataSource):
    def __init__(self):
        super().__init__("FMO")

    def update_data(self):
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
            if _flight["FNR"][2] == " ":
                _airline_iata = _flight["FNR"][:2]
                if _flight["FNR"][3:].isdecimal():
                    _flight_number = int(_flight["FNR"][3:])
                else:
                    logging.warning(f"Problem with {_flight['FNR']}")
                    continue
            else:
                continue
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
            self.update_flight(_fmo_flight)


if __name__ == "__main__":
    airport = Airport()
    airport.update_data()
