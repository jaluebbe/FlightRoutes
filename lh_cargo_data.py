#!/usr/bin/env python3
import csv
import logging
import pathlib
import arrow
from airport_info import get_airport_info, get_airport_icao
from airline_info import get_airline_icao
import flight_data_source

PWD = pathlib.Path(__file__).resolve().parent
logger = logging.getLogger(pathlib.Path(__file__).name)

_day_labels = ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"]


def _process_flight(
    flight: dict,
    utc: arrow.Arrow,
    unknown_airlines: set[str],
    unknown_airports: set[str],
) -> dict | None:
    if flight["ACtype"] in ["RFC", "RFS"]:
        return None
    airline_iata = flight["AL"]
    if not flight["FNR"][2:].isdigit():
        return None
    flight_number = int(flight["FNR"][2:])

    if airline_iata == "4Y":
        airline_icao = "BGA"
    else:
        airline_icao = get_airline_icao(
            airline_iata, flight_number=flight_number
        )
    if airline_icao is None:
        unknown_airlines.add(airline_iata)
        return None

    origin_iata = flight["DEP"]
    destination_iata = flight["ARR"]
    origin_icao = get_airport_icao(origin_iata)
    destination_icao = get_airport_icao(destination_iata)
    if origin_icao is None:
        unknown_airports.add(origin_iata)
        return None
    if destination_icao is None:
        unknown_airports.add(destination_iata)
        return None

    origin_info = get_airport_info(origin_icao)
    destination_info = get_airport_info(destination_icao)
    if origin_info is None or destination_info is None:
        return None

    _start_operation = arrow.get(
        flight["Start_Op"], "DDMMMYY", tzinfo=origin_info["Timezone"]
    )
    _end_operation = arrow.get(
        flight["End_Op"], "DDMMMYY", tzinfo=origin_info["Timezone"]
    ).ceil("day")
    if not _start_operation <= utc <= _end_operation:
        return None

    _frequency = "".join([flight[_day] for _day in _day_labels])
    if str(utc.isoweekday()) not in _frequency:
        return None

    _departure_date = utc.shift(days=int(flight["DDC"])).format("YYYYMMDD")
    _departure = arrow.get(
        f"{_departure_date}T{flight['STD']}", tzinfo=origin_info["Timezone"]
    )
    _arrival_date = utc.shift(days=int(flight["ADC"])).format("YYYYMMDD")
    _arrival = arrow.get(
        f"{_arrival_date}T{flight['STA']}", tzinfo=destination_info["Timezone"]
    )
    _route = f"{origin_icao}-{destination_icao}"
    return {
        "_id": f"{airline_iata}_{flight_number}_{_route}_{utc.format('YYYYMMDD')}",
        "airline_iata": airline_iata,
        "airline_icao": airline_icao,
        "flight_number": flight_number,
        "route": _route,
        "departure": int(_departure.timestamp()),
        "arrival": int(_arrival.timestamp()),
        "segment_number": int(flight["SNR"]),
    }


def extract_flights_from_csv(utc: arrow.Arrow) -> list[dict]:
    file_name = "LHcargo_FlightSchedule.csv"
    _unknown_airlines: set[str] = set()
    _unknown_airports: set[str] = set()
    _flights = []
    with open(PWD / file_name, encoding="utf-8") as _f:
        _reader = csv.reader(_f, delimiter=";")
        # First line is the schedule creation date (e.g. "SCD;05APR26").
        next(_reader)
        _header = next(_reader)
        for _row in _reader:
            if not _row[0].isdigit():
                continue
            _flight = _process_flight(
                dict(zip(_header, _row)),
                utc,
                _unknown_airlines,
                _unknown_airports,
            )
            if _flight is None:
                continue
            _flights.append(_flight)
    for _iata in sorted(_unknown_airlines):
        logger.warning(f"Airline ICAO for {_iata} not found.")
    for _iata in sorted(_unknown_airports):
        logger.warning(f"Unknown airport IATA: {_iata}")
    return _flights


class Airline(flight_data_source.FlightDataSource):
    def __init__(self):
        super().__init__("LH_Cargo", category="airlines")

    def update_data(self, utc=None) -> None:
        if utc is None:
            _utc = arrow.utcnow()
        else:
            _utc = arrow.get(utc)

        _flights = extract_flights_from_csv(_utc)

        # Filter out segment-0 records for flights that have higher-numbered
        # segments, keeping only the individual legs.
        _multi_segment = {
            (_flight["airline_iata"], _flight["flight_number"])
            for _flight in _flights
            if _flight["segment_number"] > 0
        }
        _stored = 0
        for _flight in _flights:
            if (
                _flight["airline_iata"],
                _flight["flight_number"],
            ) in _multi_segment and _flight["segment_number"] == 0:
                continue
            self.update_flight(_flight)
            _stored += 1

        logger.info(
            f"LH Cargo: stored {_stored} flights for "
            f"{_utc.format('YYYY-MM-DD')}."
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    airline = Airline()
    airline.update_data(arrow.utcnow().shift(days=1))
