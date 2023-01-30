#!/usr/bin/env python3
import os
import csv
import logging
import arrow
from airport_info import get_airport_info, get_airport_icao
from airline_info import get_airline_icao
import flight_data_source

PWD = os.path.dirname(os.path.abspath(__file__))
_day_labels = ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"]


def _process_flight(flight, utc):
    if flight["ACtype"] in ["RFC", "RFS"]:
        # ignore truck services
        return
    airline_iata = flight["AL"]
    flight_number = int(flight["FNR"][2:])
    airline_icao = get_airline_icao(airline_iata, flight_number=flight_number)
    if airline_iata == "4Y":
        airline_icao = "OCN"
    if airline_icao is None:
        logging.warning(f"Airline ICAO for {airline_iata} not found.")
        return
    origin_icao = get_airport_icao(flight["DEP"])
    destination_icao = get_airport_icao(flight["ARR"])
    origin_info = get_airport_info(origin_icao)
    destination_info = get_airport_info(destination_icao)
    _start_operation = arrow.get(
        flight["Start_Op"], "DDMMMYY", tzinfo=origin_info["Timezone"]
    )
    _end_operation = arrow.get(
        flight["End_Op"], "DDMMMYY", tzinfo=origin_info["Timezone"]
    ).ceil("day")
    if not _start_operation <= utc <= _end_operation:
        return
    _frequency = "".join([flight[day] for day in _day_labels])
    if str(utc.isoweekday()) not in _frequency:
        return
    _departure_date = utc.shift(days=int(flight["DDC"])).format("YYYYMMDD")
    _departure = arrow.get(
        f"{_departure_date}T{flight['STD']}", tzinfo=origin_info["Timezone"]
    )
    _arrival_date = utc.shift(days=int(flight["ADC"])).format("YYYYMMDD")
    _arrival = arrow.get(
        f"{_arrival_date}T{flight['STD']}", tzinfo=destination_info["Timezone"]
    )
    _route = f"{origin_icao}-{destination_icao}"
    return {
        "_id": f"{airline_iata}_{flight_number}_{_route}_{utc.format('YYYYMMDD')}",
        "airline_iata": airline_iata,
        "airline_icao": airline_icao,
        "flight_number": flight_number,
        "route": _route,
        "departure": int(_departure.timestamp()),
        "arival": int(_arrival.timestamp()),
        "segment_number": int(flight["SNR"]),
    }


def extract_flights_from_csv(utc):
    file_name = "LHcargo_FlightSchedule.csv"
    with open(os.path.join(PWD, file_name)) as f:
        _reader = csv.reader(f, delimiter=";")
        _schedule_date = next(_reader)
        _header = next(_reader)
        for _row in _reader:
            if not _row[0].isdigit():
                continue
            _flight = _process_flight(dict(zip(_header, _row)), utc)
            if _flight is None:
                continue
            yield _flight


class Airline(flight_data_source.FlightDataSource):
    def __init__(self):
        super().__init__("LH_Cargo", category="airlines")

    def update_data(self, utc=None):
        if utc is None:
            _utc = arrow.utcnow()
        else:
            _utc = arrow.get(utc)
        _flights = list(extract_flights_from_csv(_utc))
        multiple_segments = {
            (_flight["airline_iata"], _flight["flight_number"])
            for _flight in _flights
            if _flight["segment_number"] > 0
        }
        _filtered_flights = [
            _flight
            for _flight in _flights
            if not (
                (_flight["airline_iata"], _flight["flight_number"])
                in multiple_segments
                and _flight["segment_number"] == 0
            )
        ]
        for _flight in _filtered_flights:
            self.update_flight(_flight)


if __name__ == "__main__":
    airline = Airline()
    # fill the MongoDB with data for tomorrow:
    airline.update_data(arrow.utcnow().shift(days=1))
