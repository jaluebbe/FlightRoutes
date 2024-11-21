#!/usr/bin/env python3
import logging
import arrow
import redis
import fmo_data
import ham_data
import avinor_data
import lh_cargo_data
import anac_data
from route_utils import route_check_simple, estimate_progress
from route_info import get_recent_callsigns, get_flights_by_number
import flight_data_source
from opensky_utils import validated_callsign

logging.basicConfig(level=logging.INFO)
redis_connection = redis.Redis(decode_responses=True)


def process_data_source(
    data_source: flight_data_source.FlightDataSource
) -> None:
    for _flight in data_source.get_flights_of_day(
        arrow.utcnow().shift(days=-1)
    ):
        _results = get_flights_by_number(
            _flight["airline_iata"], _flight["flight_number"]
        )
        assumed_callsign = "{}{}".format(
            _flight["airline_icao"], _flight["flight_number"]
        )
        if len(_results) == 0:
            yield (
                assumed_callsign,
                "{}{}".format(
                    _flight["airline_iata"], _flight["flight_number"]
                ),
                _flight["route"],
            )


if __name__ == "__main__":
    data_sources = [
        fmo_data.Airport(),
        ham_data.Airport(),
        lh_cargo_data.Airline(),
        avinor_data.Airport(),
        anac_data.Agency(),
    ]
    for _data_source in data_sources:
        print(f"### missing flight mappings ({_data_source.source}) ###")
        for _flight_icao, _flight_iata, _flight_route in process_data_source(
            _data_source
        ):
            _existing_mapping = redis_connection.hget(
                "callsign_translation", _flight_icao
            )
            _callsign_input = validated_callsign(
                input(
                    f"{_flight_icao:<7} {_flight_iata:<6} {_flight_route} "
                    f"[{_existing_mapping}]: "
                )
            )
            if _callsign_input is None:
                continue
            _callsign = _callsign_input["callsign"]
            print(f"adding {_flight_icao} -> {_callsign}")
            redis_connection.hset(
                "callsign_translation", _flight_icao, _callsign
            )
