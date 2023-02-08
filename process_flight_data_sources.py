#!/usr/bin/env python3
import time
import json
import logging
import arrow
import redis
import fmo_data
import ham_data
import avinor_data
import lh_cargo_data
from route_utils import route_check_simple, estimate_progress
from route_info import (
    set_checked_flightroute,
    get_recent_callsigns,
    increase_error_count,
)
import flight_data_source
from opensky_flights_info import OpenSkyFlights
import vrs_standing_data as vsd
import pp_flightroute_data as pfd

logging.basicConfig(level=logging.INFO)
redis_connection = redis.Redis(decode_responses=True)
osf = OpenSkyFlights()


def _filter_candidates(candidates, route):
    # We check if any of the candidates was recently detected with the
    # desired flight route by OpenSky Network's processing.
    return [
        _callsign
        for _callsign in candidates
        if osf.get_routes_by_callsign(_callsign) == route
        or vsd.get_flight_route(_callsign) == route
        or pfd.get_flight_route(_callsign) == route
    ]


def process_data_source(
    data_source: flight_data_source.FlightDataSource
) -> None:
    for _flight in data_source.get_active_flights(utc):
        if _flight.get("status") == "cancelled":
            logging.debug("skipping cancelled flight: {_flight}")
            continue
        _airline_icao = _flight.get("airline_icao")
        if _airline_icao is None:
            logging.warning(f"airline_icao is missing: {_flight}")
            continue
        _key = "{}_{}_{}".format(
            _flight.get("airline_iata"),
            _flight["flight_number"],
            _flight["route"],
        )
        assumed_callsign = "{}{}".format(
            _flight["airline_icao"], _flight["flight_number"]
        )
        translated_callsign = redis_connection.hget(
            "callsign_translation", assumed_callsign
        )
        _callsign = None
        _quality = 0
        # This is the most simple way to match callsign and flight.
        if assumed_callsign in active_flights:
            _callsign = assumed_callsign
            _quality = 5
        elif assumed_callsign in recent_callsigns:
            logging.debug(
                "not (found) in the air: {} {}".format(
                    assumed_callsign, _flight["route"]
                )
            )
            continue
        elif translated_callsign is None:
            pass
        # The translation table needs manual maintenance.
        elif translated_callsign in active_flights:
            _callsign = translated_callsign
            _quality = 3
        elif translated_callsign in recent_callsigns:
            logging.debug(
                "not (found) in the air: {} {}".format(
                    translated_callsign, _flight["route"]
                )
            )
            continue
        if _callsign is not None:
            # We need to validate our match.
            check_result = route_check_simple(
                active_flights[_callsign], _flight["route"]
            )
            if check_result is None:
                continue
            if not check_result["check_failed"]:
                _flight["callsign"] = _callsign
                _flight["source"] = _data_source.source
                set_checked_flightroute(_flight, quality=_quality)
            else:
                logging.warning(
                    f"check failed for: {_callsign} {_flight['route']}"
                )
                logging.debug(
                    "check failed for: {} {} {}".format(
                        _callsign, check_result, active_flights[_callsign]
                    )
                )
                increase_error_count(_callsign, _flight["route"])
            continue
        _my_progress = float("nan")
        _time_progress = estimate_progress(_flight, utc)
        # At this point, we compare aircraft positions of the target operator
        # to the wanted flight.
        for _candidate in active_flights:
            if _candidate[:3] != _airline_icao:
                continue
            if _candidate in recent_callsigns:
                # If a simple fit for this flight has been seen recently,
                # we skip the following process.
                continue
            _check_result = route_check_simple(
                active_flights[_candidate], _flight["route"]
            )
            if _check_result is None:
                logging.warning(
                    "check not possible for {} {}".format(
                        active_flights[_candidate], _flight
                    )
                )
                continue
            if _check_result["check_failed"] == True:
                redis_connection.sadd(f"failed_candidates:{_key}", _candidate)
                redis_connection.expire(f"failed_candidates:{_key}", 24 * 3600)
                increase_error_count(_candidate, _flight["route"])
                continue
            elif _check_result["check_failed"] == False:
                if -0.4 < _check_result["progress"] - _time_progress < 0.2:
                    redis_connection.sadd(f"candidates:{_key}", _candidate)
                    redis_connection.expire(f"candidates:{_key}", 24 * 3600)
        if 1 > _time_progress > 0.1:
            _first_choice = redis_connection.sdiff(
                f"candidates:{_key}", f"failed_candidates:{_key}"
            ).difference(recent_callsigns)
            _second_choice = (
                redis_connection.smembers(f"candidates:{_key}")
                .difference(recent_callsigns)
                .difference(_first_choice)
            )
            _first_candidates = _filter_candidates(
                _first_choice, _flight["route"]
            )
            if len(_first_candidates) == 1:
                _quality = 1
                _callsign = _first_candidates[0]
                _flight["callsign"] = _callsign
                _flight["source"] = _data_source.source
                set_checked_flightroute(_flight, quality=_quality)
                continue
            elif len(_first_candidates) == 0:
                _second_candidates = _filter_candidates(
                    _second_choice, _flight["route"]
                )
                if len(_second_candidates) == 1:
                    _quality = 0
                    _callsign = _second_candidates[0]
                    _flight["callsign"] = _callsign
                    _flight["source"] = _data_source.source
                    set_checked_flightroute(_flight, quality=_quality)
                    continue


if __name__ == "__main__":
    data_sources = [
        fmo_data.Airport(),
        ham_data.Airport(),
        avinor_data.Airport(),
        lh_cargo_data.Airline(),
    ]

    supported_airlines = set()
    while True:
        t_start = time.time()
        for _data_source in data_sources:
            supported_airlines.update(_data_source.get_supported_airlines())
        recent_callsigns = get_recent_callsigns()
        opensky_data = redis_connection.get("opensky_positions")
        if opensky_data is None:
            time.sleep(5)
            continue
        opensky_data = json.loads(opensky_data)
        active_flights = opensky_data["positions"]
        for _flight in active_flights.values():
            operator_icao = _flight["operator_icao"]
            if operator_icao not in supported_airlines:
                continue
            redis_connection.sadd(
                f"aircraft_icao24s:{operator_icao}", _flight["icao24"]
            )
        utc = opensky_data["states_time"]
        logging.info(
            "### UTC {} ###".format(
                arrow.get(utc).format("YYYY-MM-DD HH:mm:ss")
            )
        )
        for _data_source in data_sources:
            process_data_source(_data_source)
        t_end = time.time()
        processing_time = t_end - t_start
        logging.info(f"processing time: {processing_time:.2f}s")
        sleep_time = max((45 - processing_time), 0)
        logging.info(f"### sleeping for {sleep_time:.2f} seconds. ###")
        time.sleep(sleep_time)
