#!/usr/bin/env python3
import logging
import redis
from route_info import get_recent_callsigns
from opensky_utils import validated_callsign
from opensky_flights_info import OpenSkyFlights

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    osf = OpenSkyFlights()
    redis_connection = redis.Redis(decode_responses=True)
    recent_callsigns = get_recent_callsigns(min_quality=0, hours=24 * 7)
    print("### missing flight mappings ###")
    for _flight in osf.get_flights_of_day():
        _callsign = _flight["callsign"]
        if _callsign in recent_callsigns:
            continue
        if (
            validated_callsign(
                _callsign,
                accepted_operators=["AUA", "BEL", "DLH", "GEC", "OCN", "SXS"],
                allow_numerical_callsign=False,
                allow_alphanumerical_callsign=True,
            )
            is None
        ):
            continue
        if _callsign in redis_connection.hvals("callsign_translation"):
            continue
        _origin = _flight["origin"]
        _destination = _flight["destination"]
        _validated_input = validated_callsign(
            input(f"{_callsign:<7} {_origin}-{_destination} : "),
            allow_numerical_callsign=True,
            allow_alphanumerical_callsign=False,
        )
        if _validated_input is None:
            continue
        _flight_icao = _validated_input["callsign"]
        print(f"adding {_flight_icao} -> {_callsign}")
        redis_connection.hset("callsign_translation", _flight_icao, _callsign)
