import re
import os
import json
import redis
import requests

PWD = os.path.dirname(os.path.abspath(__file__))
redis_connection = redis.Redis(decode_responses=True)

# regexp checking for callsign similarity rules
callsign_validator = re.compile(
    "^([A-Z]{3})[0-9](([0-9]{0,3})|([0-9]{0,2})([A-Z])|([0-9]?)([A-Z]{2}))$"
)
suffix_validator = re.compile("^[1-9]")

# This mapping between icao24 and aircraft registration should be refreshed
# frequently using prepare_aircraft_data.py followed by
# reload_icao24_to_registration() .
icao24_to_registration = {}

# Call this method to load an updated version of icao24_to_registration.json .
def reload_icao24_to_registration():
    _data = json.load(open(os.path.join(PWD, "icao24_to_registration.json")))
    icao24_to_registration.clear()
    icao24_to_registration.update(_data)


def update_icao24s_from_redis():
    icao24_to_registration.update(redis_connection.hgetall("icao24s"))


def dump_icao24_to_registration():
    with open(os.path.join(PWD, "icao24_to_registration.json"), "w") as f:
        json.dump(icao24_to_registration, f)


def request_icao24_from_opensky(icao24: str):
    if redis_connection.sismember("unknown_icao24s", icao24):
        return
    url = "https://opensky-network.org/api/metadata/aircraft/icao/"
    response = requests.get(url + icao24)
    if response.status_code == 200:
        _data = response.json()
        registration = "".join(_data["registration"].split("-"))
        if registration != "":
            redis_connection.hset("icao24s", icao24, registration)
            return registration
    redis_connection.sadd("unknown_icao24s", icao24)


reload_icao24_to_registration()


def validated_callsign(
    callsign,
    accepted_operators=None,
    allow_numerical_callsign=True,
    allow_alphanumerical_callsign=True,
):
    if callsign is None:
        return None
    # Remove trailing whitespaces and convert to upper case.
    callsign = callsign.strip().upper()
    # Check if the callsigns fits to Eurocontrol CSS rule ZG00.
    if callsign_validator.match(callsign) is None:
        return None
    # Split callsign into operator and suffix and remove leading zeros.
    operator = callsign[:3]
    suffix = callsign[3:].lstrip("0")
    if suffix_validator.match(suffix) is None:
        return None
    callsign = "{}{}".format(operator, suffix)
    operator = callsign[:3]
    if accepted_operators is not None and operator not in accepted_operators:
        return None
    response = {"callsign": callsign, "operator_icao": operator}
    if allow_numerical_callsign and suffix.isdigit():
        response["callsign_number"] = int(suffix)
    elif not allow_numerical_callsign and suffix.isdigit():
        return None
    elif allow_alphanumerical_callsign:
        pass
    else:
        return None
    return response


# checking if a position from an OpenSky Network state could be used for
# flight route analysis and if the callsign is formatted as being an airline.
def validated_position(
    opensky_state,
    accepted_operators=None,
    allow_numerical_callsign=True,
    allow_alphanumerical_callsign=True,
    allow_on_ground=False,
    use_registration=True,
):
    callsign_check = validated_callsign(
        opensky_state.callsign,
        accepted_operators,
        allow_numerical_callsign,
        allow_alphanumerical_callsign,
    )
    if callsign_check is None:
        return None
    position = {
        "utc": opensky_state.time_position,
        "latitude": opensky_state.latitude,
        "longitude": opensky_state.longitude,
        "altitude": opensky_state.baro_altitude,
        "heading": opensky_state.true_track,
        "icao24": opensky_state.icao24,
        "vertical_rate": opensky_state.vertical_rate,
        "velocity": opensky_state.velocity,
        "time_position": opensky_state.time_position,
        "on_ground": opensky_state.on_ground,
        "category": opensky_state.category,
    }
    if None in position.values():
        # incomplete position data or invalid callsign
        return None
    if opensky_state.on_ground and not allow_on_ground:
        # For aircraft on ground, route and schedule verification will
        # mostly fail.
        return None
    flight_level = int(round(opensky_state.baro_altitude / 0.3048 / 100))
    # maximum FL for Concorde
    if flight_level > 600:
        return None
    position["flight_level"] = flight_level
    if opensky_state.sensors is None:
        position["sensors"] = []
    else:
        position["sensors"] = opensky_state.sensors
    if use_registration:
        registration = icao24_to_registration.get(position["icao24"])
        if registration is None:
            registration = request_icao24_from_opensky(position["icao24"])
        if registration is not None:
            position["registration"] = registration
    position.update(callsign_check)
    return position


if __name__ == "__main__":
    update_icao24s_from_redis()
    dump_icao24_to_registration()
