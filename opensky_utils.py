import re
import json
import logging
import pathlib
import redis
import requests

PWD = pathlib.Path(__file__).resolve().parent

redis_connection = redis.Redis(decode_responses=True)

logger = logging.getLogger(__name__)

# Regexp checking for callsign similarity rules (Eurocontrol CSS rule ZG00).
callsign_validator = re.compile(
    "^([A-Z]{3})[0-9](([0-9]{0,3})|([0-9]{0,2})([A-Z])|([0-9]?)([A-Z]{2}))$"
)
suffix_validator = re.compile("^[1-9]")

# Mapping between icao24 and aircraft registration loaded from the frozen
# OpenSky aircraft database snapshot. Refresh using prepare_aircraft_data.py
# followed by reload_icao24_to_registration().
icao24_to_registration = {}


def reload_icao24_to_registration() -> None:
    with open(PWD / "icao24_to_registration.json", encoding="utf-8") as _f:
        _data = json.load(_f)
    icao24_to_registration.clear()
    icao24_to_registration.update(_data)


def update_icao24s_from_redis() -> None:
    icao24_to_registration.update(redis_connection.hgetall("icao24s"))


def dump_icao24_to_registration() -> None:
    _staging = PWD / "icao24_to_registration_new.json"
    _target = PWD / "icao24_to_registration.json"
    with open(_staging, "w", encoding="utf-8") as _f:
        json.dump(icao24_to_registration, _f)
    _staging.rename(_target)


def request_icao24_from_opensky(icao24: str) -> str | None:
    if redis_connection.sismember("unknown_icao24s", icao24):
        return None
    url = "https://opensky-network.org/api/metadata/aircraft/icao/"
    try:
        response = requests.get(url + icao24)
    except requests.exceptions.ConnectionError:
        logger.exception(f"Problem getting registration for {icao24}.")
        return None
    if response.status_code == 200:
        _data = response.json()
        registration = _data["registration"].replace("-", "").replace(".", "")
        if registration != "":
            redis_connection.hset("icao24s", icao24, registration)
            redis_connection.sadd("icao24s_from_api", icao24)
            logger.info(
                f"icao24 {icao24} ({registration}) retrieved from API — "
                f"not present in frozen aircraft database."
            )
            return registration
    redis_connection.sadd("unknown_icao24s", icao24)
    return None


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
    # Check if the callsign fits Eurocontrol CSS rule ZG00.
    if callsign_validator.match(callsign) is None:
        return None
    # Split callsign into operator and suffix and remove leading zeros.
    operator = callsign[:3]
    suffix = callsign[3:].lstrip("0")
    if suffix_validator.match(suffix) is None:
        return None
    callsign = f"{operator}{suffix}"
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
    if opensky_state.on_ground:
        required_fields = {
            k: v
            for k, v in position.items()
            if k not in ["vertical_rate", "altitude"]
        }
    else:
        required_fields = position
    if None in required_fields.values():
        return None
    if opensky_state.on_ground and not allow_on_ground:
        return None
    if not opensky_state.on_ground:
        flight_level = int(round(opensky_state.baro_altitude / 0.3048 / 100))
        # Maximum FL for Concorde.
        if flight_level > 600:
            return None
        position["flight_level"] = flight_level
    position["sensors"] = (
        [] if opensky_state.sensors is None else opensky_state.sensors
    )
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
