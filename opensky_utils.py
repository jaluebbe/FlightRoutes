import re
import json

# regexp checking for callsign similarity rules
callsign_validator = re.compile(
    "^([A-Z]{3})[0-9](([0-9]{0,3})|([0-9]{0,2})([A-Z])|([0-9]?)([A-Z]{2}))$"
)
suffix_validator = re.compile("^[1-9]")

# This mapping between icao24 and aircraft registration should be refreshed
# frequently using prepare_aircraft_data.py .
icao24_to_registration = json.load(open("icao24_to_registration.json"))

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
    if opensky_state.callsign is None:
        return None
    # Remove trailing whitespaces and convert to upper case.
    callsign = opensky_state.callsign.strip().upper()
    # Check if the callsigns fits to Eurocontrol CSS rule ZG00.
    if callsign_validator.match(callsign) is None:
        return None
    # Split callsign into operator and suffix and remove leading zeros.
    operator = callsign[:3]
    suffix = callsign[3:].lstrip("0")
    if suffix_validator.match(suffix) is None:
        return None
    callsign = "{}{}".format(operator, suffix)
    position = {
        "utc": opensky_state.time_position,
        "latitude": opensky_state.latitude,
        "longitude": opensky_state.longitude,
        "altitude": opensky_state.baro_altitude,
        "heading": opensky_state.heading,
        "callsign": callsign,
        "icao24": opensky_state.icao24,
        "vertical_rate": opensky_state.vertical_rate,
        "velocity": opensky_state.velocity,
        "time_position": opensky_state.time_position,
        "on_ground": opensky_state.on_ground,
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
    operator = callsign[:3]
    if accepted_operators is not None and operator not in accepted_operators:
        return None
    position["operator_icao"] = operator
    if opensky_state.sensors is None:
        position["sensors"] = []
    else:
        position["sensors"] = opensky_state.sensors
    if suffix.isdigit() and allow_numerical_callsign:
        position["callsign_number"] = int(suffix)
    elif allow_alphanumerical_callsign:
        pass
    else:
        return None
    if use_registration:
        registration = icao24_to_registration.get(position["icao24"])
        if registration is not None:
            position["registration"] = registration
    return position
