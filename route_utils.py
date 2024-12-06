import time
import logging
import numpy as np
import arrow
import pygeodesy.ellipsoidalVincenty as ev
import pygeodesy.ellipsoidalExact as ee
from airport_info import get_airport_info, get_airport_iata


def convert_to_iata_route(route: str) -> str:
    icaos = route.split("-")
    iatas = [get_airport_iata(_icao) or _icao for _icao in icaos]
    return "-".join(iatas)


def check_route_airports(route: str, valid_airports: list = None) -> str | None:
    airport_pattern = re.compile(r"^[A-Z]{2}[0-9A-Z]{2}$")
    airports = route.split("-")
    if not all(airport_pattern.match(airport) for airport in airports):
        return None
    if valid_airports is not None:
        if not all(airport in valid_airports for airport in airports):
            return None
    return route


def get_single_route_length(origin_icao, destination_icao):
    origin = get_airport_info(origin_icao)
    destination = get_airport_info(destination_icao)
    try:
        distance = ev.LatLon(
            origin["Latitude"], origin["Longitude"]
        ).distanceTo(
            ev.LatLon(destination["Latitude"], destination["Longitude"])
        )
    except ev.VincentyError:
        # this method is much slower and being used as fallback only
        distance = ee.LatLon(
            origin["Latitude"], origin["Longitude"]
        ).distanceTo(
            ee.LatLon(destination["Latitude"], destination["Longitude"])
        )
        logging.exception(
            f"get_single_route_length({origin_icao}, {destination_icao})"
        )
    return distance


def get_route_length(route):
    icaos = route.split("-")
    assert len(icaos) >= 2
    return sum(
        [
            get_single_route_length(*icaos[i : i + 2])
            for i in range(len(icaos) - 1)
        ]
    )


def estimate_max_flight_duration(distance, factor=0.00486, offset=1500):
    return factor * distance + 1500


def estimate_progress(flight, utc=None):
    if utc is None:
        utc = int(time.time())
    if not None in (flight.get("arrival"), flight.get("departure")):
        duration = flight["arrival"] - flight["departure"]
        return (utc - flight["departure"]) / duration
    else:
        distance = get_route_length(flight["route"])
        max_duration = estimate_max_flight_duration(distance)
    if flight.get("arrival") is not None:
        return (utc - (flight["arrival"] - max_duration)) / max_duration
    elif flight.get("departure") is not None:
        return (utc - flight["departure"]) / max_duration


def single_route_check_simple(position, route):
    icaos = route.split("-")
    if len(icaos) != 2:
        return None
    origin = get_airport_info(icaos[0])
    destination = get_airport_info(icaos[1])
    if None in (origin, destination):
        return None
    if origin == destination:
        return None
    if None in [position["latitude"], position["longitude"]]:
        return None
    try:
        _origin = ev.LatLon(origin["Latitude"], origin["Longitude"])
        _destination = ev.LatLon(
            destination["Latitude"], destination["Longitude"]
        )
        route_length = _origin.distanceTo(_destination)
        position["LatLon"] = ev.LatLon(
            position["latitude"], position["longitude"]
        )
        temp = _origin.distanceTo3(position["LatLon"])
        dist_origin = temp[0]
        bearing_from_origin = temp[2]
        temp = position["LatLon"].distanceTo3(_destination)
        dist_destination = temp[0]
        bearing_to_destination = temp[1]
    except ev.VincentyError:
        _origin = ee.LatLon(origin["Latitude"], origin["Longitude"])
        _destination = ee.LatLon(
            destination["Latitude"], destination["Longitude"]
        )
        route_length = _origin.distanceTo(_destination)
        position["LatLon"] = ee.LatLon(
            position["latitude"], position["longitude"]
        )
        temp = _origin.distanceTo3(position["LatLon"])
        dist_origin = temp[0]
        bearing_from_origin = temp[2]
        temp = position["LatLon"].distanceTo3(_destination)
        dist_destination = temp[0]
        bearing_to_destination = temp[1]
        logging.exception(f"single_route_check_simple({position}, {route})")
    deviation = dist_origin + dist_destination - route_length
    error_angle = np.abs(
        (position["heading"] - bearing_to_destination + 180) % 360 - 180
    )
    progress = dist_origin / (dist_origin + dist_destination)
    deviation_ratio = deviation / route_length
    response = {
        "route_length": route_length,
        "route": route,
        "deviation": deviation,
        "error_angle": error_angle,
        "deviation_ratio": deviation_ratio,
        "progress": progress,
        "check_failed": False,
        "dist_origin": dist_origin,
        "dist_destination": dist_destination,
        "callsign": position["callsign"],
        "icao24": position["icao24"],
    }
    if position["on_ground"]:
        if min(dist_origin, dist_destination) > 5e3:
            response["check_failed"] = True
        return response
    altitude = position["altitude"]
    vertical_rate = position["vertical_rate"]
    # A deviation of more than 15% route length but at least 265km as well as
    # deviations larger than 60% are not accepted in general.
    # Vertical speed below -5m/s is not accepted within the first 20% of the
    # flight distance. In the last 20% of the flight distance, a vertical rate
    # above 5.5m/s is not accepted.
    # Further checks take the deviation from the bearing to the destination
    # into account. These checks are considered only in the middle segment of
    # the flight to avoid confusion by holding patterns, departure and landing.
    if deviation > 265e3 and deviation_ratio > 0.15:
        response["check_failed"] = True
    elif deviation_ratio > 0.6:
        response["check_failed"] = True
    elif (
        0.12 < progress
        and dist_origin > 81.5e3
        and progress < 0.85
        and dist_destination > 77e3
        and error_angle > 61.5
    ):
        response["check_failed"] = True
    elif (
        0.1 < progress
        and dist_origin > 25e3
        and progress < 0.85
        and dist_destination > 41e3
        and error_angle > 126
    ):
        response["check_failed"] = True
    elif progress < 0.2 and vertical_rate < -5:
        response["check_failed"] = True
    elif progress > 0.8 and vertical_rate > 5.5:
        response["check_failed"] = True
    response.update(
        {
            "vertical_rate": position["vertical_rate"],
            "velocity": position["velocity"],
            "altitude": altitude,
        }
    )
    return response


def route_check_simple(position, route):
    icaos = route.split("-")
    if len(icaos) < 2 or len(icaos) == 2 and icaos[0] == icaos[1]:
        return None
    route_info = []
    route_deviation = []
    route_angle = []
    routes_ok = []
    for i in range(len(icaos) - 1):
        route_data = single_route_check_simple(
            position, "-".join(icaos[i : i + 2])
        )
        if route_data is None:
            return None
        route_info.append(route_data)
        route_deviation.append(route_data["deviation"])
        route_angle.append(route_data["error_angle"])
        if route_data["check_failed"] == False:
            routes_ok.append(route_data)
    if len(routes_ok) == 1:
        return routes_ok[0]
    # For a flight consisting of multiple route segments, the segment with the
    # smallest deviation from the aircraft position is assumed to be the
    # current segment if the check didn't fail. Otherwise, the segment where
    # the heading fits best is selected.
    # todo prefer smalled heading deviation
    current_route = route_info[np.argmin(route_deviation)]
    if current_route["check_failed"] == False:
        return current_route
    return route_info[np.argmin(route_angle)]


def combine_routes(route_a, route_b):
    _route_items_a = route_a.split("-")
    _route_items_b = route_b.split("-")
    if _route_items_a == _route_items_b:
        return "-".join(_route_items_a)
    elif _route_items_a[1:] == _route_items_b[:-1]:
        return "-".join(_route_items_a + _route_items_b[-1:])
    elif _route_items_a[-1] == _route_items_b[0]:
        return "-".join(_route_items_a + _route_items_b[1:])


def combine_flights(flight_a, flight_b):
    _arrival_a = flight_a.get("arrival")
    _departure_a = flight_a.get("departure")
    _arrival_b = flight_b.get("arrival")
    _departure_b = flight_b.get("departure")

    if _arrival_a is not None and _departure_b is not None:
        if _departure_b > _arrival_a:
            _departure = _arrival_a - estimate_max_flight_duration(
                get_route_length(flight_a["route"])
            )
            _arrival = _departure_b + estimate_max_flight_duration(
                get_route_length(flight_b["route"])
            )
            _route = combine_routes(flight_a["route"], flight_b["route"])
        elif _departure_b < _arrival_a:
            _departure = _departure_b
            _arrival = _arrival_a
            _route = combine_routes(flight_b["route"], flight_a["route"])
    elif _arrival_b is not None and _departure_a is not None:
        if _departure_a > _arrival_b:
            _departure = _arrival_b - estimate_max_flight_duration(
                get_route_length(flight_b["route"])
            )
            _arrival = _departure_a + estimate_max_flight_duration(
                get_route_length(flight_a["route"])
            )
            _route = combine_routes(flight_b["route"], flight_a["route"])
        elif _departure_a < _arrival_b:
            _departure = _departure_a
            _arrival = _arrival_b
            _route = combine_routes(flight_a["route"], flight_b["route"])
    else:
        return
    _duration = _arrival - _departure
    if _duration > 18 * 3600:
        return None
    if _route is None:
        return
    _date = arrow.get(_departure).format("YYYY-MM-DD")
    _airline_iata = flight_a["airline_iata"]
    _flight_number = flight_a["flight_number"]
    response = {
        "_id": f"{_airline_iata}_{_flight_number}_{_date}_{_route}",
        "flight_number": _flight_number,
        "airline_iata": _airline_iata,
        "airline_icao": flight_a["airline_icao"],
        "departure": _departure,
        "arrival": _arrival,
        "route": _route,
    }
    if flight_a.get("airline_name") is not None:
        response["airline_name"] = flight_a["airline_name"]
    return response
