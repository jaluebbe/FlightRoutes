from pygeodesy.ellipsoidalVincenty import LatLon
from airport_info import get_airport_info


def get_single_route_length(origin_icao, destination_icao):
    origin = get_airport_info(origin_icao)
    destination = get_airport_info(destination_icao)
    distance = LatLon(origin["Latitude"], origin["Longitude"]).distanceTo(
        LatLon(destination["Latitude"], destination["Longitude"])
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
