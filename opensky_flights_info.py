#!/usr/bin/env python3
import json
import logging
import arrow
import requests
import redis
import pymongo

from opensky_utils import validated_callsign

from config import OPENSKY_USER, OPENSKY_PASSWORD

logging.basicConfig(level=logging.INFO)

URL = url = "https://opensky-network.org/api/flights/aircraft"


class OpenSkyFlights:
    def __init__(self):
        self.myclient = pymongo.MongoClient("mongodb://localhost:27017/")
        self.mydb = self.myclient["opensky"]
        self.mycol = self.mydb["flights"]
        if "callsign_1" not in self.mycol.index_information():
            mycol.create_index("callsign")
        self.redis_connection = redis.Redis(decode_responses=True)

    def get_routes_by_callsign(self, callsign):
        routes = {
            "-".join([_flight["origin"], _flight["destination"]])
            for _flight in self.mycol.find({"callsign": callsign})
        }
        if len(routes) != 1:
            logging.debug(f"multiple routes for {callsign}: {routes}")
            return
        return routes.pop()

    def _process_flights(self, flights):
        for _flight in flights:
            _callsign_validation = validated_callsign(_flight["callsign"])
            if _callsign_validation is None:
                continue
            _callsign = _callsign_validation["callsign"]
            _operator = _callsign_validation["operator_icao"]
            _origin = _flight["estDepartureAirport"]
            _destination = _flight["estArrivalAirport"]
            _first_seen = _flight["firstSeen"]
            _last_seen = _flight["lastSeen"]
            _icao24 = _flight["icao24"]
            if None in (_origin, _destination):
                continue
            _id = f"{_icao24}_{_first_seen}"
            _osn_flight = {
                "callsign": _callsign,
                "airline_icao": _operator,
                "origin": _origin,
                "destination": _destination,
                "first_seen": _first_seen,
                "last_seen": _last_seen,
                "icao24": _icao24,
            }
            logging.debug(f"added flight: {_osn_flight}")
            self.mycol.update_one(
                {"_id": _id}, {"$set": _osn_flight}, upsert=True
            )

    def update_data(self):
        session = requests.Session()
        session.auth = (OPENSKY_USER, OPENSKY_PASSWORD)
        _utc = arrow.utcnow()
        _begin = _utc.shift(days=-2).floor("day").timestamp()
        _end = _utc.shift(days=-1).ceil("day").timestamp()
        _params = {"begin": int(_begin), "end": int(_end)}
        for _key in self.redis_connection.scan_iter("aircraft_icao24s:*"):
            for _icao24 in self.redis_connection.smembers(_key):
                _params["icao24"] = _icao24
                _response = session.get(URL, params=_params)
                if _response.status_code != 200:
                    continue
                self._process_flights(_response.json())


if __name__ == "__main__":
    osf = OpenSkyFlights()
    osf.update_data()
