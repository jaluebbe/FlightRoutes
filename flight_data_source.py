import pymongo
import arrow
from route_utils import estimate_max_flight_duration, get_route_length


def _in_bounds(flight, utc):
    _departure = flight.get("departure")
    _arrival = flight.get("arrival")
    _length = get_route_length(flight["route"])
    if _departure is not None and _arrival is not None:
        return _departure < utc < _arrival
    elif _departure is not None:
        return _departure + estimate_max_flight_duration(_length) > utc
    elif _arrival is not None:
        return _arrival - estimate_max_flight_duration(_length) < utc


class FlightDataSource:
    def __init__(self, source: str, category: str = "airports"):
        self.source = source
        self.myclient = pymongo.MongoClient("mongodb://localhost:27017/")
        self.mydb = self.myclient[category]
        self.mycol = self.mydb[self.source.lower()]

    def update_data(self):
        pass

    def get_active_flights(self, utc=None):
        if utc is None:
            utc = int(arrow.utcnow().timestamp())
        flights = [
            _flight
            for _flight in self.mycol.find(
                {
                    "$or": [
                        {
                            "departure": {
                                "$gt": utc - 24 * 3600,
                                "$lt": utc + 300,
                            }
                        },
                        {"arrival": {"$gt": utc - 300, "$lt": utc + 24 * 3600}},
                    ]
                }
            )
            if _in_bounds(_flight, utc)
        ]
        return flights

    def get_supported_airlines(self):
        return self.mycol.distinct("airline_icao")

    def update_flight(self, flight):
        self.mycol.update_one(
            {"_id": flight["_id"]}, {"$set": flight}, upsert=True
        )
