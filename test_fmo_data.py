import logging
import arrow
import pymongo
from fmo_data import Airport, _status_codes
from route_utils import get_route_length

logging.basicConfig(level=logging.INFO)

airport = Airport()

# Uncomment to refresh MongoDB from the FMO live feed before inspecting:
# airport.update_data()

utc = arrow.utcnow()
print(f"{utc.format('YYYY-MM-DD HH:mm')} UTC")
for _flight in airport.get_active_flights(utc.timestamp()):
    print(
        "{} {:>4} {} {:.3f}km {} {} {}".format(
            _flight["airline_iata"],
            _flight["flight_number"],
            _flight["route"],
            get_route_length(_flight["route"]) * 1e-3,
            _flight.get("arrival"),
            _flight.get("departure"),
            _flight.get("status"),
        )
    )

# ---------------------------------------------------------------------------
# MongoDB status inspection
# ---------------------------------------------------------------------------

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mycol = myclient["airports"]["fmo"]

status_entries = {
    _row["status"]
    for _row in mycol.find({"status": {"$exists": True}}, {"status": True})
}
print("Raw status values in DB:", status_entries)

unknown_status_codes = {
    _row["status"]
    for _row in mycol.find({"status": {"$exists": True}})
    if _row["status"] not in _status_codes.values()
}
if unknown_status_codes:
    print("Unknown status codes (not yet mapped):", unknown_status_codes)
