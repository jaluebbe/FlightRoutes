import logging
import arrow
import pymongo
from ham_data import Airport
from route_utils import get_route_length

logging.basicConfig(level=logging.INFO)

airport = Airport()

# Uncomment to refresh MongoDB from the HAM live feed before inspecting:
# airport.update_data()

utc = arrow.utcnow()
print(f"{utc.format('YYYY-MM-DD HH:mm')} UTC")
for _flight in airport.get_active_flights(utc.timestamp()):
    print(
        "{} {:>4} {} {:.3f}km arr={} dep={} status={} {}".format(
            _flight["airline_iata"],
            _flight["flight_number"],
            _flight["route"],
            get_route_length(_flight["route"]) * 1e-3,
            _flight.get("arrival"),
            _flight.get("departure"),
            _flight.get("status"),
            _flight.get("airline_name", ""),
        )
    )

# ---------------------------------------------------------------------------
# MongoDB status inspection
# ---------------------------------------------------------------------------

mycol = pymongo.MongoClient("mongodb://localhost:27017/")["airports"]["ham"]

status_entries = {
    _row["status"]
    for _row in mycol.find({"status": {"$exists": True}}, {"status": True})
    if _row.get("status") is not None
}
print("Status values in DB:", status_entries)

cancelled = mycol.count_documents({"cancelled": True})
diverted = mycol.count_documents({"diverted": True})
overlap = mycol.count_documents({"overlap": True})
print(
    f"Cancelled: {cancelled}  Diverted: {diverted}  Overlapping flight numbers: {overlap}"
)
