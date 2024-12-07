import json
import logging
import pandas as pd
import arrow
from pyopensky.trino import Trino

trino = Trino()

end_date = arrow.utcnow().shift(hours=-1).floor("day")
start_date = end_date.shift(days=-22)

# Some operator ICAOs do not represent scheduled airline flights:
excluded_operators = [
    "DCM",
    "FWR",
    "FFL",
    "XAA",
    "EJA",
    "NJE",
    "NEJ",
    "CXK",
    "EJM",
    "JAS",
    "HRT",
    "JTZ",
    "LJY",
    "LXJ",
    "MVP",
    "PBR",
    "SIS",
    "TFF",
    "TWY",
    "XFL",
    "XSR",
]


def process_flightlist(flightlist: pd.DataFrame) -> pd.DataFrame:
    flightlist["callsign"] = flightlist["callsign"].str.rstrip()
    callsign_components = flightlist["callsign"].str.extract(
        r"^(?P<operator>[A-Z]{3})0*(?P<suffix>[1-9][A-Z0-9]*)$", expand=True
    )
    flightlist["callsign"] = (
        callsign_components["operator"] + callsign_components["suffix"]
    )
    flightlist.dropna(subset=["callsign"], inplace=True)
    flightlist = flightlist[
        flightlist["callsign"].str.contains(
            r"^(?:[A-Z]{3})[1-9](?:(?:[0-9]{0,3})|(?:[0-9]{0,2})"
            r"(?:[A-Z])|(?:[0-9]?)(?:[A-Z]{2}))$",
            regex=True,
            na=False,
        )
    ].copy()
    flightlist["lastseen"] = flightlist["lastseen"].astype(int) // 10 ** 9
    flightlist["firstseen"] = flightlist["firstseen"].astype(int) // 10 ** 9
    return flightlist


_before = arrow.utcnow()

dataframes = []
current_date = start_date
while current_date < end_date:
    _flightlist = trino.flightlist(current_date.format("YYYY-MM-DD"))
    if _flightlist is not None:
        dataframes.append(process_flightlist(_flightlist))
    else:
        logging.warning(f"no flightlist available for {current_date}")
    current_date = current_date.shift(days=1)
callsign_occurences = pd.concat(dataframes, ignore_index=True)

_after = arrow.utcnow()
duration = (_after - _before).total_seconds()
# Exclude callsigns from operators that do not represent airline flights.
callsign_occurences = callsign_occurences[
    ~callsign_occurences.callsign.str[:3].isin(excluded_operators)
]

callsign_occurences = callsign_occurences.groupby(["callsign"]).agg(
    {"lastseen": "max", "firstseen": "min"}
)
# The callsign should have been active on at least two different days.
recurring_callsigns = callsign_occurences[
    callsign_occurences.lastseen - callsign_occurences.firstseen > 86400
]

_json_data = json.dumps(
    {
        "start_date": start_date.timestamp(),
        "end_date": end_date.timestamp(),
        "recurring_callsigns": recurring_callsigns.index.to_list(),
    }
)

with open("recurring_callsigns.json", "w") as f:
    f.write(_json_data + "\n")

print(
    f"Found {len(recurring_callsigns)} recurring callsigns out of "
    f"{len(callsign_occurences)} different callsigns in the time range from "
    f"{start_date.format('YYYY-MM-DD HH:mm:ss')} to "
    f"{end_date.format('YYYY-MM-DD HH:mm:ss')} within {duration:.1f}s."
)
