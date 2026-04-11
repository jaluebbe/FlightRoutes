#!venv/bin/python
import json
import logging
import pathlib
import re
import pandas as pd
from sqlalchemy import select, func, text
from pyopensky.schema import StateVectorsData4
from pyopensky.trino import Trino

PWD = pathlib.Path(__file__).resolve().parent
OUTPUT_FILE = PWD / "recurring_callsigns.json"
STAGING_FILE = PWD / "recurring_callsigns_new.json"

# Maximum barometric altitude accepted for state vectors, in metres.
# Matches the Concorde ceiling used in opensky_utils.py (60,000 ft).
_MAX_BARO_ALTITUDE = 18288

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

raw_callsign_pattern = re.compile(
    r"^(?P<operator>[A-Z]{3})0*(?P<suffix>[1-9][A-Z0-9]*)$"
)
callsign_pattern = re.compile(
    r"^(?:[A-Z]{3})[1-9](?:(?:[0-9]{0,3})|(?:[0-9]{0,2})"
    "(?:[A-Z])|(?:[0-9]?)(?:[A-Z]{2}))$"
)

# Operator ICAO codes excluded because they represent private aviation,
# fractional ownership, or air-taxi operations rather than scheduled airline
# flights. Including them would produce large volumes of noise in the
# review_flight_data_sources workflow.
_EXCLUDED_OPERATORS = {
    "DCM",  # private
    "EJA",  # NetJets (US)
    "EJM",  # Executive Jet Management
    "EUW",  # Eurowings (private charter arm)
    "FFL",  # Flexjet
    "FWR",  # Flexjet
    "HRT",  # Air Hamburg
    "JAS",  # JetSuite
    "JTZ",  # Jetlux / private
    "LJY",  # Learjet / private
    "LXJ",  # Flexjet
    "MVP",  # MVP Airlines (air taxi)
    "NEJ",  # NetJets Europe (variant code)
    "NJE",  # NetJets Europe
    "PBR",  # private
    "SIS",  # private
    "TFF",  # private
    "TWY",  # Taxiway / ground ops
    "XAA",  # private
    "XFL",  # Flexjet
    "XSR",  # private
    "CXK",  # private
}


def recombine_callsign_components(callsign: str) -> str | None:
    _raw_match = raw_callsign_pattern.match(callsign)
    if not _raw_match:
        return None
    _combined = _raw_match.group("operator") + _raw_match.group("suffix")
    if not callsign_pattern.match(_combined):
        return None
    return _combined


def fetch_data(
    trino_connection: Trino,
    start_hour: pd.Timestamp,
    stop_hour: pd.Timestamp,
) -> pd.DataFrame:
    query = (
        select(
            StateVectorsData4.callsign,
            func.min(StateVectorsData4.time).label("first_seen"),
            func.max(StateVectorsData4.time).label("last_seen"),
        )
        .where(
            StateVectorsData4.hour >= start_hour,
            StateVectorsData4.hour < stop_hour,
            StateVectorsData4.callsign.isnot(None),
            StateVectorsData4.onground == False,
            StateVectorsData4.time.isnot(None),
            StateVectorsData4.icao24.isnot(None),
            StateVectorsData4.lat.isnot(None),
            StateVectorsData4.lon.isnot(None),
            StateVectorsData4.velocity.isnot(None),
            StateVectorsData4.heading.isnot(None),
            StateVectorsData4.vertrate.isnot(None),
            StateVectorsData4.baroaltitude.isnot(None),
            StateVectorsData4.lastposupdate.isnot(None),
            StateVectorsData4.baroaltitude <= _MAX_BARO_ALTITUDE,
            text(
                "REGEXP_LIKE(callsign, "
                "'^[A-Z][A-Z][A-Z][0-9][0-9]?[0-9A-Z]?[0-9A-Z]?')"
            ),
        )
        .group_by(StateVectorsData4.callsign)
    )
    return trino_connection.query(query)


def main() -> None:
    stop_ts = pd.Timestamp.now("UTC").floor("D") - pd.Timedelta(hours=1)
    start_ts = stop_ts - pd.Timedelta(days=22)
    stop_hour = stop_ts.floor("1h")
    start_hour = start_ts.floor("1h")

    logger.info(
        f"Querying Trino for callsigns from "
        f"{start_hour.strftime('%Y-%m-%d %H:%M')} to "
        f"{stop_hour.strftime('%Y-%m-%d %H:%M')} UTC."
    )

    _before = pd.Timestamp.now("UTC")
    trino = Trino()
    callsign_occurrences = fetch_data(trino, start_hour, stop_hour)
    duration = (pd.Timestamp.now("UTC") - _before).total_seconds()

    logger.info(f"Trino query completed in {duration:.1f}s.")

    callsign_occurrences.loc[:, "callsign"] = callsign_occurrences[
        "callsign"
    ].str.rstrip()
    callsign_occurrences["callsign"] = callsign_occurrences["callsign"].apply(
        recombine_callsign_components
    )
    callsign_occurrences.dropna(subset=["callsign"], inplace=True)

    # Exclude callsigns whose operator prefix belongs to private aviation,
    # fractional ownership, or air-taxi operations — not scheduled airline
    # flights. These appear in high volume and would otherwise produce noise.
    callsign_occurrences = callsign_occurrences[
        ~callsign_occurrences.callsign.str[:3].isin(_EXCLUDED_OPERATORS)
    ]

    # A callsign is considered recurring if it was active on at least two
    # distinct days within the query window.
    recurring_callsigns = callsign_occurrences[
        callsign_occurrences.last_seen - callsign_occurrences.first_seen
        > pd.Timedelta(days=1)
    ]

    _data = {
        "start_date": start_ts.timestamp(),
        "end_date": stop_ts.timestamp(),
        "recurring_callsigns": recurring_callsigns.callsign.to_list(),
    }

    with open(STAGING_FILE, "w", encoding="utf-8") as _f:
        json.dump(_data, _f)
        _f.write("\n")

    STAGING_FILE.rename(OUTPUT_FILE)

    logger.info(
        f"Found {len(recurring_callsigns)} recurring callsigns out of "
        f"{len(callsign_occurrences)} after filtering, written to {OUTPUT_FILE}."
    )


if __name__ == "__main__":
    main()
