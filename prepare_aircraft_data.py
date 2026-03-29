#!/usr/bin/env python3
import json
import csv
import logging
import pathlib
import requests

URL = (
    "https://s3.opensky-network.org/data-samples/metadata/"
    "aircraft-database-complete-2025-08.csv"
)
PWD = pathlib.Path(__file__).resolve().parent
OUTPUT_FILE = PWD / "icao24_to_registration.json"
STAGING_FILE = PWD / "icao24_to_registration_new.json"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main() -> None:
    logger.info(f"Downloading aircraft database from {URL}")
    with requests.Session() as _session:
        with _session.get(URL, stream=True) as _response:
            _response.raise_for_status()
            # The S3 archive uses single-quote quoting for all fields
            # including column headers.
            _reader = csv.DictReader(
                _response.iter_lines(decode_unicode="utf-8"),
                delimiter=",",
                quotechar="'",
            )
            # Hyphens are stripped from registrations to match the format
            # used in opensky_utils.py. Only aircraft with a known operator
            # ICAO are included, as the data is used for airline flight
            # matching only.
            icao24_to_registration = {
                _row["icao24"]: "".join(_row["registration"].split("-"))
                for _row in _reader
                if _row["operatorIcao"] != "" and _row["registration"] != ""
            }

    logger.info(f"Loaded {len(icao24_to_registration)} aircraft records.")

    with open(STAGING_FILE, "w", encoding="utf-8") as _f:
        json.dump(icao24_to_registration, _f)

    STAGING_FILE.rename(OUTPUT_FILE)
    logger.info(f"Written to {OUTPUT_FILE}.")


if __name__ == "__main__":
    main()
