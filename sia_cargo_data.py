#!/usr/bin/env python3
import logging
import pathlib
import arrow
import pandas as pd
from airport_info import get_airport_info, get_airport_icao
import flight_data_source

PWD = pathlib.Path(__file__).resolve().parent

# Default schedule file name — replace with the current timetable XLSX.
SCHEDULE_FILE = PWD / "SIACargoTimetable.xlsx"

# Header row is row index 4 (0-based) in the Excel file.
_HEADER_ROW = 4

# All times in the schedule are local — the airport database provides
# the IANA timezone for each ICAO code to convert to UTC.
# Day columns are Monday-first, Sunday last.
_DAY_COLUMNS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

# Carrier ICAO and IATA codes for the operators in this schedule.
# SQ = Singapore Airlines Cargo, TR = Scoot.
_CARRIER_ICAO = {
    "SQ": "SIA",
    "TR": "TGW",
}
_CARRIER_IATA = {
    "SQ": "SQ",
    "TR": "TR",
}

logger = logging.getLogger(pathlib.Path(__file__).name)


def _parse_time(hhmm_str: str, date_str: str, timezone: str) -> arrow.Arrow:
    """Parse a HH:MM time string into an Arrow UTC datetime.

    The schedule uses an optional +N suffix to indicate day offsets, e.g.
    '08:00+1' means the next day, '14:00+3' means three days later.
    """
    _hhmm = str(hhmm_str).strip()
    _day_offset = 0
    if "+" in _hhmm:
        _hhmm, _offset = _hhmm.split("+", 1)
        _day_offset = int(_offset)
    _hh, _mm = _hhmm.split(":")
    return arrow.get(f"{date_str}T{_hh}:{_mm}", tzinfo=timezone).shift(
        days=_day_offset
    )


def _operating_days(row: pd.Series) -> set[int]:
    """Return a set of ISO weekday numbers (1=Mon … 7=Sun) on which this
    row operates, based on the checkmark columns."""
    _col_to_iso = {
        "Mon": 1,
        "Tue": 2,
        "Wed": 3,
        "Thu": 4,
        "Fri": 5,
        "Sat": 6,
        "Sun": 7,
    }
    return {_col_to_iso[_col] for _col in _DAY_COLUMNS if row[_col] == "✓"}


def _load_schedule(path: pathlib.Path) -> pd.DataFrame:
    df = pd.read_excel(path, header=_HEADER_ROW)
    # Trucking rows are road feeder services, not actual flights.
    df = df[df["Aircraft Classification"] != "Trucking"].copy()
    df = df[df["Carrier Code"].notna()].copy()
    return df


def _process_row(
    row: pd.Series,
    utc: arrow.Arrow,
    unknown_airports: set[str],
) -> dict | None:
    carrier_code = row["Carrier Code"]
    airline_icao = _CARRIER_ICAO.get(carrier_code)
    airline_iata = _CARRIER_IATA.get(carrier_code)
    if airline_icao is None:
        logger.warning(f"Unknown carrier code: {carrier_code}")
        return None

    if not str(row["Flight No"]).isdigit():
        return None
    flight_number = int(row["Flight No"])

    origin_iata = row["Origin"]
    destination_iata = row["Destination"]
    origin_icao = get_airport_icao(origin_iata)
    destination_icao = get_airport_icao(destination_iata)

    if origin_icao is None:
        unknown_airports.add(origin_iata)
        return None
    if destination_icao is None:
        unknown_airports.add(destination_iata)
        return None

    origin_info = get_airport_info(origin_icao)
    destination_info = get_airport_info(destination_icao)
    if origin_info is None or destination_info is None:
        return None

    # Validity dates are strings in DD-MMM-YYYY format.
    _validity_from = arrow.get(row["Validity From"], "DD-MMM-YYYY")
    _validity_to = arrow.get(row["Validity To"], "DD-MMM-YYYY").ceil("day")
    if not _validity_from <= utc <= _validity_to:
        return None

    if utc.isoweekday() not in _operating_days(row):
        return None

    _date = utc.format("YYYY-MM-DD")
    _departure = _parse_time(
        str(row["Dep. Time"]), _date, origin_info["Timezone"]
    )
    _arrival = _parse_time(
        str(row["Arr. Time"]), _date, destination_info["Timezone"]
    )

    _route = f"{origin_icao}-{destination_icao}"
    return {
        "_id": f"{airline_iata}_{flight_number}_{_route}_{utc.format('YYYYMMDD')}",
        "airline_iata": airline_iata,
        "airline_icao": airline_icao,
        "flight_number": flight_number,
        "route": _route,
        "departure": int(_departure.timestamp()),
        "arrival": int(_arrival.timestamp()),
        "segment_number": 0,
    }


class Airline(flight_data_source.FlightDataSource):
    def __init__(self, schedule_file: pathlib.Path = SCHEDULE_FILE):
        super().__init__("SIACargo", category="airlines")
        self._schedule_file = schedule_file

    def update_data(self, utc=None) -> None:
        if utc is None:
            _utc = arrow.utcnow()
        else:
            _utc = arrow.get(utc)

        _df = _load_schedule(self._schedule_file)
        _unknown_airports: set[str] = set()
        _flights = []

        for _, _row in _df.iterrows():
            _flight = _process_row(_row, _utc, _unknown_airports)
            if _flight is None:
                continue
            _flights.append(_flight)

        for _iata in sorted(_unknown_airports):
            logger.warning(f"Unknown airport IATA: {_iata}")

        _stored = 0
        for _flight in _flights:
            self.update_flight(_flight)
            _stored += 1

        logger.info(
            f"SIA Cargo: stored {_stored} flights for "
            f"{_utc.format('YYYY-MM-DD')}."
        )


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(
        description="Import SIA Cargo schedule into MongoDB."
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Target date in YYYY-MM-DD format (default: tomorrow UTC).",
    )
    parser.add_argument(
        "--file",
        default=str(SCHEDULE_FILE),
        help="Path to the schedule XLSX file.",
    )
    args = parser.parse_args()
    _target = (
        arrow.get(args.date) if args.date else arrow.utcnow().shift(days=1)
    )
    logger.info(f"Targeting date: {_target.format('YYYY-MM-DD')}")
    airline = Airline(schedule_file=pathlib.Path(args.file))
    airline.update_data(_target)
