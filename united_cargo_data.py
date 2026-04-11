#!/usr/bin/env python3
import datetime
import logging
import pathlib
import arrow
import pandas as pd
from airport_info import get_airport_info, get_airport_icao
import flight_data_source

PWD = pathlib.Path(__file__).resolve().parent

# Schedule XLSX files are downloaded manually from:
# https://www.unitedcargo.com/en/us/shipping-tools/schedules.html
# Expected file name pattern (United's convention):
# United_Cargo_Flight_Schedule-Full_Network-<Month><YYYY>.xlsx
# e.g. United_Cargo_Flight_Schedule-Full_Network-April2026.xlsx
_FILE_PATTERN = "United_Cargo_Flight_Schedule-Full_Network-{month}{year}.xlsx"

# All times are local. DOW digits: 1=Mon, 2=Tue, 3=Wed, 4=Thu, 5=Fri, 6=Sat, 7=Sun.
# JV sheet contains flights operated by partner carriers under their own codes
# (LH, OS, SN, LX, 4Y) — these are already covered by other data sources.
_UA_ICAO = "UAL"
_UA_IATA = "UA"

# Sheets that carry United-operated flights and their header row (0-based).
# Widebody uses integer times (2220 = 22:20) and has Eff/Dis date columns.
# Narrowbody and UAX use datetime.time objects and cover the whole month.
_SHEETS = {
    "Widebody": {"header": None, "has_validity": True, "time_format": "int"},
    "Narrowbody": {"header": 7, "has_validity": False, "time_format": "time"},
    "UAX": {"header": 7, "has_validity": False, "time_format": "time"},
}

logger = logging.getLogger(pathlib.Path(__file__).name)


def _parse_int_time(value: int, date_str: str, timezone: str) -> arrow.Arrow:
    """Parse a HHMM integer (e.g. 2220) into an Arrow UTC datetime."""
    _hhmm = str(int(value)).zfill(4)
    return arrow.get(f"{date_str}T{_hhmm[:2]}:{_hhmm[2:]}", tzinfo=timezone)


def _parse_time_obj(
    value: datetime.time, date_str: str, timezone: str
) -> arrow.Arrow:
    """Parse a datetime.time object into an Arrow UTC datetime."""
    return arrow.get(
        f"{date_str}T{value.hour:02d}:{value.minute:02d}", tzinfo=timezone
    )


def _operating_days(dow) -> set[int]:
    """Return a set of ISO weekday numbers from a DOW value.

    DOW is a string or int of digit characters where each digit is an
    ISO weekday number, e.g. '1234567' = every day, 146 = Mon/Thu/Sat.
    """
    return {int(_d) for _d in str(int(dow)) if _d.isdigit()}


def _load_widebody(
    path: pathlib.Path, month_start: arrow.Arrow
) -> pd.DataFrame:
    """Load the Widebody sheet, which has repeated region sub-headers and
    integer-formatted times. Rows where Org is not a 3-letter code are dropped.
    """
    df_raw = pd.read_excel(path, sheet_name="Widebody", header=None)
    _col_names = [
        "Sales region",
        "Org",
        "Des",
        "Flight #",
        "Eff Date",
        "Dis Date",
        "Departs",
        "Arrives",
        "A/C type",
        "DOW",
        "Notes",
    ]
    rows = []
    for _, _row in df_raw.iterrows():
        _vals = _row.tolist()
        # Skip rows that are section headers or metadata (Org is not 3 chars).
        _org = _vals[1] if len(_vals) > 1 else None
        if not isinstance(_org, str) or len(_org) != 3:
            continue
        rows.append(dict(zip(_col_names, _vals)))
    df = pd.DataFrame(rows)
    # Eff/Dis dates are datetime objects — rows without them cover the whole month.
    df["Eff Date"] = df["Eff Date"].fillna(month_start.datetime)
    df["Dis Date"] = df["Dis Date"].fillna(month_start.shift(months=1).datetime)
    return df


def _load_simple_sheet(
    path: pathlib.Path, sheet: str, header: int
) -> pd.DataFrame:
    """Load a sheet with a single header row and no region sub-headers."""
    df = pd.read_excel(path, sheet_name=sheet, header=header)
    # Keep only rows with a valid 3-letter Org code.
    df = df[
        df["Org"].apply(lambda x: isinstance(x, str) and len(x) == 3)
    ].copy()
    return df


def _process_widebody_row(
    row: pd.Series,
    utc: arrow.Arrow,
    unknown_airports: set[str],
) -> dict | None:
    _flight_str = str(row["Flight #"]).strip()
    if not _flight_str.startswith("UA ") or not _flight_str[3:].isdigit():
        return None
    flight_number = int(_flight_str[3:])

    _eff = arrow.get(row["Eff Date"])
    _dis = arrow.get(row["Dis Date"]).ceil("day")
    if not _eff <= utc <= _dis:
        return None

    try:
        _days = _operating_days(row["DOW"])
    except (ValueError, TypeError):
        return None
    if utc.isoweekday() not in _days:
        return None

    origin_iata = str(row["Org"]).strip()
    destination_iata = str(row["Des"]).strip()
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

    _date = utc.format("YYYY-MM-DD")
    _departure = _parse_int_time(row["Departs"], _date, origin_info["Timezone"])
    _arrival = _parse_int_time(
        row["Arrives"], _date, destination_info["Timezone"]
    )
    if _arrival < _departure:
        _arrival = _arrival.shift(days=1)

    _route = f"{origin_icao}-{destination_icao}"
    return {
        "_id": f"{_UA_IATA}_{flight_number}_{_route}_{utc.format('YYYYMMDD')}",
        "airline_iata": _UA_IATA,
        "airline_icao": _UA_ICAO,
        "flight_number": flight_number,
        "route": _route,
        "departure": int(_departure.timestamp()),
        "arrival": int(_arrival.timestamp()),
        "segment_number": 0,
    }


def _process_simple_row(
    row: pd.Series,
    utc: arrow.Arrow,
    month_start: arrow.Arrow,
    unknown_airports: set[str],
) -> dict | None:
    _fn = row["Flight #"]
    if not str(int(_fn)).isdigit() if isinstance(_fn, (int, float)) else True:
        return None
    flight_number = int(_fn)

    try:
        _days = _operating_days(row["DOW"])
    except (ValueError, TypeError):
        return None
    if utc.isoweekday() not in _days:
        return None

    # These sheets cover the whole month — check utc is within month.
    _month_end = month_start.shift(months=1)
    if not month_start <= utc <= _month_end:
        return None

    origin_iata = str(row["Org"]).strip()
    destination_iata = str(row["Des"]).strip()
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

    _date = utc.format("YYYY-MM-DD")
    _departure = _parse_time_obj(row["Departs"], _date, origin_info["Timezone"])
    _arrival = _parse_time_obj(
        row["Arrives"], _date, destination_info["Timezone"]
    )
    if _arrival < _departure:
        _arrival = _arrival.shift(days=1)

    _route = f"{origin_icao}-{destination_icao}"
    return {
        "_id": f"{_UA_IATA}_{flight_number}_{_route}_{utc.format('YYYYMMDD')}",
        "airline_iata": _UA_IATA,
        "airline_icao": _UA_ICAO,
        "flight_number": flight_number,
        "route": _route,
        "departure": int(_departure.timestamp()),
        "arrival": int(_arrival.timestamp()),
        "segment_number": 0,
    }


class Airline(flight_data_source.FlightDataSource):
    def __init__(self):
        super().__init__("UnitedCargo", category="airlines")

    def update_data(self, utc=None) -> None:
        if utc is None:
            _utc = arrow.utcnow()
        else:
            _utc = arrow.get(utc)

        # Derive the first day of the schedule's month from the target date.
        _month_start = _utc.floor("month")
        _schedule_file = PWD / _FILE_PATTERN.format(
            month=_utc.format("MMMM"), year=_utc.format("YYYY")
        )
        if not _schedule_file.exists():
            logger.warning(
                f"Schedule file not found: {_schedule_file.name} — download from "
                f"https://www.unitedcargo.com/en/us/shipping-tools/schedules.html"
            )
            return

        _unknown_airports: set[str] = set()
        _flights = []

        # Widebody sheet.
        _df_wb = _load_widebody(_schedule_file, _month_start)
        for _, _row in _df_wb.iterrows():
            _flight = _process_widebody_row(_row, _utc, _unknown_airports)
            if _flight is not None:
                _flights.append(_flight)

        # Narrowbody and UAX sheets.
        for _sheet, _meta in _SHEETS.items():
            if _meta["has_validity"]:
                continue
            _df = _load_simple_sheet(_schedule_file, _sheet, _meta["header"])
            for _, _row in _df.iterrows():
                _flight = _process_simple_row(
                    _row, _utc, _month_start, _unknown_airports
                )
                if _flight is not None:
                    _flights.append(_flight)

        for _iata in sorted(_unknown_airports):
            logger.warning(f"Unknown airport IATA: {_iata}")

        _stored = 0
        for _flight in _flights:
            self.update_flight(_flight)
            _stored += 1

        logger.info(
            f"United Cargo: stored {_stored} flights for "
            f"{_utc.format('YYYY-MM-DD')}."
        )


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(
        description="Import United Cargo schedule into MongoDB."
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Target date in YYYY-MM-DD format (default: tomorrow UTC).",
    )
    args = parser.parse_args()
    _target = (
        arrow.get(args.date) if args.date else arrow.utcnow().shift(days=1)
    )
    logger.info(f"Targeting date: {_target.format('YYYY-MM-DD')}")
    airline = Airline()
    airline.update_data(_target)
