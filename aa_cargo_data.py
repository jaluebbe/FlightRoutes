#!/usr/bin/env python3
# Download the monthly Confirmed FS schedule CSV manually from:
# https://www.aacargo.com/ship/schedules.html
# and place it in the same directory as this script.
# Expected file name: ConfirmedFS<Mon><YYYY>.csv (e.g. ConfirmedFSMay2026.csv).
import csv
import io
import logging
import pathlib
import arrow
from airport_info import get_airport_info, get_airport_icao
import flight_data_source

PWD = pathlib.Path(__file__).resolve().parent

# File name pattern for the Confirmed FS schedule.
# Confirmed covers all AA flights including regional partners (CRJ, ERJ),
# and is a superset of the Expedite schedule.
_FILE_PATTERN = "ConfirmedFS{month}{year}.csv"

# Number of header rows before data begins:
# 1 blank + 1 title + 1 validity + 1 blank + 2 column headers + 1 DOW key
# + 1 column alias = 8 rows.
_HEADER_ROWS = 8

# Disclaimer line marks end of data.
_DISCLAIMER_PREFIX = "Data shown in this schedule"

_AA_ICAO = "AAL"
_AA_IATA = "AA"

logger = logging.getLogger(pathlib.Path(__file__).name)


def _parse_dow(dow_str: str) -> set[int]:
    """Return a set of ISO weekday numbers from a DOW string.

    AA uses digits 1-7 with dots for non-operating days, e.g. '1234567'
    means daily, '.....6.' means Saturday only. 'DAILY' is also used.
    """
    _s = dow_str.strip()
    if _s.upper() == "DAILY":
        return set(range(1, 8))
    return {int(_d) for _d in _s if _d.isdigit()}


def _parse_date(date_str: str) -> arrow.Arrow | None:
    """Parse M/D/YY date format used in AA CSV files (e.g. '4/1/26')."""
    _s = date_str.strip()
    if not _s:
        return None
    try:
        return arrow.get(_s, "M/D/YY")
    except Exception:
        return None


def _read_csv_file(path: pathlib.Path) -> list[dict]:
    """Read a locally saved AA Cargo CSV and return parsed data rows."""
    with open(path, encoding="utf-8-sig") as _f:
        _lines = _f.read().splitlines()

    _data_lines = []
    for _line in _lines[_HEADER_ROWS:]:
        if _line.startswith(_DISCLAIMER_PREFIX):
            break
        _data_lines.append(_line)

    _reader = csv.reader(io.StringIO("\n".join(_data_lines)))
    _rows = []
    for _row in _reader:
        if len(_row) < 9 or not _row[0].strip():
            continue
        if len(_row[0].strip()) != 3:
            continue
        _rows.append(
            {
                "origin": _row[0].strip(),
                "dest": _row[1].strip(),
                "flight_number": _row[2].strip(),
                "dow": _row[3].strip(),
                "departs": _row[4].strip(),
                "arrives": _row[5].strip(),
                "eff_date": _row[8].strip(),
                "dis_date": _row[9].strip() if len(_row) > 9 else "",
            }
        )
    return _rows


def _process_row(
    row: dict,
    utc: arrow.Arrow,
    unknown_airports: set[str],
) -> dict | None:
    if not row["flight_number"].isdigit():
        return None
    flight_number = int(row["flight_number"])

    _eff = _parse_date(row["eff_date"])
    _dis = _parse_date(row["dis_date"])
    if _eff is None:
        return None
    if _dis is None:
        _dis = _eff.shift(months=1)
    if not _eff <= utc <= _dis.ceil("day"):
        return None

    _days = _parse_dow(row["dow"])
    if utc.isoweekday() not in _days:
        return None

    origin_icao = get_airport_icao(row["origin"])
    dest_icao = get_airport_icao(row["dest"])
    if origin_icao is None:
        unknown_airports.add(row["origin"])
        return None
    if dest_icao is None:
        unknown_airports.add(row["dest"])
        return None

    origin_info = get_airport_info(origin_icao)
    dest_info = get_airport_info(dest_icao)
    if origin_info is None or dest_info is None:
        return None

    _date = utc.format("YYYY-MM-DD")
    _departure = arrow.get(
        f"{_date}T{row['departs']}", tzinfo=origin_info["Timezone"]
    )
    _arrival = arrow.get(
        f"{_date}T{row['arrives']}", tzinfo=dest_info["Timezone"]
    )
    if _arrival < _departure:
        _arrival = _arrival.shift(days=1)

    _route = f"{origin_icao}-{dest_icao}"
    return {
        "_id": f"{_AA_IATA}_{flight_number}_{_route}_{utc.format('YYYYMMDD')}",
        "airline_iata": _AA_IATA,
        "airline_icao": _AA_ICAO,
        "flight_number": flight_number,
        "route": _route,
        "departure": int(_departure.timestamp()),
        "arrival": int(_arrival.timestamp()),
        "segment_number": 0,
    }


class Airline(flight_data_source.FlightDataSource):
    def __init__(self):
        super().__init__("AACargo", category="airlines")

    def update_data(self, utc=None) -> None:
        if utc is None:
            _utc = arrow.utcnow()
        else:
            _utc = arrow.get(utc)

        _path = PWD / _FILE_PATTERN.format(
            month=_utc.format("MMM"), year=_utc.format("YYYY")
        )
        if not _path.exists():
            logger.warning(
                f"Schedule file not found: {_path.name} — download from "
                f"https://www.aacargo.com/ship/schedules.html"
            )
            return

        _rows = _read_csv_file(_path)
        logger.debug(f"Read {len(_rows)} rows from {_path.name}")

        _unknown_airports: set[str] = set()
        _flights = []
        for _row in _rows:
            _flight = _process_row(_row, _utc, _unknown_airports)
            if _flight is not None:
                _flights.append(_flight)

        for _iata in sorted(_unknown_airports):
            logger.warning(f"Unknown airport IATA: {_iata}")

        _stored = 0
        for _flight in _flights:
            self.update_flight(_flight)
            _stored += 1

        logger.info(
            f"AA Cargo: stored {_stored} flights for "
            f"{_utc.format('YYYY-MM-DD')}."
        )


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(
        description="Import American Airlines Cargo schedule into MongoDB."
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
