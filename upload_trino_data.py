#!venv/bin/python
import argparse
import json
import logging
import pathlib
import arrow
import pandas as pd
from pyopensky.trino import Trino
from upload_utils import set_routes_bulk, _build_route_data
from vrs_standing_data import get_airline_routes

PWD = pathlib.Path(__file__).resolve().parent

logger = logging.getLogger(__name__)


def _bulk_set_routes(
    routes: list[dict],
) -> tuple[int, int, int, int]:
    """Call set_routes_bulk and return (new, updated, unchanged, rejected).

    set_routes_bulk returns (stored, rejected) where stored = new + updated.
    For the full breakdown we call the API directly here.
    """
    import os
    import requests

    _api_url = os.getenv("ROUTES_API_URL")
    _api_key = os.getenv("ROUTES_API_KEY", "")
    if not _api_url:
        return 0, 0, 0, len(routes)
    try:
        _response = requests.post(
            f"{_api_url}/api/set_routes",
            json=routes,
            headers={"api_key": _api_key},
            timeout=30,
        )
        _response.raise_for_status()
        _r = _response.json()
        return (
            _r["stored_new"],
            _r["stored_updated"],
            _r["stored_unchanged"],
            _r["rejected"],
        )
    except requests.RequestException:
        logger.exception("Bulk API error")
        return 0, 0, 0, len(routes)


def _top_airlines_from_recurring(
    json_path: pathlib.Path, n: int = 100
) -> list[str]:
    """Return the top-n operator ICAO codes by recurring callsign count,
    derived from recurring_callsigns.json."""
    with open(json_path, encoding="utf-8") as _f:
        _data = json.load(_f)
    return (
        pd.Series(_data["recurring_callsigns"])
        .str[:3]
        .value_counts()
        .head(n)
        .index.tolist()
    )


def filter_callsigns(flightlist: pd.DataFrame) -> pd.DataFrame:
    flightlist.loc[:, "callsign"] = flightlist["callsign"].str.rstrip()
    _components = flightlist["callsign"].str.extract(
        r"^(?P<operator>[A-Z]{3})0*(?P<suffix>[1-9][A-Z0-9]*)$",
        expand=True,
    )
    flightlist.loc[:, "callsign"] = (
        _components["operator"] + _components["suffix"]
    )
    flightlist.dropna(subset=["callsign"], inplace=True)
    flightlist = flightlist[
        flightlist["callsign"].str.contains(
            r"^(?:[A-Z]{3})[1-9](?:(?:[0-9]{0,3})|(?:[0-9]{0,2})"
            r"(?:[A-Z])|(?:[0-9]?)(?:[A-Z]{2}))$",
            regex=True,
            na=False,
        )
    ]
    return flightlist


def filter_airports(
    flightlist: pd.DataFrame, valid_airports: set[str]
) -> pd.DataFrame:
    _icao_pattern = r"^[A-Z]{2}[0-9A-Z]{2}$"
    flightlist = flightlist.dropna(subset=["departure", "arrival"])
    if flightlist.empty:
        return flightlist
    flightlist = flightlist[
        flightlist["departure"].isin(valid_airports)
        & flightlist["arrival"].isin(valid_airports)
        & (flightlist["departure"] != flightlist["arrival"])
    ]
    return flightlist


def split_route(route: str) -> list[str]:
    _airports = route.split("-")
    return [
        f"{_airports[i]}-{_airports[i + 1]}" for i in range(len(_airports) - 1)
    ]


def split_routes(routes: list[str]) -> list[str]:
    return list(
        {_simple for _route in routes for _simple in split_route(_route)}
    )


def process_airline(
    trino: Trino, operator: str, start_date: str, end_date: str
) -> pd.DataFrame:
    flightlist = trino.flightlist(
        start_date, end_date, callsign=f"{operator}_%"
    )
    if flightlist is None or flightlist.empty:
        return pd.DataFrame(), 0, 0, 0, 0, 0

    for _col in ("departure", "arrival", "callsign"):
        flightlist[_col] = flightlist[_col].astype(object)

    flightlist = filter_callsigns(flightlist)

    _vrs_routes = split_routes(get_airline_routes(operator))
    if not _vrs_routes:
        return pd.DataFrame(), 0, 0, 0, 0, 0

    _airport_set = {
        _airport for _route in _vrs_routes for _airport in _route.split("-")
    }
    flightlist = filter_airports(flightlist, _airport_set)

    flightlist["route"] = flightlist["departure"] + "-" + flightlist["arrival"]
    flightlist = flightlist[["callsign", "route"]].drop_duplicates()

    # Keep only callsigns that were consistently observed on a single route.
    _route_counts = flightlist.groupby("callsign")["route"].nunique()
    _consistent = _route_counts[_route_counts == 1].index
    flightlist = flightlist[flightlist["callsign"].isin(_consistent)]

    flightlist = flightlist[flightlist["route"].isin(_vrs_routes)]

    _qualified = len(flightlist)

    # Build validated route dicts locally; IATA conversion happens here.
    _routes = []
    for _index, _row in flightlist.iterrows():
        _data = _build_route_data(
            _row["callsign"], _row["route"], plausible=False
        )
        if _data is not None:
            _routes.append(_data)

    _new, _updated, _unchanged, _rejected = (0, 0, 0, 0)
    if _routes:
        # set_routes_bulk returns (stored, rejected); stored = new + updated
        # For the detailed breakdown we need the bulk endpoint response.
        _new, _updated, _unchanged, _rejected = _bulk_set_routes(_routes)

    return flightlist, _qualified, _new, _updated, _unchanged, _rejected


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Upload callsign-route mappings derived from OpenSky Trino data, "
            "validated against VRS standing routes."
        )
    )
    parser.add_argument(
        "--days",
        type=int,
        default=14,
        help="Number of days to look back from two days ago (default: 14).",
    )
    parser.add_argument(
        "--airline",
        nargs="+",
        metavar="ICAO",
        default=None,
        help="Process only these operator ICAO codes (default: top --top).",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=100,
        help=(
            "Number of top airlines to derive from recurring_callsigns.json "
            "when --airline is not specified (default: 100)."
        ),
    )
    args = parser.parse_args()

    _end = arrow.utcnow().shift(days=-2)
    _begin = _end.shift(days=-args.days)
    _start_date = _begin.format("YYYY-MM-DD")
    _end_date = _end.format("YYYY-MM-DD")

    if args.airline:
        _airlines = args.airline
    else:
        _json = PWD / "recurring_callsigns.json"
        _airlines = _top_airlines_from_recurring(_json, n=args.top)

    _total_qualified = 0
    _total_new = 0
    _total_updated = 0
    _total_unchanged = 0
    _total_rejected = 0
    trino = Trino()
    for _airline in _airlines:
        try:
            (
                _,
                _qualified,
                _new,
                _updated,
                _unchanged,
                _rejected,
            ) = process_airline(trino, _airline, _start_date, _end_date)
            _total_qualified += _qualified
            _total_new += _new
            _total_updated += _updated
            _total_unchanged += _unchanged
            _total_rejected += _rejected
        except Exception:
            logger.exception(f"{_airline}: unhandled error, continuing")
    logger.warning(
        f"Done: {len(_airlines)} airlines — "
        f"{_total_qualified} qualified, "
        f"{_total_new} new, {_total_updated} updated, "
        f"{_total_unchanged} unchanged, {_total_rejected} rejected "
        f"({_start_date} to {_end_date})"
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    main()
