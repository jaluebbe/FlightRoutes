import itertools
import logging
import os

import pandas as pd
import requests

from opensky_utils import validated_callsign
from route_utils import check_route_airports, convert_to_iata_route
from vrs_standing_data import get_airline_routes

logger = logging.getLogger(__name__)

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

_API_URL = os.getenv("ROUTES_API_URL")
_API_KEY = os.getenv("ROUTES_API_KEY", "")

if not _API_URL:
    raise RuntimeError(
        "ROUTES_API_URL is not set. "
        "Add it to your .env file: ROUTES_API_URL=https://my-api.example.com"
    )

_HEADERS = {"api_key": _API_KEY}


def _build_route_data(
    callsign: str, route: str, plausible: bool
) -> dict | None:
    """Validate callsign and route, build the route data dict.

    Returns None if validation fails.
    """
    callsign_info = validated_callsign(callsign.upper())
    if callsign_info is None:
        logger.debug(f"{callsign} did not pass callsign check.")
        return None
    callsign = callsign_info["callsign"]
    route = route.upper()
    if check_route_airports(route, None) is None:
        logger.debug(f"{callsign}: {route} did not pass route check.")
        return None
    _iata_route = convert_to_iata_route(route)
    return {
        "_airport_codes_iata": _iata_route,
        "airport_codes": route,
        "callsign": callsign,
        "plausible": int(plausible),
    }


def set_route(
    callsign: str,
    route: str,
    plausible: bool = False,
    check_airports: bool = True,
) -> dict | None:
    """Set a single callsign-route mapping via the API.

    Validates the callsign and route locally before writing.
    Returns the route data dict if stored, None if rejected or invalid.
    """
    if check_airports:
        callsign_info = validated_callsign(callsign.upper())
        if callsign_info is None:
            logger.debug(f"{callsign} did not pass callsign check.")
            return None
        operator = callsign_info["operator_icao"]
        route_upper = route.upper()
        airline_routes = get_airline_routes(operator)
        airline_airports = list(
            {
                airport
                for _route in airline_routes
                for airport in _route.split("-")
            }
        )
        if check_route_airports(route_upper, airline_airports) is None:
            logger.debug(f"{callsign}: {route} did not pass route check.")
            return None

    _data = _build_route_data(callsign, route, plausible)
    if _data is None:
        return None

    try:
        _response = requests.post(
            f"{_API_URL}/api/set_route",
            json=_data,
            headers=_HEADERS,
            timeout=10,
        )
        _response.raise_for_status()
        _result = _response.json()
        _status = _result.get("status")
        return _data if _status in ("stored_new", "stored_updated") else None
    except requests.RequestException:
        logger.exception(f"API error for {callsign}")
        return None


def set_routes_bulk(routes: list[dict]) -> tuple[int, int, int, int]:
    """Write a list of pre-validated route dicts via the bulk API endpoint.

    Each dict must have keys: _airport_codes_iata, airport_codes,
    callsign, plausible.
    Returns (stored_new, stored_updated, stored_unchanged, rejected) counts.
    """
    if not routes:
        return 0, 0, 0, 0
    try:
        _response = requests.post(
            f"{_API_URL}/api/set_routes",
            json=routes,
            headers=_HEADERS,
            timeout=30,
        )
        _response.raise_for_status()
        _result = _response.json()
        return (
            _result["stored_new"],
            _result["stored_updated"],
            _result["stored_unchanged"],
            _result["rejected"],
        )
    except requests.RequestException:
        logger.exception("Bulk API error")
        return 0, 0, 0, len(routes)


def get_known_callsigns() -> list[str]:
    """Return all callsigns known to the database."""
    try:
        _response = requests.get(
            f"{_API_URL}/api/all_callsigns",
            headers=_HEADERS,
            timeout=30,
        )
        _response.raise_for_status()
        return _response.json()
    except requests.RequestException:
        logger.exception("API error fetching all callsigns")
        return []


def get_route(callsign: str) -> dict | None:
    """Return the known route data for a given callsign. May return None."""
    try:
        _response = requests.get(
            f"{_API_URL}/api/route/{callsign}",
            headers=_HEADERS,
            timeout=10,
        )
        _response.raise_for_status()
        _data = _response.json()
        if _data.get("airport_codes") == "unknown":
            return None
        return _data
    except requests.RequestException:
        logger.exception(f"API error fetching route for {callsign}")
        return None


def routes_vary(grouped_routes: pd.DataFrame) -> bool:
    """Compares all distinct routes and returns True if any origin or
    destination appears more than once."""
    origin_counts = grouped_routes["origin"].value_counts()
    destination_counts = grouped_routes["destination"].value_counts()
    return any(origin_counts > 1) or any(destination_counts > 1)


def check_route_change(grouped_routes: pd.DataFrame) -> pd.DataFrame:
    """Routes for a given callsign may vary over time. The most recent
    route is returned if it appeared at least two times with no overlap
    with older routes."""
    grouped_routes_sorted = grouped_routes.sort_values(by="first_departure")
    last_item = grouped_routes_sorted.iloc[-1]
    other_items = grouped_routes_sorted.iloc[:-1]
    if last_item["count"] > 1 and all(
        last_item["first_departure"] > other_items["last_arrival"]
    ):
        return grouped_routes_sorted.iloc[[-1]]
    return grouped_routes_sorted.iloc[0:0]


def analyse_route_times(df_callsign: pd.DataFrame) -> pd.DataFrame:
    """Analyse routes belonging to the same callsign ordered by departure
    time. Routes are combined if their destination/origin match and the
    stopover time is not too long."""
    df_callsign["next_origin"] = df_callsign["origin"].shift(-1)
    df_callsign["next_destination"] = df_callsign["destination"].shift(-1)
    df_callsign["next_departure"] = df_callsign["departure"].shift(-1)
    df_callsign["next_arrival"] = df_callsign["arrival"].shift(-1)
    df_callsign["stopover_time"] = (
        df_callsign["next_departure"] - df_callsign["arrival"]
    )
    df_callsign = df_callsign[
        df_callsign["next_origin"] == df_callsign["destination"]
    ]
    df_callsign = df_callsign[df_callsign["stopover_time"] < 14400]
    df_callsign["route"] = (
        df_callsign["origin"]
        .str.cat(df_callsign["destination"], sep="-")
        .str.cat(df_callsign["next_destination"], sep="-")
    )
    df_routes = (
        df_callsign.iloc[:-1]
        .groupby(["callsign", "route"])
        .agg(
            count=("callsign", "size"),
            min_stopover_time=("stopover_time", "min"),
            first_departure=("departure", "min"),
            last_arrival=("next_arrival", "max"),
        )
        .reset_index()
    )
    if df_routes.shape[0] <= 1:
        return df_routes
    new_route = check_route_change(df_routes)
    if not new_route.empty:
        logger.debug(
            f"Route change detected in analyse_route_times: {new_route}"
        )
        return new_route
    return df_routes


def combine_all_routes(routes: list[str]) -> str:
    """Combines a list of routes into a single route."""
    if len(routes) < 2:
        raise ValueError("The function expects at least two routes to combine.")
    for perm in itertools.permutations(routes):
        combined_route = perm[0].split("-")
        valid_combination = True
        for i in range(1, len(perm)):
            segments = perm[i].split("-")
            if combined_route[-2:] == segments[:2]:
                combined_route += segments[2:]
            elif segments[-2:] == combined_route[:2]:
                combined_route = segments + combined_route[2:]
            else:
                valid_combination = False
                break
        if valid_combination:
            return "-".join(combined_route)


def merge_routes(df_routes: pd.DataFrame) -> pd.DataFrame:
    """Merges a dataframe of routes for a callsign into a single route."""
    grouped_routes = (
        df_routes.groupby("callsign")
        .agg(route=("route", lambda x: combine_all_routes(x.tolist())))
        .reset_index()
    )
    return grouped_routes.dropna(subset=["route"])


def filter_callsigns(flightlist: pd.DataFrame) -> pd.DataFrame:
    """Filter a dataframe to exclude callsigns that do not represent
    scheduled airline flights."""
    callsign_components = (
        flightlist["callsign"]
        .str.rstrip()
        .str.extract(
            r"^(?P<operator>[A-Z]{3})0*(?P<suffix>[1-9][A-Z0-9]*)$",
            expand=True,
        )
    )
    flightlist["callsign"] = (
        callsign_components["operator"] + callsign_components["suffix"]
    )
    flightlist.dropna(subset=["callsign"], inplace=True)
    flightlist = flightlist.loc[
        flightlist["callsign"].str.contains(
            r"^(?:[A-Z]{3})[1-9](?:(?:[0-9]{0,3})|(?:[0-9]{0,2})(?:[A-Z])|"
            r"(?:[0-9]?)(?:[A-Z]{2}))$",
            regex=True,
            na=False,
        )
    ]
    return flightlist


def process_callsign(df_callsign: pd.DataFrame) -> None | dict:
    if df_callsign.empty:
        return
    df_callsign = df_callsign[
        df_callsign["origin"] != df_callsign["destination"]
    ].copy()
    if df_callsign.empty:
        return
    df_callsign["route"] = (
        df_callsign["origin"] + "-" + df_callsign["destination"]
    )
    df_callsign = df_callsign.sort_values(by="departure")
    grouped_callsign_routes = (
        df_callsign.groupby(["callsign", "route", "origin", "destination"])
        .agg(
            count=("callsign", "size"),
            first_departure=("departure", "min"),
            last_arrival=("arrival", "max"),
        )
        .reset_index()
        .sort_values(by="first_departure")
    )
    plausible = True
    if grouped_callsign_routes.shape[0] == 1:
        result = grouped_callsign_routes.iloc[[0]]
        plausible = result["count"].iloc[0] > 1
    else:
        result = check_route_change(grouped_callsign_routes)
        if not result.empty:
            pass
        else:
            if routes_vary(grouped_callsign_routes):
                return
            if grouped_callsign_routes["count"].min() <= 1:
                return
            result = analyse_route_times(df_callsign)
            if result.empty:
                return
            elif result.shape[0] == 1:
                pass
            elif result.shape[0] > 1 and result["count"].min() > 1:
                result = merge_routes(result)
                if result.empty:
                    return
    route_candidate = result[["callsign", "route"]].to_dict(orient="records")[0]
    return set_route(
        **route_candidate, plausible=plausible, check_airports=False
    )
