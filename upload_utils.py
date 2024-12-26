import itertools
import json
import logging
import os
import re

import pandas as pd
import redis
import requests

from opensky_utils import validated_callsign
from route_utils import check_route_airports, convert_to_iata_route
from vrs_standing_data import get_airline_routes

redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
redis_connection = redis.Redis(host=redis_host, decode_responses=True)


def set_route(
    callsign: str,
    route: str,
    plausible: bool = False,
    check_airports: bool = True,
):
    callsign_info = validated_callsign(callsign.upper())
    if callsign_info is None:
        print(f"{callsign} did not pass callsign check.")
        return
    callsign = callsign_info["callsign"]
    operator = callsign_info["operator_icao"]
    route = route.upper()
    if check_airports:
        airline_routes = get_airline_routes(operator)
        airline_airports = list(
            {
                airport
                for route in airline_routes
                for airport in route.split("-")
            }
        )
    else:
        airline_airports = None
    if check_route_airports(route, airline_airports) is None:
        print(f"{callsign}: {route} did not pass route check.")
        return
    _key = f"route:{callsign}"
    if not plausible:
        _existing_entry = redis_connection.get(_key)
        if (
            _existing_entry is not None
            and json.loads(_existing_entry)["plausible"]
        ):
            return
    _iata_route = convert_to_iata_route(route)
    _data = {
        "_airport_codes_iata": _iata_route,
        "airport_codes": route,
        "callsign": callsign,
        "plausible": int(plausible),
    }
    redis_connection.set(_key, json.dumps(_data))
    return _data


def get_known_callsigns() -> list[str]:
    """Return all callsigns known to the Redis DB."""
    return [key.split(":")[1] for key in redis_connection.scan_iter("route:*")]


def get_route(callsign: str) -> dict | None:
    """Return the known route data for a given callsign. May return None."""
    data = redis_connection.get(f"route:{callsign}")
    if data is not None:
        return json.loads(data)


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
    """Analyse routes belonging to the same callsign ordered by departure time.
    Routes are combined if their destination/origin match and the stopover
    time is not too long."""
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
        logging.debug(
            f"Route change detected in analyse_route_times: {new_route}"
        )
        return new_route
    return df_routes


def combine_all_routes(routes: list[str]) -> str:
    """
    Combines a list of routes into a single route.
    """
    if len(routes) < 2:
        raise ValueError("The function expects at least two routes to combine.")
    # Check all possible permutations of the routes
    for perm in itertools.permutations(routes):
        combined_route = perm[0].split("-")
        valid_combination = True
        for i in range(1, len(perm)):
            segments = perm[i].split("-")
            # Check if the last two segments of the combined route match the
            # first two segments of the current route
            if combined_route[-2:] == segments[:2]:
                combined_route += segments[2:]
            # Check if the last two segments of the current route match the
            # first two segments of the combined route
            elif segments[-2:] == combined_route[:2]:
                combined_route = segments + combined_route[2:]
            else:
                valid_combination = False
                break
        if valid_combination:
            return "-".join(combined_route)


def merge_routes(df_routes: pd.DataFrame) -> pd.DataFrame:
    """
    Merges a dataframe of routes for a callsign into a single route.
    """
    grouped_routes = (
        df_routes.groupby("callsign")
        .agg(route=("route", lambda x: combine_all_routes(x.tolist())))
        .reset_index()
    )
    grouped_routes = grouped_routes.dropna(subset=["route"])
    return grouped_routes


def process_callsign(df_callsign: pd.DataFrame) -> None | dict:
    if df_callsign.empty:
        return
    df_callsign = df_callsign[
        df_callsign["origin"] != df_callsign["destination"]
    ].copy()
    if df_callsign.empty:
        # No recent callsign history except for roundtrips.
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
