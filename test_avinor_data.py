import logging
import arrow
import pymongo
import pytest
from unittest.mock import patch, MagicMock
from avinor_data import (
    Airport,
    _get_date_and_time,
    _status_codes,
    request_airport_data,
)
from route_utils import get_route_length

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_response(xml: str) -> MagicMock:
    mock = MagicMock()
    mock.text = xml
    return mock


def _wrap(airport_name: str, flights_xml: str) -> str:
    return (
        f'<?xml version="1.0" encoding="ISO-8859-1"?>'
        f'<airport name="{airport_name}">'
        f'<flights lastUpdate="2026-04-06T12:00:00Z">'
        f"{flights_xml}"
        f"</flights></airport>"
    )


def _flight_xml(
    unique_id,
    airline,
    flight_id,
    dom_int,
    schedule_time,
    arr_dep,
    airport,
    via_airport=None,
    status_code=None,
    status_time=None,
):
    via = f"<via_airport>{via_airport}</via_airport>" if via_airport else ""
    status = (
        f'<status code="{status_code}" time="{status_time}"/>'
        if status_code
        else ""
    )
    return (
        f'<flight uniqueID="{unique_id}">'
        f"<airline>{airline}</airline>"
        f"<flight_id>{flight_id}</flight_id>"
        f"<dom_int>{dom_int}</dom_int>"
        f"<schedule_time>{schedule_time}</schedule_time>"
        f"<arr_dep>{arr_dep}</arr_dep>"
        f"<airport>{airport}</airport>"
        f"{via}{status}"
        f"</flight>"
    )


# ---------------------------------------------------------------------------
# _get_date_and_time
# ---------------------------------------------------------------------------


def test_get_date_and_time_uses_schedule_time():
    flight = {"schedule_time": "2026-04-06T10:30:00Z"}
    date, ts = _get_date_and_time(flight)
    assert date == "2026-04-06"
    assert ts == arrow.get("2026-04-06T10:30:00Z").timestamp()


def test_get_date_and_time_uses_status_time_when_present():
    flight = {
        "schedule_time": "2026-04-06T10:30:00Z",
        "status": {"@code": "A", "@time": "2026-04-06T10:22:34Z"},
    }
    date, ts = _get_date_and_time(flight)
    assert date == "2026-04-06"
    assert ts == arrow.get("2026-04-06T10:22:34Z").timestamp()


def test_get_date_and_time_status_without_time_uses_schedule():
    """Status present but no @time — falls back to schedule_time."""
    flight = {
        "schedule_time": "2026-04-06T10:30:00Z",
        "status": {"@code": "C"},
    }
    date, ts = _get_date_and_time(flight)
    assert ts == arrow.get("2026-04-06T10:30:00Z").timestamp()


# ---------------------------------------------------------------------------
# _status_codes
# ---------------------------------------------------------------------------


def test_status_codes_complete():
    for code in ("A", "C", "D", "E", "N"):
        assert code in _status_codes


# ---------------------------------------------------------------------------
# request_airport_data — XML parsing via mocked requests.get
# ---------------------------------------------------------------------------


def test_request_airport_data_basic_departure():
    xml = _wrap(
        "BGO",
        _flight_xml(
            1,
            "SK",
            "SK267",
            "D",
            "2026-04-06T12:20:00Z",
            "D",
            "OSL",
            status_code="D",
            status_time="2026-04-06T12:25:00Z",
        ),
    )
    with patch("avinor_data.requests.get", return_value=_make_response(xml)):
        results = list(request_airport_data("BGO"))
    assert len(results) == 1
    _r = results[0]
    assert _r["airline_iata"] == "SK"
    assert _r["airline_icao"] == "SAS"
    assert _r["flight_number"] == 267
    assert _r["route"] == "ENBR-ENGM"
    assert "departure" in _r
    assert "arrival" not in _r
    assert _r["status"] == "departed"


def test_request_airport_data_basic_arrival():
    xml = _wrap(
        "BGO",
        _flight_xml(
            2,
            "LH",
            "LH872",
            "S",
            "2026-04-06T10:55:00Z",
            "A",
            "FRA",
            status_code="A",
            status_time="2026-04-06T11:08:38Z",
        ),
    )
    with patch("avinor_data.requests.get", return_value=_make_response(xml)):
        results = list(request_airport_data("BGO"))
    assert len(results) == 1
    _r = results[0]
    assert _r["airline_iata"] == "LH"
    assert _r["flight_number"] == 872
    assert _r["route"] == "EDDF-ENBR"
    assert "arrival" in _r
    assert "departure" not in _r
    assert _r["status"] == "arrived"


def test_request_airport_data_single_flight_no_crash():
    """A single flight must not crash — force_list fix."""
    xml = _wrap(
        "ALF",
        _flight_xml(
            1,
            "SK",
            "SK4543",
            "D",
            "2026-04-06T05:15:00Z",
            "D",
            "OSL",
            status_code="D",
            status_time="2026-04-06T05:07:00Z",
        ),
    )
    with patch("avinor_data.requests.get", return_value=_make_response(xml)):
        results = list(request_airport_data("ALF"))
    assert len(results) == 1


def test_request_airport_data_stopover():
    """via_airport should appear as intermediate ICAO in the route."""
    xml = _wrap(
        "ALF",
        _flight_xml(
            3,
            "WF",
            "WF904",
            "D",
            "2026-04-06T10:30:00Z",
            "A",
            "TOS",
            via_airport="HFT",
            status_code="A",
            status_time="2026-04-06T10:22:34Z",
        ),
    )
    with patch("avinor_data.requests.get", return_value=_make_response(xml)):
        results = list(request_airport_data("ALF"))
    assert len(results) == 1
    _r = results[0]
    icaos = _r["route"].split("-")
    assert len(icaos) == 3
    assert icaos[-1] == "ENAT"


def test_request_airport_data_stopover_strip():
    """via_airport with space after comma must be stripped correctly."""
    xml = _wrap(
        "BGO",
        _flight_xml(
            4,
            "WF",
            "WF1306",
            "D",
            "2026-04-06T11:40:00Z",
            "A",
            "BOO",
            via_airport="TRD",
            status_code="A",
            status_time="2026-04-06T11:32:36Z",
        ),
    )
    with patch("avinor_data.requests.get", return_value=_make_response(xml)):
        results = list(request_airport_data("BGO"))
    assert len(results) == 1
    icaos = results[0]["route"].split("-")
    assert len(icaos) == 3
    assert None not in icaos


def test_request_airport_data_no_status():
    """Flights without a status element should still be yielded."""
    xml = _wrap(
        "ALF",
        _flight_xml(
            5,
            "SK",
            "SK4434",
            "D",
            "2026-04-06T19:50:00Z",
            "A",
            "OSL",
            via_airport="TOS",
        ),
    )
    with patch("avinor_data.requests.get", return_value=_make_response(xml)):
        results = list(request_airport_data("ALF"))
    assert len(results) == 1
    assert "status" not in results[0]


def test_request_airport_data_4y_override():
    """4Y (Sundair) must be resolved to ICAO BGA via override."""
    xml = _wrap(
        "ALF",
        _flight_xml(
            6,
            "4Y",
            "4Y1302",
            "S",
            "2026-04-05T12:40:00Z",
            "A",
            "FRA",
            status_code="A",
            status_time="2026-04-05T12:27:06Z",
        ),
    )
    with patch("avinor_data.requests.get", return_value=_make_response(xml)):
        results = list(request_airport_data("ALF"))
    assert len(results) == 1
    assert results[0]["airline_icao"] == "BGA"


def test_request_airport_data_3char_airline():
    """3-char airline codes should resolve via get_airline_iata."""
    xml = _wrap(
        "OSL",
        _flight_xml(
            7,
            "DLH",
            "LH100",
            "S",
            "2026-04-06T10:00:00Z",
            "D",
            "FRA",
            status_code="D",
            status_time="2026-04-06T10:05:00Z",
        ),
    )
    with patch("avinor_data.requests.get", return_value=_make_response(xml)):
        results = list(request_airport_data("OSL"))
    assert len(results) == 1
    assert results[0]["airline_icao"] == "DLH"
    assert results[0]["airline_iata"] == "LH"


def test_request_airport_data_empty_flights():
    """Airport with no flights returns nothing without error."""
    xml = (
        '<?xml version="1.0" encoding="ISO-8859-1"?>'
        '<airport name="BJF"><flights lastUpdate="2026-04-06T12:00:00Z"/></airport>'
    )
    with patch("avinor_data.requests.get", return_value=_make_response(xml)):
        results = list(request_airport_data("BJF"))
    assert results == []


def test_request_airport_data_id_format():
    """_id must follow the pattern iata_flightnumber_date_route."""
    xml = _wrap(
        "BGO",
        _flight_xml(
            8,
            "KL",
            "KL1167",
            "S",
            "2026-04-06T11:25:00Z",
            "A",
            "AMS",
            status_code="A",
            status_time="2026-04-06T11:22:55Z",
        ),
    )
    with patch("avinor_data.requests.get", return_value=_make_response(xml)):
        results = list(request_airport_data("BGO"))
    assert len(results) == 1
    _id = results[0]["_id"]
    parts = _id.split("_")
    assert parts[0] == "KL"
    assert parts[1] == "1167"
    assert parts[2] == "2026-04-06"
    assert parts[3] == results[0]["route"]


def test_request_airport_data_new_time_status():
    """Status code E must map to new_time."""
    xml = _wrap(
        "BGO",
        _flight_xml(
            9,
            "WF",
            "WF924",
            "D",
            "2026-04-06T13:05:00Z",
            "A",
            "TOS",
            status_code="E",
            status_time="2026-04-06T13:01:00Z",
        ),
    )
    with patch("avinor_data.requests.get", return_value=_make_response(xml)):
        results = list(request_airport_data("BGO"))
    assert len(results) == 1
    assert results[0]["status"] == "new_time"


def test_request_airport_data_route_length_reasonable():
    """BGO-FRA route should be roughly 1162 km spherical."""
    xml = _wrap(
        "BGO",
        _flight_xml(
            10,
            "LH",
            "LH873",
            "S",
            "2026-04-06T11:40:00Z",
            "D",
            "FRA",
            status_code="D",
            status_time="2026-04-06T12:13:00Z",
        ),
    )
    with patch("avinor_data.requests.get", return_value=_make_response(xml)):
        results = list(request_airport_data("BGO"))
    assert len(results) == 1
    km = get_route_length(results[0]["route"]) * 1e-3
    assert 1050 < km < 1300


# ---------------------------------------------------------------------------
# Smoke test against live MongoDB (requires update_data to have been run)
# ---------------------------------------------------------------------------


def test_smoke_active_flights():
    airport = Airport()
    utc = arrow.utcnow().timestamp()
    flights = airport.get_active_flights(utc)
    assert isinstance(flights, list)
    for _flight in flights:
        assert "airline_iata" in _flight
        assert "airline_icao" in _flight
        assert "flight_number" in _flight
        assert "route" in _flight
        assert "-" in _flight["route"]
