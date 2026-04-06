import pytest
from route_utils import (
    estimate_max_flight_duration,
    get_route_length,
    get_single_route_length,
)

# ---------------------------------------------------------------------------
# get_single_route_length
# ---------------------------------------------------------------------------


def test_get_single_route_length_known_pair():
    """EDDG to EDDF: roughly 242 km spherical."""
    result = get_single_route_length("EDDG", "EDDF")
    assert 235_000 < result < 250_000


def test_get_single_route_length_symmetric():
    """Distance A→B must equal B→A."""
    forward = get_single_route_length("EDDG", "EDDF")
    reverse = get_single_route_length("EDDF", "EDDG")
    assert abs(forward - reverse) < 1.0


def test_get_single_route_length_same_airport():
    """Same origin and destination should return approximately zero."""
    result = get_single_route_length("EDDG", "EDDG")
    assert result == pytest.approx(0.0, abs=1.0)


# ---------------------------------------------------------------------------
# get_route_length
# ---------------------------------------------------------------------------


def test_get_route_length_two_airports():
    """Two-segment route should match get_single_route_length."""
    via_string = get_route_length("EDDG-EDDF")
    direct = get_single_route_length("EDDG", "EDDF")
    assert via_string == pytest.approx(direct, rel=1e-6)


def test_get_route_length_three_airports():
    """Three-airport route should be sum of two legs."""
    leg1 = get_single_route_length("EDDG", "EDDF")
    leg2 = get_single_route_length("EDDF", "EDDM")
    total = get_route_length("EDDG-EDDF-EDDM")
    assert total == pytest.approx(leg1 + leg2, rel=1e-6)


def test_get_route_length_three_greater_than_two():
    """Route with stopover must be longer than direct flight."""
    direct = get_route_length("EDDG-EDDM")
    via_frankfurt = get_route_length("EDDG-EDDF-EDDM")
    assert via_frankfurt > direct


def test_get_route_length_requires_at_least_two_airports():
    with pytest.raises((AssertionError, ValueError)):
        get_route_length("EDDG")


# ---------------------------------------------------------------------------
# estimate_max_flight_duration
# ---------------------------------------------------------------------------


def test_estimate_max_flight_duration_increases_with_distance():
    short = estimate_max_flight_duration(get_route_length("EDDG-EDDF"))
    long_ = estimate_max_flight_duration(get_route_length("EDDF-EGLL"))
    assert long_ > short


def test_estimate_max_flight_duration_minimum_is_offset():
    """For a zero-distance route the result should be the offset (1500s)."""
    result = estimate_max_flight_duration(0)
    assert result == pytest.approx(1500, rel=1e-6)


def test_estimate_max_flight_duration_transatlantic_reasonable():
    """EDDF to KJFK is roughly 6200 km; max duration should be 8–12 hours."""
    distance = get_route_length("EDDF-KJFK")
    duration_seconds = estimate_max_flight_duration(distance)
    assert 8 * 3600 < duration_seconds < 12 * 3600


# ---------------------------------------------------------------------------
# route_check_simple and single_route_check_simple
#
# Positions are taken from the original test_route_check.py, which used a
# real KLM flight (KLM23C / KLM1107) on route EHAM-ESSA.
# Timestamps: 1512286238 = 2017-12-03 ~07:30 UTC (near Amsterdam, climbing)
#             1512289352 = 2017-12-03 ~08:22 UTC (cruise, over Denmark/Sweden)
#             1512292418 = 2017-12-03 ~09:13 UTC (near Stockholm, descending)
# ---------------------------------------------------------------------------

from route_utils import route_check_simple, single_route_check_simple

_POS_NEAR_AMSTERDAM = {
    "utc": 1512286238,
    "latitude": 52.3035,
    "longitude": 4.7786,
    "altitude": 125,
    "heading": 20.0,
    "vertical_rate": 5.0,
    "velocity": 80.0,
    "on_ground": False,
    "callsign": "KLM23C",
    "icao24": "484161",
}

_POS_CRUISE = {
    "utc": 1512289352,
    "latitude": 56.088,
    "longitude": 11.779,
    "altitude": 41025,
    "heading": 20.0,
    "vertical_rate": 0.0,
    "velocity": 250.0,
    "on_ground": False,
    "callsign": "KLM23C",
    "icao24": "484161",
}

_POS_NEAR_STOCKHOLM = {
    "utc": 1512292418,
    "latitude": 59.665,
    "longitude": 17.987,
    "altitude": 550,
    "heading": 20.0,
    "vertical_rate": -5.0,
    "velocity": 80.0,
    "on_ground": False,
    "callsign": "KLM23C",
    "icao24": "484161",
}

_POS_DEVIATED = {
    "utc": 1512289352,
    "latitude": 60.0,
    "longitude": 8.0,
    "altitude": 41025,
    "heading": 90.0,
    "vertical_rate": 0.0,
    "velocity": 250.0,
    "on_ground": False,
    "callsign": "KLM23C",
    "icao24": "484161",
}


def test_single_route_check_cruise_position_passes():
    """Cruise position on EHAM-ESSA should not fail."""
    result = single_route_check_simple(_POS_CRUISE.copy(), "EHAM-ESSA")
    assert result is not None
    assert result["check_failed"] is False


def test_single_route_check_deviated_position_fails():
    """Position far off track should fail the check."""
    result = single_route_check_simple(_POS_DEVIATED.copy(), "EHAM-ESSA")
    assert result is not None
    assert result["check_failed"] is True


def test_single_route_check_wrong_direction_fails():
    """Cruise position on EHAM-ESSA checked against reversed route ESSA-EHAM
    should fail since progress would be inverted."""
    result = single_route_check_simple(_POS_CRUISE.copy(), "ESSA-EHAM")
    assert result is not None
    assert result["check_failed"] is True


def test_single_route_check_same_airports_returns_none():
    result = single_route_check_simple(_POS_CRUISE.copy(), "EHAM-EHAM")
    assert result is None


def test_single_route_check_returns_expected_fields():
    result = single_route_check_simple(_POS_CRUISE.copy(), "EHAM-ESSA")
    assert result is not None
    for _key in (
        "route",
        "deviation",
        "error_angle",
        "progress",
        "check_failed",
        "dist_origin",
        "dist_destination",
    ):
        assert _key in result


def test_route_check_simple_multi_segment_cruise_passes():
    """Cruise position mid-flight on EHAM-ESSA should pass route_check_simple."""
    result = route_check_simple(_POS_CRUISE.copy(), "EHAM-ESSA")
    assert result is not None
    assert result["check_failed"] is False


def test_route_check_simple_wrong_route_fails():
    result = route_check_simple(_POS_CRUISE.copy(), "ESSA-EDDF")
    assert result is not None
    assert result["check_failed"] is True
