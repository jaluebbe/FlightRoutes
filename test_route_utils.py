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
