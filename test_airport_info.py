import math
import pytest
from airport_info import (
    get_airport_iata,
    get_airport_icao,
    get_airport_info,
    get_airport_label,
    get_closest_airport,
    get_closest_airports,
    get_distance,
)

# ---------------------------------------------------------------------------
# get_distance
# ---------------------------------------------------------------------------


def test_get_distance_identical_points_no_exception():
    """Identical points must return 0 without raising due to acos domain error."""
    result = get_distance(52.0, 7.0, 52.0, 7.0)
    assert result == pytest.approx(0.0, abs=1.0)


def test_get_distance_known_pair():
    """EDDG (Münster/Osnabrück) to EDDF (Frankfurt): roughly 242 km spherical."""
    result = get_distance(52.1347, 7.6848, 50.0264, 8.5431)
    assert 235_000 < result < 250_000


def test_get_distance_antipodal_points():
    """Antipodal points should return approximately half Earth's circumference."""
    result = get_distance(0.0, 0.0, 0.0, 180.0)
    assert result == pytest.approx(math.pi * 6.370e6, rel=1e-4)


def test_get_distance_none_argument_returns_nan():
    assert math.isnan(get_distance(None, 7.0, 52.0, 7.0))
    assert math.isnan(get_distance(52.0, None, 52.0, 7.0))
    assert math.isnan(get_distance(52.0, 7.0, None, 7.0))
    assert math.isnan(get_distance(52.0, 7.0, 52.0, None))


# ---------------------------------------------------------------------------
# get_airport_info
# ---------------------------------------------------------------------------


def test_get_airport_info_known():
    result = get_airport_info("EDDG")
    assert result is not None
    assert result["ICAO"] == "EDDG"
    assert result["IATA"] == "FMO"
    assert isinstance(result["Latitude"], float)
    assert isinstance(result["Longitude"], float)


def test_get_airport_info_unknown_returns_none():
    assert get_airport_info("ZZZZ") is None


def test_get_airport_info_invalid_raises():
    with pytest.raises(ValueError):
        get_airport_info("ED")
    with pytest.raises(ValueError):
        get_airport_info("EDDGG")
    with pytest.raises(ValueError):
        get_airport_info(None)


# ---------------------------------------------------------------------------
# get_airport_label
# ---------------------------------------------------------------------------


def test_get_airport_label_known():
    label = get_airport_label("EDDF")
    assert "EDDF" in label
    assert "Frankfurt" in label


def test_get_airport_label_unknown():
    label = get_airport_label("ZZZZ")
    assert "ZZZZ" in label
    assert "unknown" in label.lower()


# ---------------------------------------------------------------------------
# get_airport_iata
# ---------------------------------------------------------------------------


def test_get_airport_iata_known():
    assert get_airport_iata("EDDG") == "FMO"
    assert get_airport_iata("EDDF") == "FRA"


def test_get_airport_iata_airport_without_iata_returns_empty_or_none():
    """EDWO (Osnabrück-Atterheide) has no IATA code."""
    result = get_airport_iata("EDWO")
    assert result is None or result == ""


def test_get_airport_iata_unknown_icao_returns_none():
    assert get_airport_iata("ZZZZ") is None


# ---------------------------------------------------------------------------
# get_airport_icao
# ---------------------------------------------------------------------------


def test_get_airport_icao_known():
    assert get_airport_icao("FMO") == "EDDG"
    assert get_airport_icao("FRA") == "EDDF"


def test_get_airport_icao_unknown_returns_none():
    """City group codes like NYC and LON are not airport IATA codes."""
    assert get_airport_icao("NYC") is None
    assert get_airport_icao("LON") is None


def test_get_airport_icao_invalid_raises():
    with pytest.raises(ValueError):
        get_airport_icao("FRA1")
    with pytest.raises(ValueError):
        get_airport_icao("FR")
    with pytest.raises(ValueError):
        get_airport_icao(None)


def test_get_airport_icao_roundtrip():
    """ICAO -> IATA -> ICAO must be stable for airports with IATA codes."""
    for icao in ("EDDG", "EDDF", "EDDH", "EGLL"):
        iata = get_airport_iata(icao)
        if iata:
            assert get_airport_icao(iata) == icao


# ---------------------------------------------------------------------------
# get_closest_airports
# ---------------------------------------------------------------------------


def test_get_closest_airports_near_eddg():
    """EDDG is the closest airport to its own coordinates."""
    results = get_closest_airports(52.1347, 7.6848)
    assert len(results) > 0
    assert results[0]["ICAO"] == "EDDG"


def test_get_closest_airports_returns_list_of_dicts():
    results = get_closest_airports(52.1347, 7.6848)
    assert isinstance(results, list)
    for _item in results:
        assert isinstance(_item, dict)
        assert "ICAO" in _item
        assert "Distance" in _item


def test_get_closest_airports_iata_only_all_have_iata():
    results = get_closest_airports(52.1347, 7.6848, iata_only=True)
    assert len(results) > 0
    for _item in results:
        assert len(_item["IATA"]) == 3


def test_get_closest_airports_iata_only_limit():
    results = get_closest_airports(52.1347, 7.6848, iata_only=True)
    assert len(results) <= 5


def test_get_closest_airports_ordered_by_distance():
    results = get_closest_airports(52.1347, 7.6848)
    distances = [_item["Distance"] for _item in results]
    assert distances == sorted(distances)


def test_get_closest_airports_distant_location_returns_results():
    """The latitude window pre-filter does not cap distance — airports from
    within ±1° latitude are returned regardless of longitude separation.
    A mid-Atlantic position at 0°N 30°W still matches Brazilian airports."""
    results = get_closest_airports(0.0, -30.0)
    assert len(results) > 0
    for _item in results:
        assert _item["Distance"] > 1_000_000


def test_get_closest_airports_fiji_east(caplog):
    """NFNL (Labasa) is near longitude +179.3 — tests antimeridian east side."""
    results = get_closest_airports(-16.466, 179.340)
    icaos = [_item["ICAO"] for _item in results]
    assert "NFNL" in icaos


def test_get_closest_airports_fiji_west():
    """NFNM (Taveuni) is near longitude -179.9 — tests antimeridian west side."""
    results = get_closest_airports(-16.691, -179.877)
    icaos = [_item["ICAO"] for _item in results]
    assert "NFNM" in icaos


# ---------------------------------------------------------------------------
# get_closest_airport
# ---------------------------------------------------------------------------


def test_get_closest_airport_returns_single_dict():
    result = get_closest_airport(52.1347, 7.6848)
    assert isinstance(result, dict)
    assert result["ICAO"] == "EDDG"


def test_get_closest_airport_distant_location_returns_nearest():
    """get_closest_airport always returns the nearest result from the latitude
    band — it does not filter by maximum distance."""
    result = get_closest_airport(0.0, -30.0)
    assert isinstance(result, dict)
    assert result["Distance"] > 1_000_000
