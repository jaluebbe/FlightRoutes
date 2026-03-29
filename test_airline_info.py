import pytest
from airline_info import (
    get_airline_by_iata,
    get_airline_iata,
    get_airline_icao,
    get_airline_icaos,
    get_airline_info,
    get_airlines_by_iata,
)

# ---------------------------------------------------------------------------
# get_airline_info
# ---------------------------------------------------------------------------


def test_get_airline_info_known():
    result = get_airline_info("DLH")
    assert result is not None
    assert result["ICAO"] == "DLH"
    assert result["IATA"] == "LH"
    assert "Lufthansa" in result["Name"]


def test_get_airline_info_unknown_returns_none():
    assert get_airline_info("ZZZ") is None


def test_get_airline_info_invalid_raises():
    with pytest.raises(ValueError):
        get_airline_info("DL")
    with pytest.raises(ValueError):
        get_airline_info("DLHX")
    with pytest.raises(ValueError):
        get_airline_info("DL1")
    with pytest.raises(ValueError):
        get_airline_info(None)


# ---------------------------------------------------------------------------
# get_airline_iata
# ---------------------------------------------------------------------------


def test_get_airline_iata_known():
    assert get_airline_iata("DLH") == "LH"
    assert get_airline_iata("KLM") == "KL"
    assert get_airline_iata("AAL") == "AA"
    assert get_airline_iata("BAW") == "BA"
    assert get_airline_iata("SWR") == "LX"


def test_get_airline_iata_unknown_returns_none():
    assert get_airline_iata("ZZZ") is None


# ---------------------------------------------------------------------------
# get_airlines_by_iata
# ---------------------------------------------------------------------------


def test_get_airlines_by_iata_unique_returns_one():
    results = get_airlines_by_iata("LH")
    assert isinstance(results, list)
    assert any(_row["ICAO"] == "DLH" for _row in results)


def test_get_airlines_by_iata_duplicate_iata_returns_multiple():
    """BA is a known duplicate IATA — both BAW and others share it."""
    results = get_airlines_by_iata("BA")
    assert len(results) > 1


def test_get_airlines_by_iata_unknown_returns_empty_list():
    results = get_airlines_by_iata("ZZ")
    assert results == []


def test_get_airlines_by_iata_invalid_raises():
    with pytest.raises(ValueError):
        get_airlines_by_iata("LHX")
    with pytest.raises(ValueError):
        get_airlines_by_iata("L")
    with pytest.raises(ValueError):
        get_airlines_by_iata(None)


def test_get_airlines_by_iata_returns_list_of_dicts():
    results = get_airlines_by_iata("LH")
    for _item in results:
        assert isinstance(_item, dict)
        assert "ICAO" in _item
        assert "IATA" in _item
        assert "Name" in _item


# ---------------------------------------------------------------------------
# get_airline_by_iata
# ---------------------------------------------------------------------------


def test_get_airline_by_iata_unique():
    result = get_airline_by_iata("KL")
    assert result is not None
    assert result["ICAO"] == "KLM"


def test_get_airline_by_iata_duplicate_without_name_returns_none():
    """BA is a duplicate IATA — without a name hint the result is ambiguous."""
    result = get_airline_by_iata("BA")
    assert result is None


def test_get_airline_by_iata_duplicate_resolved_by_name():
    result = get_airline_by_iata("BA", name="British Airways")
    assert result is not None
    assert result["ICAO"] == "BAW"


def test_get_airline_by_iata_name_disambiguation():
    # "ITA" scores lower than "ITA Airways" against the DB name "ITA Airways"
    # due to string length effects — use the fuller name to disambiguate from
    # Alitalia, whose name "Alitalia" scores higher against the short hint "ITA".
    for _iata, _name, _expected_icao in [
        ("LH", "Lufthansa", "DLH"),
        ("LX", "Swiss", "SWR"),
        ("AZ", "ITA Airways", "ITY"),
        ("AA", "American Airlines", "AAL"),
        ("EW", "Eurowings", "EWG"),
    ]:
        result = get_airline_by_iata(_iata, name=_name)
        assert result is not None, f"No result for {_iata} / {_name}"
        assert result["ICAO"] == _expected_icao, (
            f"Expected {_expected_icao} for {_iata} / {_name}, "
            f"got {result['ICAO']}"
        )


def test_get_airline_by_iata_low_similarity_still_returns_best_match(caplog):
    """A poor name hint should still return the best available match but
    emit a warning when the score falls below the threshold."""
    import logging

    with caplog.at_level(logging.WARNING, logger="airline_info"):
        result = get_airline_by_iata("BA", name="XYZXYZXYZ")
    assert result is not None
    assert any(
        "low similarity" in _record.message for _record in caplog.records
    )


def test_get_airline_by_iata_lh_cargo_flight_number():
    result = get_airline_by_iata("LH", flight_number=8100)
    assert result is not None
    assert "Cargo" in result["Name"]


def test_get_airline_by_iata_lh_regular_flight_number():
    result = get_airline_by_iata("LH", flight_number=400)
    assert result is not None
    assert result["ICAO"] == "DLH"


def test_get_airline_by_iata_unknown_returns_none():
    assert get_airline_by_iata("ZZ") is None


# ---------------------------------------------------------------------------
# get_airline_icao
# ---------------------------------------------------------------------------


def test_get_airline_icao_with_name():
    assert get_airline_icao("LH", name="Lufthansa") == "DLH"
    assert get_airline_icao("KL", name="KLM") == "KLM"
    assert get_airline_icao("LX", name="Swiss") == "SWR"
    assert get_airline_icao("AZ", name="ITA Airways") == "ITY"
    assert get_airline_icao("AA", name="American Airlines") == "AAL"


def test_get_airline_icao_unique_iata_no_name_needed():
    assert get_airline_icao("KL") == "KLM"


def test_get_airline_icao_duplicate_without_name_returns_none():
    assert get_airline_icao("BA") is None


def test_get_airline_icao_unknown_returns_none():
    assert get_airline_icao("ZZ") is None


# ---------------------------------------------------------------------------
# get_airline_icaos
# ---------------------------------------------------------------------------


def test_get_airline_icaos_unique():
    result = get_airline_icaos("KL")
    assert isinstance(result, list)
    assert "KLM" in result


def test_get_airline_icaos_duplicate_returns_multiple():
    result = get_airline_icaos("BA")
    assert isinstance(result, list)
    assert len(result) > 1
    assert "BAW" in result


def test_get_airline_icaos_unknown_returns_empty_list():
    result = get_airline_icaos("ZZ")
    assert result == []


def test_get_airline_icaos_returns_list_of_strings():
    for _icao in get_airline_icaos("LH"):
        assert isinstance(_icao, str)
        assert len(_icao) == 3
