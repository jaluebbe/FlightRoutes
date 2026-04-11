import io
import logging
import pathlib
import tempfile
import arrow
import pytest
from unittest.mock import patch
from aa_cargo_data import (
    _parse_date,
    _parse_dow,
    _read_csv_file,
    _process_row,
    Airline,
)

# ---------------------------------------------------------------------------
# CSV fixture helpers
# ---------------------------------------------------------------------------

_HEADER = (
    "\n"
    "Confirmed FS Schedule,,,,,,,,,,\n"
    '"Effective May 1, 2026 to May 31, 2026",,,,,,,,,,\n'
    "\n"
    "Origin,Dest.,Flight,Flight,Departure,Arrival,Aircraft,Subfleet,"
    "Effective,Discontinue,\n"
    "Airport,Airport,Number,Frequency,Time,Time,Type,Type,Date, Date,\n"
    '"1=MON, 2=TUE, 3=WED, 4=THUR, 5=FRI, 6=SAT, 7=SUN",,,,,,,,,,\n'
    "Column1,Column2,Column3,Column4,Column5,Column6,Column7,Column73,"
    "Column8,Column9,\n"
)


def _make_csv(*data_rows: str) -> str:
    return _HEADER + "\n".join(data_rows) + "\n"


def _write_csv(tmp_path: pathlib.Path, content: str) -> pathlib.Path:
    _f = tmp_path / "ConfirmedFSMay2026.csv"
    _f.write_text(content, encoding="utf-8-sig")
    return _f


def _run_update(csv_content: str, utc: arrow.Arrow) -> list[dict]:
    stored = []
    airline = Airline()
    with tempfile.TemporaryDirectory() as _d:
        _p = (
            pathlib.Path(_d)
            / f"ConfirmedFS{utc.format('MMM')}{utc.format('YYYY')}.csv"
        )
        _p.write_text(csv_content, encoding="utf-8-sig")
        with patch("aa_cargo_data.PWD", pathlib.Path(_d)):
            with patch.object(
                airline, "update_flight", side_effect=stored.append
            ):
                airline.update_data(utc)
    return stored


# Monday 2026-05-04 — isoweekday() == 1
_MON = arrow.get("2026-05-04T12:00:00")
# Wednesday 2026-05-06
_WED = arrow.get("2026-05-06T12:00:00")

# JFK -> LHR every day, valid all month
_ROW_JFK_LHR = "JFK,LHR,0100,1234567,22:00,10:00,772,772,5/1/26,5/31/26,"
# CLT -> CDG Monday only
_ROW_CLT_CDG_MON = "CLT,CDG,0786,1......,17:35,07:45,772,772,5/1/26,5/31/26,"
# DFW -> NRT with short validity (Mon–Wed 5/4–5/6 only)
_ROW_DFW_NRT = "DFW,NRT,0061,123....,11:00,14:30,789,789,5/4/26,5/6/26,"
# Domestic CLT->ATL every day
_ROW_CLT_ATL = "CLT,ATL,0001,1234567,07:00,08:15,738,38K,5/1/26,5/31/26,"
# Non-numeric flight number — should be skipped
_ROW_BAD_FNR = "CLT,LHR,ABCD,1234567,17:00,05:00,772,772,5/1/26,5/31/26,"
# Unknown airport
_ROW_UNKNOWN_APT = "CLT,ZZZ,0999,1234567,10:00,12:00,320,320,5/1/26,5/31/26,"
# Overnight: departs 23:00, arrives 07:00 next day
_ROW_OVERNIGHT = "MIA,LHR,0200,1234567,23:00,07:00,777,772,5/1/26,5/31/26,"


# ---------------------------------------------------------------------------
# _parse_dow
# ---------------------------------------------------------------------------


def test_parse_dow_daily():
    assert _parse_dow("1234567") == set(range(1, 8))


def test_parse_dow_daily_keyword():
    assert _parse_dow("DAILY") == set(range(1, 8))


def test_parse_dow_single_day():
    assert _parse_dow(".....6.") == {6}


def test_parse_dow_multiple_days():
    assert _parse_dow("1...5..") == {1, 5}


def test_parse_dow_dots_only():
    assert _parse_dow(".......") == set()


# ---------------------------------------------------------------------------
# _parse_date
# ---------------------------------------------------------------------------


def test_parse_date_valid():
    result = _parse_date("5/1/26")
    assert result.format("YYYY-MM-DD") == "2026-05-01"


def test_parse_date_empty():
    assert _parse_date("") is None


def test_parse_date_whitespace():
    assert _parse_date("  ") is None


# ---------------------------------------------------------------------------
# _read_csv_file
# ---------------------------------------------------------------------------


def test_read_csv_file_parses_data_rows(tmp_path):
    _p = _write_csv(tmp_path, _make_csv(_ROW_JFK_LHR, _ROW_CLT_ATL))
    rows = _read_csv_file(_p)
    assert len(rows) == 2
    assert rows[0]["origin"] == "JFK"
    assert rows[0]["dest"] == "LHR"
    assert rows[0]["flight_number"] == "0100"
    assert rows[0]["departs"] == "22:00"
    assert rows[0]["arrives"] == "10:00"
    assert rows[0]["eff_date"] == "5/1/26"
    assert rows[0]["dis_date"] == "5/31/26"


def test_read_csv_file_stops_at_disclaimer(tmp_path):
    content = (
        _make_csv(_ROW_JFK_LHR)
        + "Data shown in this schedule is for information only.\n"
        + _ROW_CLT_ATL
    )
    _p = _write_csv(tmp_path, content)
    rows = _read_csv_file(_p)
    assert len(rows) == 1


def test_read_csv_file_skips_non_airport_rows(tmp_path):
    content = _make_csv("US OUTBOUND,,,,,,,,,,", _ROW_JFK_LHR)
    _p = _write_csv(tmp_path, content)
    rows = _read_csv_file(_p)
    assert len(rows) == 1


# ---------------------------------------------------------------------------
# _process_row — via _run_update
# ---------------------------------------------------------------------------


def test_active_daily_flight_stored():
    stored = _run_update(_make_csv(_ROW_JFK_LHR), _MON)
    assert len(stored) == 1
    _f = stored[0]
    assert _f["airline_iata"] == "AA"
    assert _f["airline_icao"] == "AAL"
    assert _f["flight_number"] == 100
    assert _f["route"] == "KJFK-EGLL"


def test_wrong_day_of_week_skipped():
    # CLT->CDG Monday only; Wednesday should skip
    stored = _run_update(_make_csv(_ROW_CLT_CDG_MON), _WED)
    assert stored == []


def test_correct_day_of_week_stored():
    stored = _run_update(_make_csv(_ROW_CLT_CDG_MON), _MON)
    assert len(stored) == 1


def test_outside_validity_skipped():
    # DFW->NRT valid only 5/4–5/6; check on 5/8
    stored = _run_update(
        _make_csv(_ROW_DFW_NRT), arrow.get("2026-05-08T12:00:00")
    )
    assert stored == []


def test_within_validity_stored():
    stored = _run_update(_make_csv(_ROW_DFW_NRT), _MON)
    assert len(stored) == 1


def test_nonnumeric_flight_number_skipped():
    stored = _run_update(_make_csv(_ROW_BAD_FNR), _MON)
    assert stored == []


def test_unknown_airport_skipped(caplog):
    with caplog.at_level(logging.WARNING, logger="aa_cargo_data.py"):
        stored = _run_update(_make_csv(_ROW_UNKNOWN_APT), _MON)
    assert stored == []
    assert any("ZZZ" in _r.message for _r in caplog.records)


def test_overnight_arrival_shifted():
    stored = _run_update(_make_csv(_ROW_OVERNIGHT), _MON)
    assert len(stored) == 1
    _f = stored[0]
    assert _f["arrival"] > _f["departure"]
    # Must be more than 1 hour apart (not same-day wrap)
    assert _f["arrival"] - _f["departure"] > 3600


def test_domestic_flight_stored():
    stored = _run_update(_make_csv(_ROW_CLT_ATL), _MON)
    assert len(stored) == 1
    assert stored[0]["route"] == "KCLT-KATL"


def test_flight_id_format():
    stored = _run_update(_make_csv(_ROW_JFK_LHR), _MON)
    _id = stored[0]["_id"]
    assert _id == "AA_100_KJFK-EGLL_20260504"


def test_missing_file_logs_warning(caplog, tmp_path):
    airline = Airline()
    stored = []
    with caplog.at_level(logging.WARNING, logger="aa_cargo_data.py"):
        with patch("aa_cargo_data.PWD", tmp_path):
            with patch.object(
                airline, "update_flight", side_effect=stored.append
            ):
                airline.update_data(_MON)
    assert stored == []
    assert any("not found" in _r.message for _r in caplog.records)


def test_smoke_active_flights():
    airline = Airline()
    utc = arrow.utcnow().timestamp()
    flights = airline.get_active_flights(utc)
    assert isinstance(flights, list)
