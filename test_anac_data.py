import logging
import arrow
import pytest
from unittest.mock import patch
from anac_data import Agency, _fetch_schedule

# ---------------------------------------------------------------------------
# Sample CSV rows derived from the real ANAC feed structure.
# Times are UTC as stated in the file header.
# ---------------------------------------------------------------------------

_HEADER = (
    "Cód. Empresa;Empresa;Nr. Voo;Equip.;"
    "Seg;Ter;Qua;Qui;Sex;Sáb;Dom;"
    "Qtde Assentos;Nº SIROS;Data Registro;"
    "Início Operação;Fim Operação;"
    "Natureza Operação;Tipo Serviço;Objeto Transporte;"
    "Nr. Etapa;Cód Origem;Arpt Origem;Cód Destino;Arpt Destino;"
    "Partida Prevista;Chegada Prevista;Codeshare"
)

# AAL flight every day, GRU->MIA, single segment
_ROW_AAL_GRU_MIA = (
    "AAL;AMERICAN AIRLINES, INC.;0906;B788;"
    "1;2;3;4;5;6;7;"
    "295;AAL-001;13/01/2026 14:59:45;"
    "2026-03-31;2026-04-30;"
    "INTERNACIONAL;REGULAR DE PASSAGEIROS;PASSAGEIROS;"
    "1;SBGR;GUARULHOS - SP;KMIA;MIAMI INTERNATIONAL;"
    "02:15;10:45;"
)

# GLO flight Mon only, segment 1 of multi-segment
_ROW_GLO_SEG1 = (
    "GLO;GOL LINHAS AEREAS S.A.;1234;B738;"
    "1;0;0;0;0;0;0;"
    "180;GLO-001;01/01/2026 10:00:00;"
    "2026-03-01;2026-04-30;"
    "DOMESTICA;REGULAR DE PASSAGEIROS;PASSAGEIROS;"
    "1;SBGR;GUARULHOS - SP;SBSV;SALVADOR - BA;"
    "06:00;08:00;"
)

# GLO flight Mon only, segment 0 of multi-segment (should be filtered out)
_ROW_GLO_SEG0 = (
    "GLO;GOL LINHAS AEREAS S.A.;1234;B738;"
    "1;0;0;0;0;0;0;"
    "180;GLO-002;01/01/2026 10:00:00;"
    "2026-03-01;2026-04-30;"
    "DOMESTICA;REGULAR DE PASSAGEIROS;PASSAGEIROS;"
    "0;SBGR;GUARULHOS - SP;SBSV;SALVADOR - BA;"
    "06:00;08:00;"
)

# Flight with unknown airline ICAO
_ROW_UNKNOWN_AIRLINE = (
    "ZZZ;UNKNOWN AIRLINE;0001;B738;"
    "1;2;3;4;5;6;7;"
    "100;ZZZ-001;01/01/2026 10:00:00;"
    "2026-03-01;2026-04-30;"
    "DOMESTICA;REGULAR DE PASSAGEIROS;PASSAGEIROS;"
    "1;SBGR;GUARULHOS - SP;SBBR;BRASILIA - DF;"
    "08:00;09:30;"
)

# Flight outside operation period
_ROW_EXPIRED = (
    "AAL;AMERICAN AIRLINES, INC.;0999;B788;"
    "1;2;3;4;5;6;7;"
    "295;AAL-999;01/01/2026 00:00:00;"
    "2026-01-01;2026-01-31;"
    "INTERNACIONAL;REGULAR DE PASSAGEIROS;PASSAGEIROS;"
    "1;SBGR;GUARULHOS - SP;KMIA;MIAMI INTERNATIONAL;"
    "02:15;10:45;"
)

# Flight with non-numeric flight number
_ROW_NONNUMERIC_FNR = (
    "AAL;AMERICAN AIRLINES, INC.;ABC;B788;"
    "1;2;3;4;5;6;7;"
    "295;AAL-ABC;01/01/2026 00:00:00;"
    "2026-03-01;2026-04-30;"
    "INTERNACIONAL;REGULAR DE PASSAGEIROS;PASSAGEIROS;"
    "1;SBGR;GUARULHOS - SP;KMIA;MIAMI INTERNATIONAL;"
    "02:15;10:45;"
)

# Overnight flight: departure 23:00, arrival 01:30 (next day)
_ROW_OVERNIGHT = (
    "AAL;AMERICAN AIRLINES, INC.;0123;B788;"
    "1;2;3;4;5;6;7;"
    "295;AAL-123;01/01/2026 00:00:00;"
    "2026-03-01;2026-04-30;"
    "INTERNACIONAL;REGULAR DE PASSAGEIROS;PASSAGEIROS;"
    "1;KMIA;MIAMI INTERNATIONAL;SBGR;GUARULHOS - SP;"
    "23:00;01:30;"
)


def _make_csv(*rows: str) -> str:
    """Build a mock CSV response with the metadata header and data rows."""
    lines = ["Importante: Horários em UTC", _HEADER] + list(rows)
    return "\n".join(lines)


def _run_update(csv_text: str, utc: arrow.Arrow) -> list[dict]:
    """Run Agency.update_data with a mocked _fetch_schedule and capture stored flights."""
    import csv as csv_module

    rows = list(csv_module.DictReader(csv_text.splitlines()[1:], delimiter=";"))
    stored = []
    agency = Agency()
    with patch("anac_data._fetch_schedule", return_value=rows):
        with patch.object(agency, "update_flight", side_effect=stored.append):
            agency.update_data(utc)
    return stored


# Monday 2026-04-06 — isoweekday() == 1
_MONDAY = arrow.get("2026-04-06T12:00:00")
# Wednesday — isoweekday() == 3
_WEDNESDAY = arrow.get("2026-04-02T12:00:00")


# ---------------------------------------------------------------------------
# _fetch_schedule — unit test the metadata-line skip and encoding
# ---------------------------------------------------------------------------


def test_fetch_schedule_skips_metadata_line():
    mock_text = _make_csv(_ROW_AAL_GRU_MIA)
    mock_response = type(
        "R",
        (),
        {
            "text": mock_text,
            "raise_for_status": lambda self: None,
            "encoding": None,
        },
    )()

    import csv as csv_module

    rows = list(
        csv_module.DictReader(mock_text.splitlines()[1:], delimiter=";")
    )
    assert rows[0]["Cód. Empresa"] == "AAL"
    assert "Início Operação" in rows[0]


# ---------------------------------------------------------------------------
# Flight filtering
# ---------------------------------------------------------------------------


def test_active_flight_is_stored():
    stored = _run_update(_make_csv(_ROW_AAL_GRU_MIA), _MONDAY)
    assert len(stored) == 1
    _f = stored[0]
    assert _f["airline_iata"] == "AA"
    assert _f["airline_icao"] == "AAL"
    assert _f["flight_number"] == 906
    assert _f["route"] == "SBGR-KMIA"


def test_flight_outside_operation_period_skipped():
    stored = _run_update(_make_csv(_ROW_EXPIRED), _MONDAY)
    assert stored == []


def test_flight_wrong_day_of_week_skipped():
    """_ROW_GLO_SEG1 operates on Monday only; Wednesday should skip it."""
    stored = _run_update(_make_csv(_ROW_GLO_SEG1), _WEDNESDAY)
    assert stored == []


def test_flight_correct_day_of_week_stored():
    stored = _run_update(_make_csv(_ROW_GLO_SEG1), _MONDAY)
    assert len(stored) == 1


def test_unknown_airline_skipped(caplog):
    with caplog.at_level(logging.WARNING, logger="anac_data.py"):
        stored = _run_update(_make_csv(_ROW_UNKNOWN_AIRLINE), _MONDAY)
    assert stored == []
    assert any("ZZZ" in _r.message for _r in caplog.records)


def test_nonnumeric_flight_number_skipped():
    stored = _run_update(_make_csv(_ROW_NONNUMERIC_FNR), _MONDAY)
    assert stored == []


# ---------------------------------------------------------------------------
# Multi-segment filtering
# ---------------------------------------------------------------------------


def test_segment_zero_filtered_when_higher_segment_exists():
    """When segment 1 exists for a flight, segment 0 should not be stored."""
    stored = _run_update(_make_csv(_ROW_GLO_SEG0, _ROW_GLO_SEG1), _MONDAY)
    assert len(stored) == 1
    assert stored[0]["segment_number"] == 1


def test_segment_zero_stored_when_only_segment():
    """A segment-0 flight with no other segments should be stored."""
    stored = _run_update(_make_csv(_ROW_GLO_SEG0), _MONDAY)
    assert len(stored) == 1
    assert stored[0]["segment_number"] == 0


# ---------------------------------------------------------------------------
# Timestamps
# ---------------------------------------------------------------------------


def test_departure_and_arrival_timestamps_are_utc():
    stored = _run_update(_make_csv(_ROW_AAL_GRU_MIA), _MONDAY)
    _f = stored[0]
    _dep = arrow.get(_f["departure"])
    _arr = arrow.get(_f["arrival"])
    assert _dep.format("HH:mm") == "02:15"
    assert _arr.format("HH:mm") == "10:45"
    assert _arr > _dep


def test_overnight_flight_arrival_shifted_to_next_day():
    stored = _run_update(_make_csv(_ROW_OVERNIGHT), _MONDAY)
    assert len(stored) == 1
    _f = stored[0]
    _dep = arrow.get(_f["departure"])
    _arr = arrow.get(_f["arrival"])
    assert _arr > _dep
    assert (_arr - _dep).total_seconds() > 0


# ---------------------------------------------------------------------------
# _id format
# ---------------------------------------------------------------------------


def test_flight_id_format():
    stored = _run_update(_make_csv(_ROW_AAL_GRU_MIA), _MONDAY)
    _id = stored[0]["_id"]
    parts = _id.split("_")
    assert parts[0] == "AA"
    assert parts[1] == "906"
    assert parts[2] == "SBGR-KMIA"
    assert parts[3] == "20260406"


# ---------------------------------------------------------------------------
# Smoke test against live MongoDB (requires update_data to have been run)
# ---------------------------------------------------------------------------


def test_smoke_active_flights():
    agency = Agency()
    utc = arrow.utcnow().timestamp()
    flights = agency.get_active_flights(utc)
    assert isinstance(flights, list)
    for _flight in flights:
        assert "airline_iata" in _flight
        assert "airline_icao" in _flight
        assert "flight_number" in _flight
        assert "route" in _flight
