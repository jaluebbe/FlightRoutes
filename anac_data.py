#!/usr/bin/env python3
import csv
import logging
import pathlib
import arrow
import requests
from airline_info import get_airline_iata
import flight_data_source

URL = "https://siros.anac.gov.br/siros/registros/diario/diario.csv"
_day_labels = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]

logger = logging.getLogger(pathlib.Path(__file__).name)


def _fetch_schedule() -> list[dict]:
    with requests.Session() as _session:
        _response = _session.get(URL)
        _response.raise_for_status()
        _response.encoding = "utf-8"
    # First line is a metadata comment ("Importante: Horários em UTC"),
    # not part of the CSV. DictReader handles the header on the second line.
    return list(csv.DictReader(_response.text.splitlines()[1:], delimiter=";"))


class Agency(flight_data_source.FlightDataSource):
    def __init__(self):
        super().__init__("ANAC", category="agencies")
        self.allow_alphanumerical_candidates = False

    def update_data(self, utc=None) -> None:
        if utc is None:
            _utc = arrow.utcnow()
        else:
            _utc = arrow.get(utc)

        rows = _fetch_schedule()
        flights = []
        _unknown_airlines: dict[str, str] = {}

        for _row in rows:
            _start_operation = arrow.get(_row["Início Operação"])
            _end_operation = arrow.get(_row["Fim Operação"]).ceil("day")
            if not _start_operation <= _utc <= _end_operation:
                continue
            _frequency = "".join([_row[_day] for _day in _day_labels])
            if str(_utc.isoweekday()) not in _frequency:
                continue
            _airline_icao = _row["Cód. Empresa"]
            _airline_iata = get_airline_iata(_airline_icao)
            if _airline_iata is None:
                _unknown_airlines[_airline_icao] = _row["Empresa"]
                continue
            if not _row["Nr. Voo"].isdigit():
                continue
            _flight_number = int(_row["Nr. Voo"])
            _segment_number = int(_row["Nr. Etapa"])
            _origin = _row["Cód Origem"]
            _destination = _row["Cód Destino"]
            _date = _utc.format("YYYY-MM-DD")
            # Times are already UTC as stated in the file header.
            _departure = arrow.get(f"{_date}T{_row['Partida Prevista']}")
            _arrival = arrow.get(f"{_date}T{_row['Chegada Prevista']}")
            if _departure > _arrival:
                _arrival = _arrival.shift(days=1)
            _route = f"{_origin}-{_destination}"
            flights.append(
                {
                    "_id": f"{_airline_iata}_{_flight_number}_{_route}_{_utc.format('YYYYMMDD')}",
                    "airline_iata": _airline_iata,
                    "airline_icao": _airline_icao,
                    "flight_number": _flight_number,
                    "route": _route,
                    "departure": int(_departure.timestamp()),
                    "arrival": int(_arrival.timestamp()),
                    "segment_number": _segment_number,
                }
            )

        # Filter out segment-0 records for flights that have higher-numbered
        # segments, consistent with lh_cargo_data.py handling.
        _multi_segment = {
            (_flight["airline_iata"], _flight["flight_number"])
            for _flight in flights
            if _flight["segment_number"] > 0
        }
        for _flight in flights:
            if (
                _flight["airline_iata"],
                _flight["flight_number"],
            ) in _multi_segment and _flight["segment_number"] == 0:
                continue
            self.update_flight(_flight)

        for _icao, _name in sorted(_unknown_airlines.items()):
            logger.warning(f"unknown airline ICAO: {_icao} ({_name}), skipping")
        logger.info(
            f"ANAC: processed {len(flights)} flights for {_utc.format('YYYY-MM-DD')}"
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    agency = Agency()
    agency.update_data(arrow.utcnow().shift(days=1))
