#!/usr/bin/env python3
import csv
import arrow
import requests
from airline_info import get_airline_iata
import flight_data_source

URL = "https://siros.anac.gov.br/siros/registros/diario/diario.csv"
_day_labels = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]


class Agency(flight_data_source.FlightDataSource):
    def __init__(self):
        super().__init__("ANAC", category="agencies")

    def update_data(self, utc):
        if utc is None:
            _utc = arrow.utcnow()
        else:
            _utc = arrow.get(utc)
        with requests.Session() as s:
            _response = s.get(URL)
        for _row in csv.DictReader(
            _response.text.splitlines()[1:], delimiter=";"
        ):
            _start_operation = arrow.get(_row["Início Operação"])
            _end_operation = arrow.get(_row["Fim Operação"]).ceil("day")
            if not _start_operation <= _utc <= _end_operation:
                continue
            _frequency = "".join([_row[day] for day in _day_labels])
            if str(_utc.isoweekday()) not in _frequency:
                continue
            _airline_icao = _row["Cód. Empresa"]
            _airline_iata = get_airline_iata(_airline_icao)
            _airline_name = _row["Empresa"]
            _flight_number = int(_row["Nr. Voo"])
            _segment_number = int(_row["Nr. Etapa"])
            _origin = _row["Cód Origem"]
            _destination = _row["Cód Destino"]
            _date = _utc.format("YYYY-MM-DD")
            _departure = arrow.get(f"{_date}T{_row['Partida Prevista']}")
            _arrival = arrow.get(f"{_date}T{_row['Chegada Prevista']}")
            if _departure > _arrival:
                _arrival = _arrival.shift(days=1)
            _route = f"{_origin}-{_destination}"
            _flight = {
                "_id": f"{_airline_iata}_{_flight_number}_{_route}_{_utc.format('YYYYMMDD')}",
                "airline_iata": _airline_iata,
                "airline_icao": _airline_icao,
                "flight_number": _flight_number,
                "route": _route,
                "departure": int(_departure.timestamp()),
                "arrival": int(_arrival.timestamp()),
                "segment_number": _segment_number,
            }
            self.update_flight(_flight)


if __name__ == "__main__":
    agency = Agency()
    # fill the MongoDB with data for tomorrow:
    agency.update_data(arrow.utcnow().shift(days=1))
