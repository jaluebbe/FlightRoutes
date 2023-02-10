# FlightRoutes
The purpose of this repository is to connect aircraft positions observed in [OpenSky Network](https://opensky-network.org/) to origin, destination and flight number.
The focus is on public airline flights only.
Arrival and/or departure times may be utilised for the matching process but are not redistributed.

## Data sources
Before using data from any of the following sources please review their terms of use.

### OpenSky Network
All currently detected aircraft positions are available via the [Python API](https://openskynetwork.github.io/opensky-api/python.html#opensky-python-api) (registration recommended).

An estimate for flight routes is available via the 
[REST API flights by aircraft](https://openskynetwork.github.io/opensky-api/rest.html#flights-by-aircraft) (registration required).

[Aircraft database](https://opensky-network.org/aircraft-database) (no registration required for read-access).

The [Historical database](https://opensky-network.org/data/impala) is available for researchers on request.

### OurAirports
[World wide airport information](https://ourairports.com/data/) as public domain data.

### Virtual Radar Server
[Virtual Radar Server](http://www.virtualradarserver.co.uk/)'s [standing data](https://github.com/vradarserver/standing-data) includes crowd sourced data of airlines, aircraft, airports and flight routes.

### Münster Osnabrück International Airport (FMO/EDDG)
[Arrivals and Departures in XML format](https://opendata.stadt-muenster.de/dataset/flugplandaten-des-flughafen-m%C3%BCnsterosnabr%C3%BCck-fmo/resource/79054aed-5eaf-4aba-9239)
 (no registration required).

### Luxembourg Airport (LUX/ELLX)
[Arrivals and Departures in JSON format](https://data.public.lu/en/datasets/arrivees-et-departs-de-laeroport-de-luxembourg/) (no registration required).
Repeated calls from the same IP may get blocked by the firewall. Could be circumvented by using a random https proxy for each request.
Airports names but not their respective IATA codes are available.

### Hamburg Airport (HAM/EDDH)
[Developer portal](https://portal.api.hamburg-airport.de/) offering arrival and departure data in JSON format (registration required).

### Norwegian airports operated by Avinor
[Arrivals and departures in XML format](https://avinor.no/en/corporate/services/flydata/flydata-i-xml-format)  (no registration required).

### Lufthansa Cargo schedule
[Flight schedules in XML, XLSX, CSV and PDF format](https://lufthansa-cargo.com/de/network/schedule-routings)
(no registration required).

### Brazilian ANAC
[Flight schedules in CSV format](https://siros.anac.gov.br/siros/registros/diario/diario.csv) (no registration required).

## Installation
```
git clone git@github.com:openskynetwork/opensky-api.git
pip install -e opensky-api/python
```
