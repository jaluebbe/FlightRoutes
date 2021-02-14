import json
import pandas as pd

url = "https://opensky-network.org/datasets/metadata/aircraftDatabase.csv"
df = pd.read_csv(url)
df.dropna(subset=["icao24", "registration", "operatoricao"], inplace=True)
df.registration = df.registration.apply(lambda r: "".join(r.split("-")))
df.set_index("icao24", inplace=True)
icao24_to_registration = df.registration.to_dict()
json.dump(icao24_to_registration, open("icao24_to_registration.json", "w"))
