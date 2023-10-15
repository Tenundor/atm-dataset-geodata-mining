import argparse
import csv
import os
import pathlib
from datetime import datetime

from dadata import Dadata
from dotenv import load_dotenv
from ratelimit import limits
from tqdm import tqdm

from exceptions import BaseGeodataGettingException


@limits(calls=30, period=1)
def _get_metro_geo_data(dataset_in: list[dict]) -> list[dict]:
    out = dataset_in[:]
    dadata_client = Dadata(DADATA_API_KEY, DADATA_SECRET_KEY)
    for dataset_entry in tqdm(out):
        city_fias_id = dataset_entry["city_fias_id"]
        if not city_fias_id or (city_fias_id not in cities_with_metro_fias_ids):
            continue
        if not dataset_entry["address_rus"]:
            continue
        dadata_response = dadata_client.clean("address", dataset_entry["address_rus"])
        metro_out = dadata_response.get("metro")
        if metro_out is None:
            continue
        for count, metro_entry in enumerate(metro_out, start=1):
            station_name_key = f"metro_station_name_{count}"
            line_name_key = f"metro_line_name_{count}"
            distance_key = f"metro_distance_{count}"
            dataset_entry[station_name_key] = metro_entry["name"]
            dataset_entry[line_name_key] = metro_entry["line"]
            dataset_entry[distance_key] = float(metro_entry["distance"])
    return out


if __name__ == "__main__":
    load_dotenv()
    DADATA_API_KEY = os.getenv("DADATA_API_KEY")
    DADATA_SECRET_KEY = os.getenv("DADATA_SECRET_KEY")

    cities_with_metro_fias_ids = (
        "0c5b2444-70a0-4932-980c-b4dc0d3f02b5"  # Москва
        "c2deb16a-0330-4f05-821f-1d09c93331e6"  # Санкт-Петербург
        "bb035cc3-1dc2-4627-9d25-a1bf2d4b936b"  # Самара
        "555e7d61-d9a7-4ba6-9770-6caa8198c483"  # Нижний Новгород
        "93b3df57-4c89-44df-ac42-96f05e9cd3b9"  # Казань
        "2763c110-cb8b-416a-9dac-ad28a55b4402"  # Екатеринбург
        "8dea00e3-9aab-4d8e-887c-ef2aaa546456"  # Новосибирск
    )

    arg_parser = argparse.ArgumentParser(
        description=(
            "The script receives address data and city fias id specified in csv file. "
            "Using the Dadata service, the script adds data on the nearest metro "
            "stations"
        )
    )
    arg_parser.add_argument("dataset_filename", type=str,
                            help="location of dataset csv file")

    dataset_path = pathlib.Path(arg_parser.parse_args().dataset_filename)
    dataset = []
    with open(dataset_path) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            dataset.append(row)

    if not dataset:
        raise BaseGeodataGettingException("Failed to get data from csv file")
    if len(dataset) > 10000:
        raise BaseGeodataGettingException(
            f"It is impossible to complete {len(dataset)} requests to the "
            f"Dadata service. Because this will exceed the daily limit of "
            f"10 000 free requests."
        )

    dataset_out = _get_metro_geo_data(dataset)

    today = datetime.today().strftime("%d_%m_%y")
    out_filename = f"with_metro_{today}_{dataset_path.name}"
    new_field_names = []
    for number in range(1, 4):
        new_field_names.append(f"metro_station_name_{number}")
        new_field_names.append(f"metro_line_name_{number}")
        new_field_names.append(f"metro_distance_{number}")
    field_names_to_write = list(dataset[0].keys()) + new_field_names

    with open(out_filename, "w") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=field_names_to_write)
        writer.writeheader()
        for entry in dataset_out:
            writer.writerow(entry)
