import argparse
import csv
import os
import pathlib
import time

from dadata import Dadata
from dotenv import load_dotenv
from tqdm import tqdm


class BaseGeodataGettingException(Exception):
    ...


if __name__ == "__main__":
    load_dotenv()
    DADATA_API_KEY = os.getenv("DADATA_API_KEY")
    DADATA_SECRET_KEY = os.getenv("DADATA_SECRET_KEY")

    arg_parser = argparse.ArgumentParser(
        description=(
            "The script receives ATM coordinates from the specified csv file. "
            "Using the Dadata service, it get the city, district and street "
            "on which it is located for each ATM. Returns a file with the "
            "original dataset, which is supplemented with columns `city`, "
            "`district`, `street` containing names in Russian. The script only "
            "works for coordinates in Russia"
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
    dadata_client = Dadata(DADATA_API_KEY, DADATA_SECRET_KEY)
    field_names_to_add = (
        "city_with_type",
        "city_fias_id",
        "city_district_with_type",
        "city_district_fias_id",
        "federal_district",
        "capital_marker",
        "fias_id",
        "fias_level",
        "metro",
        "region_with_type",
        "region_fias_id",
        "settlement_with_type",
        "settlement_fias_id",
        "street_with_type",
        "street_fias_id",
    )
    for entry in tqdm(dataset):
        if not entry["lat"] or not entry["long"]:
            for key in field_names_to_add:
                entry[key] = None
            continue
        result = dadata_client.geolocate(name="address", lat=entry["lat"],
                                         lon=entry["long"])
        if not result or result[0].get("data") is None:
            for key in field_names_to_add:
                entry[key] = None
            continue
        first_result = result[0]["data"]
        for key in field_names_to_add:
            entry[key] = first_result[key]
        time.sleep(0.033)  # A primitive solution to protect against
        # exceeding the number of API calls per second (max 30 per second).
        # Because asynchrony and multithreading are not expected for this script

    with open(f"updated_{dataset_path.name}", "w") as csvfile:
        field_names_to_write = tuple(dataset[0].keys())
        writer = csv.DictWriter(csvfile, fieldnames=field_names_to_write)
        writer.writeheader()
        for entry in dataset:
            writer.writerow(entry)
