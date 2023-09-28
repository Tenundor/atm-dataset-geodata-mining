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
def _get_coordinates_by_fias_ids(fias_ids: set[str]) -> dict:
    coordinates_by_fias = {}
    for fias_id in tqdm(fias_ids):
        if not fias_id:
            continue
        result = dadata_client.find_by_id("address", fias_id)
        if not result or (geo_data := result[0].get("data")) is None:
            continue
        if not (lat := geo_data["geo_lat"]) or not (lon := geo_data["geo_lon"]):
            continue
        try:
            coordinates_by_fias[fias_id] = {
                "lat": float(lat),
                "lon": float(lon),
            }
        except TypeError:
            continue
    return coordinates_by_fias


if __name__ == "__main__":
    load_dotenv()
    DADATA_API_KEY = os.getenv("DADATA_API_KEY")
    DADATA_SECRET_KEY = os.getenv("DADATA_SECRET_KEY")

    arg_parser = argparse.ArgumentParser(
        description=(
            "The script receives `city_fias_id`, `region_fias_id` and "
            "`street_fias_id` data specified in csv file. "
            "Using the Dadata service, the script receives the coordinates "
            "of cities, regions and streets and supplements the received dataset "
            "with them"
        )
    )
    arg_parser.add_argument("dataset_filename", type=str,
                            help="location of dataset csv file")
    dataset_path = pathlib.Path(arg_parser.parse_args().dataset_filename)
    dataset = []
    cities = set()
    regions = set()
    streets = set()
    with open(dataset_path) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            dataset.append(row)
            cities.add(row["city_fias_id"])
            regions.add(row["region_fias_id"])
            streets.add(row["street_fias_id"])

    if not dataset:
        raise BaseGeodataGettingException("Failed to get data from csv file")

    dadata_client = Dadata(DADATA_API_KEY, DADATA_SECRET_KEY)
    cities_coordinates = _get_coordinates_by_fias_ids(cities)
    regions_coordinates = _get_coordinates_by_fias_ids(regions)
    streets_coordinates = _get_coordinates_by_fias_ids(streets)
    today = datetime.today().strftime("%d_%m_%y")

    for entry in dataset:
        blank_coord = {"lat": None, "lon": None}
        city_coord = cities_coordinates.get(entry["city_fias_id"], blank_coord)
        entry["city_lat"] = city_coord["lat"]
        entry["city_lon"] = city_coord["lon"]
        region_coord = regions_coordinates.get(entry["region_fias_id"], blank_coord)
        entry["region_lat"] = region_coord["lat"]
        entry["region_lon"] = region_coord["lon"]
        street_coord = streets_coordinates.get(entry["street_fias_id"], blank_coord)
        entry["street_lat"] = street_coord["lat"]
        entry["street_lon"] = street_coord["lon"]

    out_filename = f"with_coord_{today}_{dataset_path.name}"
    with open(out_filename, "w") as csvfile:
        field_names_to_write = tuple(dataset[0].keys())
        writer = csv.DictWriter(csvfile, fieldnames=field_names_to_write)
        writer.writeheader()
        for entry in dataset:
            writer.writerow(entry)
