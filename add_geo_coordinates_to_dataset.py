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
def _get_additional_geo_data_by_fias_ids(fias_ids: set[str]) -> dict:
    out = {}
    for fias_id in tqdm(fias_ids):
        if not fias_id:
            continue
        result = dadata_client.find_by_id("address", fias_id)
        if not result or (geo_data := result[0].get("data")) is None:
            continue
        if not (lat := geo_data["geo_lat"]) or not (lon := geo_data["geo_lon"]):
            continue
        try:
            out[fias_id] = {
                "coordinates": {
                    "lat": float(lat),
                    "lon": float(lon),
                }
            }
        except TypeError:
            continue
        out[fias_id]["city_area"] = geo_data.get("city_area")
        out[fias_id]["city_district"] = {
            "fias_id": geo_data.get("city_district_fias_id"),
            "name_with_type": geo_data.get("city_district_with_type"),
        }

    return out


if __name__ == "__main__":
    load_dotenv()
    DADATA_API_KEY = os.getenv("DADATA_API_KEY")
    DADATA_SECRET_KEY = os.getenv("DADATA_SECRET_KEY")

    arg_parser = argparse.ArgumentParser(
        description=(
            "The script receives `area_fias_id`, `city_fias_id`, `region_fias_id`, "
            "`settlement_fias_id` and `street_fias_id` data specified in csv file. "
            "Using the Dadata service, the script receives the coordinates "
            "of cities, regions and streets and supplements the received dataset "
            "with them. Also trying to get `city_area` and `city_district` using "
            "the `street_fias_id` for cities"
        )
    )
    arg_parser.add_argument("dataset_filename", type=str,
                            help="location of dataset csv file")
    dataset_path = pathlib.Path(arg_parser.parse_args().dataset_filename)
    dataset = []
    areas = set()
    cities = set()
    regions = set()
    streets = set()
    settlements = set()
    with open(dataset_path) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            dataset.append(row)
            areas.add(row["area_fias_id"])
            cities.add(row["city_fias_id"])
            regions.add(row["region_fias_id"])
            streets.add(row["street_fias_id"])
            settlements.add(row["settlement_fias_id"])
    if not dataset:
        raise BaseGeodataGettingException("Failed to get data from csv file")

    dadata_client = Dadata(DADATA_API_KEY, DADATA_SECRET_KEY)
    areas_geo_data = _get_additional_geo_data_by_fias_ids(areas)
    cities_geo_data = _get_additional_geo_data_by_fias_ids(cities)
    regions_geo_data = _get_additional_geo_data_by_fias_ids(regions)
    streets_geo_data = _get_additional_geo_data_by_fias_ids(streets)
    settlement_geo_data = _get_additional_geo_data_by_fias_ids(settlements)
    today = datetime.today().strftime("%d_%m_%y")

    for entry in dataset:
        blank_entry = {"coordinates": {"lat": None, "lon": None},
                       "city_area": None,
                       "city_district": {"fias_id": None, "name_with_type": None}}
        city_entry = cities_geo_data.get(entry["city_fias_id"], blank_entry)
        entry["city_lat"] = city_entry["coordinates"]["lat"]
        entry["city_lon"] = city_entry["coordinates"]["lon"]
        region_entry = regions_geo_data.get(entry["region_fias_id"], blank_entry)
        entry["region_lat"] = region_entry["coordinates"]["lat"]
        entry["region_lon"] = region_entry["coordinates"]["lon"]
        street_entry = streets_geo_data.get(entry["street_fias_id"], blank_entry)
        entry["street_lat"] = street_entry["coordinates"]["lat"]
        entry["street_lon"] = street_entry["coordinates"]["lon"]
        entry["city_area"] = street_entry["city_area"]
        entry["city_district_fias_id"] = street_entry["city_district"]["fias_id"]
        entry["city_district_with_type"] = street_entry["city_district"]["name_with_type"]
        area_entry = areas_geo_data.get(entry["area_fias_id"], blank_entry)
        entry["area_lat"] = area_entry["coordinates"]["lat"]
        entry["area_lon"] = area_entry["coordinates"]["lon"]
        settlement_entry = settlement_geo_data.get(entry["settlement_fias_id"], blank_entry)
        entry["settlement_lat"] = settlement_entry["coordinates"]["lat"]
        entry["settlement_lon"] = settlement_entry["coordinates"]["lon"]

    out_filename = f"with_coord_{today}_{dataset_path.name}"
    with open(out_filename, "w") as csvfile:
        field_names_to_write = tuple(dataset[0].keys())
        writer = csv.DictWriter(csvfile, fieldnames=field_names_to_write)
        writer.writeheader()
        for entry in dataset:
            writer.writerow(entry)
