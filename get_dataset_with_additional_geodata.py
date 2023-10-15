import argparse
import csv
import os
import pathlib

from dadata import Dadata
from dotenv import load_dotenv
from ratelimit import limits
from tqdm import tqdm

from exceptions import BaseGeodataGettingException


@limits(calls=30, period=1)
def _get_addresses_by_coordinates(dataset_in: list[dict]) -> list[dict]:
    field_names_to_add = (
        "area_fias_id",
        "area_with_type",
        "city_with_type",
        "city_fias_id",
        "federal_district",
        "capital_marker",
        "fias_id",
        "fias_level",
        "region_with_type",
        "region_fias_id",
        "settlement_with_type",
        "settlement_fias_id",
        "street_with_type",
        "street_fias_id",
    )
    dadata_client = Dadata(DADATA_API_KEY, DADATA_SECRET_KEY)
    out = dataset_in[:]
    for dataset_entry in tqdm(out):
        if not dataset_entry["lat"] or not dataset_entry["long"]:
            for key in field_names_to_add:
                dataset_entry[key] = None
            continue
        dadata_response = dadata_client.geolocate(
            name="address",
            lat=dataset_entry["lat"],
            lon=dataset_entry["long"]
        )
        if not dadata_response or dadata_response[0].get("data") is None:
            for key in field_names_to_add:
                dataset_entry[key] = None
            continue
        first_result = dadata_response[0]["data"]
        for key in field_names_to_add:
            dataset_entry[key] = first_result[key]
    return out


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
    arg_parser.add_argument("--csv_dialect", type=str, default="unix",
                            help="Dialect of the csv file. The default is unix, "
                                 "optionally you can specify 'excel' or 'excel_tab'")
    csv_dialects = ["excel", "excel_tab", "unix"]
    dataset_path = pathlib.Path(arg_parser.parse_args().dataset_filename)
    csv_dialect = arg_parser.parse_args().csv_dialect
    if csv_dialect not in csv_dialects:
        valid_dialects = ", '".join(csv_dialects)
        raise BaseGeodataGettingException(
            "The 'csv_dialect' parameter value is incorrect. "
            f"Valid values: '{valid_dialects}'"
        )

    dataset = []
    with open(dataset_path) as csvfile:
        reader = csv.DictReader(csvfile, dialect=csv_dialect)
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
    dataset_out = _get_addresses_by_coordinates(dataset)
    with open(f"updated_{dataset_path.name}", "w") as csvfile:
        field_names_to_write = tuple(dataset_out[0].keys())
        writer = csv.DictWriter(csvfile, fieldnames=field_names_to_write)
        writer.writeheader()
        for entry in dataset_out:
            writer.writerow(entry)
