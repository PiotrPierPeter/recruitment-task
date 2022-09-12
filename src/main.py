import sys

from functions.config import schema_reservations
from functions.extract import extract_reservations, extract_location_details
from functions.load import load
from functions.spark import get_spark
from functions.transform import extract_hotel_id_from_file_path_and_drop, filter_proper_records, find_duplicates, \
    find_errors_in_reservations, get_reservation_table, filter_corrupt_records, get_details_table
from functions.utils import parse_args


def main(
    reservations_path: str,
    details_path: str,
    output_path: str
):
    spark = get_spark()
    df_reservations = extract_reservations(reservations_path, schema_reservations, spark)
    df_reservations = extract_hotel_id_from_file_path_and_drop(df_reservations)
    df_reservations_filtered = filter_proper_records(df_reservations)
    if (df_reservations_duplicates := find_duplicates(df_reservations)).count() > 0:
        print("Duplicates found")
        df_errors = find_errors_in_reservations(df_reservations_duplicates)  # TODO: Add loading to error table
    else:
        df_errors = find_errors_in_reservations(df_reservations)

    df_reservations_table = get_reservation_table(df_reservations_filtered)  # TODO: Add loading to error table

    df_details = extract_location_details(details_path, spark)
    corrupted_details = filter_corrupt_records(df_details)  # TODO: Add loading to error table
    df_details = df_details.drop("corrupt_record")

    df_details_table = get_details_table(df_details)

    load(df_details_table, output_path, "details")

    load(df_reservations_table, output_path, "reservations")


if __name__ == "__main__":
    main(**vars(parse_args(sys.argv[1:])))
