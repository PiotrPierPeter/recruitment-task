from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as f

from .udf import get_hotel_id, get_region


def extract_hotel_id_from_file_path_and_drop(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "hotel_id",
        get_hotel_id(f.col("file_path"))  # type: ignore
    ).drop(f.col("file_path"))


def find_duplicates(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn(
            "row_number",
            f.row_number().over(
                Window.partitionBy(["booking_reference", "hotel_id"])
                .orderBy(["booking_reference", "hotel_id"])
            )
        )
        .filter(f.col("row_number") != 1)
    )


def find_errors_in_reservations(df: DataFrame) -> DataFrame:
    errors_df = df.select(
        f.col("booking_reference"),
        f.when(
            f.col("date_booked") >= f.col("date_check_in"),
            f.lit("date_booked equal or greater than date_check_in")
        ).otherwise(f.lit(None)).alias("error_date_booked_date_check_in"),
        f.when(
            f.col("date_check_in") >= f.col("date_check_out"),
            f.lit("date_check_in equal or greater than date_check_out")
        ).otherwise(f.lit(None)).alias("error_date_check_in_date_check_out"),
        f.when(
            f.col("date_booked").isNull(),
            f.lit("Null value in date_booked column")
        ).otherwise(f.lit(None)).alias("null_in_date_booked"),
        f.when(
            f.col("date_check_in").isNull(),
            f.lit("Null value in date_check_in column")
        ).otherwise(f.lit(None)).alias("null_in_date_check_in"),
        f.when(
            f.col("booking_reference").isNull(),
            f.lit("Null value in booking_reference column")
        ).otherwise(f.lit(None)).alias("null_in_booking_reference"),
        f.when(
            f.col("date_check_out").isNull(),
            f.lit("Null value in date_check_out column")
        ).otherwise(f.lit(None)).alias("null_in_date_check_out"),
        f.when(
            f.col("hotel_id").isNull(),
            f.lit("Null value in hotel_id column")
        ).otherwise(f.lit(None)).alias("null_in_hotel_id"),
        f.col("hotel_id"),
    ).filter(
        f.col("error_date_booked_date_check_in").isNotNull() |
        f.col("error_date_check_in_date_check_out").isNotNull() |
        f.col("null_in_date_booked").isNotNull() |
        f.col("null_in_date_check_in").isNotNull() |
        f.col("null_in_booking_reference").isNotNull() |
        f.col("null_in_date_check_out").isNotNull() |
        f.col("null_in_hotel_id").isNotNull()
    )

    return errors_df.select(
        f.col("booking_reference"),
        f.concat_ws("; ", *(errors_df.columns[:-1])).alias("errors"),
        f.col("hotel_id"),
    )


def filter_proper_records(df: DataFrame) -> DataFrame:
    return df.filter(
        f.col("booking_reference").isNotNull() &
        f.col("date_booked").isNotNull() &
        f.col("date_check_in").isNotNull() &
        f.col("date_check_out").isNotNull() &
        f.col("hotel_id").isNotNull() &
        (f.col("date_booked") < f.col("date_check_in")) &
        (f.col("date_check_in") < f.col("date_check_out"))
    ).dropDuplicates(["booking_reference", "hotel_id"])


def filter_corrupt_records(df: DataFrame) -> DataFrame:
    return df.select("corrupt_record").filter(f.col("corrupt_record").isNotNull())


def get_reservation_table(df_reservations_filtered: DataFrame) -> DataFrame:
    df_reservations_agg = df_reservations_filtered.groupBy(
        f.col("hotel_id").alias("hotel_id_reservation_agg")
    ).agg(
        f.count(f.col("booking_reference")).alias("reservation_amount"),
        f.format_number(f.avg(f.col("price_eur")), 2).alias("average_price")
    )

    return df_reservations_filtered.join(
        df_reservations_agg,
        on=df_reservations_filtered["hotel_id"] == df_reservations_agg["hotel_id_reservation_agg"],
        how="left"
    ).select(
        f.col("date_booked"),
        f.col("date_check_in"),
        f.datediff(f.col("date_check_out"), f.col("date_check_in")).alias("length_of_stay"),
        f.col("reservation_amount"),
        f.col("average_price")
    )


def get_details_table(df_details: DataFrame):
    df_brand = df_details.groupby(
        f.col("brand").alias("brand_name")
    ).agg(
        f.count(f.col("brand")).alias("brand_amount")
    )
    return df_details.withColumn(
        "total_trips",
        (f.col("trip_types").getItem(0)["value"] +
         f.col("trip_types").getItem(1)["value"] +
         f.col("trip_types").getItem(2)["value"] +
         f.col("trip_types").getItem(3)["value"] +
         f.col("trip_types").getItem(4)["value"])
    ).join(
        df_brand,
        on=df_details["brand"] == df_brand["brand_name"],
        how="left"
    ).select(
        f.format_number(
            (100 * f.col("trip_types").getItem(0)["value"] / f.col("total_trips")),
            2
        ).alias("business_trips_percent"),
        f.format_number(
            (100 * f.col("trip_types").getItem(1)["value"] / f.col("total_trips")),
            2
        ).alias("couples_trips_percent"),
        f.format_number(
            (100 * f.col("trip_types").getItem(2)["value"] / f.col("total_trips")),
            2
        ).alias("solo_trips_percent"),
        f.format_number(
            (100 * f.col("trip_types").getItem(3)["value"] / f.col("total_trips")),
            2).alias("family_trips_percent"),
        f.format_number(
            (100 * f.col("trip_types").getItem(4)["value"] / f.col("total_trips")),
            2
        ).alias("friends_trips_percent"),
        f.col("location_id"),
        f.size(f.col("amenities")).alias("amount_of_amenities"),
        f.concat_ws(", ", f.col("styles")).alias("hotel_style"),
        get_region(f.col("ancestors").getItem(1)["location_id"]).alias("region"),  # type: ignore
        f.col("rating"),
        f.when(
            f.col("brand").isNotNull(),
            f.col("brand_amount") - 1
        ).otherwise(
            f.lit("No data")
        ).alias("hotel_with_same_brand"),
    )
