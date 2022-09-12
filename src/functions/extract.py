from .config import DATE_FORMAT
from .utils import convert_convert_columns_to_snake_case
from pyspark.sql import functions as f
from pyspark.sql.types import StructType
from pyspark.sql import SparkSession, DataFrame


@convert_convert_columns_to_snake_case
def extract_reservations(
        path: str,
        schema_of_data: StructType,
        spark: SparkSession,
        separator: str = ","
) -> DataFrame:
    return (
        spark.read.csv(
            path,
            sep=separator,
            header=False,
            schema=schema_of_data
        )
        .withColumn("file_path", f.input_file_name())
        .filter(f.col("index").isNotNull())
        .withColumn("DateCheckIn", f.date_format(
            f.to_timestamp(f.col("DateCheckIn")),
            DATE_FORMAT
        ))
        .withColumn("DateCheckOut", f.date_format(
            f.to_timestamp(f.col("DateCheckOut")),
            DATE_FORMAT
        ))
        .withColumn("DateBooked", f.date_format(
            f.to_timestamp(f.col("DateBooked")),
            DATE_FORMAT
        ))
    )


@convert_convert_columns_to_snake_case
def extract_location_details(path: str, spark: SparkSession) -> DataFrame:
    return spark.read.json(path).cache()