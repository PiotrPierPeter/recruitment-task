from pyspark.sql import functions as f
from pyspark.sql.types import StringType, StructType


@f.udf(returnType=StringType())
def get_hotel_id(value: str):
    return value.split("/")[-1].split(".")[0]


@f.udf(returnType=StringType())
def get_region(value: str):
    return f"region: {value}"