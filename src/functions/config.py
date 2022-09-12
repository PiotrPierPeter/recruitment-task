from pyspark.sql.types import StringType, StructType


DATE_FORMAT = "yyyy-MM-dd"

schema_reservations = StructType() \
    .add("index", StringType(), True) \
    .add("booking_reference", StringType(), True) \
    .add("DateBooked", StringType(), True) \
    .add("DateCheckIn", StringType(), True) \
    .add("DateCheckOut", StringType(), True) \
    .add("PriceEUR", StringType(), True) \
    .add("file_path", StringType(), True)
