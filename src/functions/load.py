from pyspark.sql import DataFrame


def load(df: DataFrame, path: str, table: str, separator: str = ",") -> None:
    df.repartition(1).write.csv(
        f"{path}/{table}",
        header=True,
        sep=separator,
        emptyValue="",
        nullValue="",
        mode="overwrite"
    )
