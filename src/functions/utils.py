import functools
from re import sub
from typing import Callable, List

from pyspark.sql import DataFrame
import argparse


def convert_convert_columns_to_snake_case(func: Callable):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        df: DataFrame = func(*args, **kwargs)
        if not df:
            return None
        for column_name in df.columns:
            column_in_snake_case = '_'.join(
                sub(r"(\s|_|-)+", " ",
                    sub(r"[A-Z]{2,}(?=[A-Z][a-z]+[0-9]*|\b)|[A-Z]?[a-z]+[0-9]*|[A-Z]|[0-9]+",
                        lambda mo: ' ' + mo.group(0).lower(), column_name)).split()
            )
            df = df.withColumnRenamed(
                column_name,
                column_in_snake_case
            )
        return df
    return wrapper


def parse_args(args: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "reservations_path",
        type=str
    )

    parser.add_argument(
        "details_path",
        type=str
    )

    parser.add_argument(
        "output_path",
        type=str
    )

    parsed_args = parser.parse_args(args)

    return parsed_args
