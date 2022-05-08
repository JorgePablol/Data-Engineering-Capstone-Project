from pandas import DataFrame
import pandas as pd
from typing import NoReturn


def change_column_datatype(df: DataFrame, column: str, datatype: str) -> NoReturn:
    if datatype == "str":
        df[column] = df[column].astype(str)
    elif datatype == "float":
        df[column] = df[column].astype(float)
    elif datatype == "int":
        df[column] = df[column].astype(int)


def create_date_yyyy_mm_dd(df: DataFrame) -> DataFrame:
    df["daily"] = df["daily"].astype(str)
    df["daily"] = df["daily"].apply(lambda x: x.strip())
    df["daily_year"] = df["daily"].copy()
    df["daily_year"] = df["daily_year"].astype(str)
    df["daily_year"] = df["daily_year"].apply(lambda x: x[:4])
    df["daily_year"] = df["daily_year"].astype(int)

    df["daily_month"] = df["daily"].copy()
    df["daily_month"] = df["daily_month"].astype(str)
    df["daily_month"] = df["daily_month"].apply(lambda x: x[5:7])
    df["daily_month"] = df["daily_month"].astype(int)

    df["daily_day"] = df["daily"].copy()
    df["daily_day"] = df["daily_day"].astype(str)
    df["daily_day"] = df["daily_day"].apply(lambda x: x[8:])
    df["daily_day"] = df["daily_day"].astype(int)
    return df


def create_date_yyyy_mm_dd_custom(df: DataFrame, column: str) -> DataFrame:
    df[column] = df[column].astype(str)
    df[column] = df[column].apply(lambda x: x.strip())
    df["execution_year"] = df[column].copy()
    df["execution_year"] = df["execution_year"].astype(str)
    df["execution_year"] = df["execution_year"].apply(lambda x: x[:4])
    df["execution_year"] = df["execution_year"].astype(int)

    df["execution_month"] = df[column].copy()
    df["execution_month"] = df["execution_month"].astype(str)
    df["execution_month"] = df["execution_month"].apply(lambda x: x[5:7])
    df["execution_month"] = df["execution_month"].astype(int)

    df["execution_day"] = df[column].copy()
    df["execution_day"] = df["execution_day"].astype(str)
    df["execution_day"] = df["execution_day"].apply(lambda x: x[8:10])
    df["execution_day"] = df["execution_day"].astype(int)

    df["execution_hour"] = df[column].copy()
    df["execution_hour"] = df["execution_hour"].astype(str)
    df["execution_hour"] = df["execution_hour"].apply(lambda x: x[11:13])
    df["execution_hour"] = df["execution_hour"].astype(int)

    df["execution_minute"] = df[column].copy()
    df["execution_minute"] = df["execution_minute"].astype(str)
    df["execution_minute"] = df["execution_minute"].apply(lambda x: x[14:16])
    df["execution_minute"] = df["execution_minute"].astype(int)
    return df
