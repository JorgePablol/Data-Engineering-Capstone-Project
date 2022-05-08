from typing import NoReturn

from pandas import DataFrame


def change_column_datatype(df: DataFrame, column: str, datatype: str) -> NoReturn:
    """Changes the column variable type.

    @param df: the dataframe that has the column.
    @param column: the column to be modified.
    @param datatype: the datatype that the column will be converted to.
    """
    if datatype == "str":
        df[column] = df[column].astype(str)
    elif datatype == "float":
        df[column] = df[column].astype(float)
    elif datatype == "int":
        df[column] = df[column].astype(int)


def create_date_yyyy_mm_dd(df: DataFrame) -> DataFrame:
    """Slices the daily column into 3 columns of each of its values.

    @param df: the dataframe that must have a daily column.
    @return: dataframe the dataframe with brand new 3 columns.
    """
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


def create_date_yyyy_mm_dd_hh_mins(df: DataFrame, column: str) -> DataFrame:
    """Slices each value of the execution date column.

    @param df: the dataframe that must be sliced.
    @param column: the column that must be in the format %Y/%m/%d-%H:%M.
    @return df: the dataframe with the brand new 5 columns.
    """
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
