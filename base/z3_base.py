import datetime as dt
from typing import Tuple, Dict, List
import pytz
from pandas import DataFrame
import psycopg2
import pandas as pd
import logging
import numpy as np
import ast
import os

from base.constants import z3_daily
from base.z3_interface import z3Interface


class z3Base(z3Interface):
    DAILY_FORMAT: str = "%Y/%m/%d"

    def _get_date_range(self) -> Tuple[str, str]:
        tz = pytz.timezone('America/Mexico_City')
        today_mx: dt.date = dt.datetime.now(tz=tz).today()
        yesterday: dt.date = (today_mx - dt.timedelta(days=1))
        thirty_days_ago: dt.date = (yesterday - dt.timedelta(days=20))  # TODO DEFINE IF ITS GOING TO BE 10 OR 30
        yesterday: str = yesterday.strftime(self.DAILY_FORMAT)
        thirty_days_ago: str = thirty_days_ago.strftime(self.DAILY_FORMAT)

        return thirty_days_ago, yesterday

    def perform_query(self) -> DataFrame:
        thirty_days_ago, yesterday = self._get_date_range()
        db_name: str = f"scrappers_{self.client.lower()}"
        connection: str = f"host={self.host_db} dbname={db_name} user={self.user_db} password={self.password_db}"
        df: DataFrame = pd.DataFrame()
        conn = psycopg2.connect(connection)
        try:
            conn.set_session(autocommit=True, readonly=True)
            cur = conn.cursor()
            query_tuple: Tuple = (self.config_report,
                                  thirty_days_ago,
                                  yesterday,
                                  thirty_days_ago,
                                  yesterday)

            cur.execute(self.query_db, query_tuple)
            df: DataFrame = DataFrame(
                cur.fetchall(),
                columns=self.columns,
            )
        except psycopg2.Error as e:
            logging.info(e)

        finally:
            conn.close()

        return df

    def extract(self) -> Tuple[DataFrame, bool]:
        df: DataFrame = self.perform_query()
        df['client'] = self.client
        df['config_report_id'] = self.config_report
        df['provider'] = self.provider
        empty_df: int = df.shape[0]
        empty_df: bool = empty_df == 0
        return df, empty_df

    @staticmethod
    def _transform_quantitative_indicators(df: DataFrame, column: str) -> Tuple[DataFrame, bool]:
        first_q: float = np.percentile(df[column], 25)
        third_q: float = np.percentile(df[column], 75)

        iqr: float = third_q - first_q
        lower_limit: float = first_q - (iqr * 3)
        lower_limit: float = lower_limit if lower_limit > 0 else 1
        upper_limit: float = third_q + (iqr * 3)

        df_lower: DataFrame = df[df[column] < lower_limit]
        df_upper: DataFrame = df[df[column] > upper_limit]

        df_fails: DataFrame = pd.DataFrame()
        df_fails: DataFrame = pd.concat([df_lower, df_fails])
        df_fails: DataFrame = pd.concat([df_upper, df_fails])

        empty_df: int = df_fails.shape[0]
        empty_df: bool = empty_df == 0
        return df_fails, empty_df

    def _transform_latest_date_comparison(self, df: DataFrame) -> DataFrame:
        df['daily'] = pd.to_datetime(df['daily']).dt.date
        df = df.sort_values(by='daily', ascending=False)
        df_latest_date = df.iloc[:1]
        latest_date = df_latest_date['daily'].values
        latest_date = latest_date[0]
        latest_date_string: str = latest_date.strftime(self.DAILY_FORMAT)

        tz = pytz.timezone('America/Mexico_City')
        today_mx: dt.date = dt.datetime.now(tz=tz).today()
        today_mx: dt.date = today_mx.date()
        yesterday: dt.date = (today_mx - dt.timedelta(days=1))
        yesterday_str: str = yesterday.strftime(self.DAILY_FORMAT)
        today_str: str = today_mx.strftime(self.DAILY_FORMAT)

        days_difference = yesterday - latest_date
        days_difference = days_difference.days

        df_latest_date['days_difference'] = days_difference
        df_latest_date['latest_date_str'] = latest_date_string
        df_latest_date['today'] = today_str
        df_latest_date['yesterday'] = yesterday_str

        # df_latest_date = df_latest_date.query("days_difference > 1")

        return df_latest_date

    def transform(self, z3_df: DataFrame) -> Tuple[DataFrame, bool, DataFrame]:
        indicator_2_emptiness: bool = True
        z3_indicator_1, indicator_1_emptiness = self._transform_quantitative_indicators(
            z3_df,
            self.key_performance_indicator_1
        )

        if self.key_performance_indicator_2:
            z3_indicator_2, indicator_2_emptiness = self._transform_quantitative_indicators(
                z3_df,
                self.key_performance_indicator_2
            )

        z3_rows, rows_emptiness = self._transform_quantitative_indicators(
            z3_df,
            'scrapper_rows'
        )

        z3_unified: DataFrame = pd.DataFrame()
        if not indicator_1_emptiness:
            z3_unified: DataFrame = pd.concat([z3_indicator_1, z3_unified])

        if not indicator_2_emptiness and self.key_performance_indicator_2:
            z3_unified: DataFrame = pd.concat([z3_indicator_2, z3_unified])

        if not rows_emptiness:
            z3_unified: DataFrame = pd.concat([z3_rows, z3_unified])

        z3_unified: DataFrame = z3_unified.drop_duplicates(keep='first')

        z3_unified_empty: bool = bool(indicator_2_emptiness and indicator_1_emptiness)

        z3_latest_date = self._transform_latest_date_comparison(z3_df)
        return z3_unified, z3_unified_empty, z3_latest_date

    def iterate_providers(self):
        for self.client in self.clients:
            provider_and_config_report_id: Dict[str, Dict] = ast.literal_eval(os.getenv(self.client.upper().strip()))
            provider_and_config_report_id: Dict[str, str] = provider_and_config_report_id.get(self.report_type)
            providers: List[str] = list(provider_and_config_report_id.keys())

            self.password_db: str = os.getenv("PASSWORD")
            self.user_db: str = os.getenv("USER_DB")
            self.host_db: str = os.getenv("HOST_DB") if self.client.upper() not in os.getenv(
                "CLIENTS_DB2") else os.getenv(
                "HOST_DB_2")
            for self.provider in providers:
                self.config_report: int = provider_and_config_report_id.get(self.provider)
                z3_df, empty_df = self.extract()
                if not empty_df:
                    z3_indicators, z3_indicators_empty, z3_latest_date = self.transform(z3_df=z3_df)
                    if not z3_indicators_empty:
                        self.z3_indicators_master = pd.concat([z3_indicators, self.z3_indicators_master])

                    self.z3_latest_date_master = pd.concat([z3_latest_date, self.z3_latest_date_master])

        self.z3_latest_date_master = self.z3_latest_date_master.sort_values(by='days_difference', ascending=False)

    def load(self):
        pass

    def main(self):
        self.iterate_providers()
        display(self.z3_indicators_master.head(30))
        display(self.z3_latest_date_master.head(30))
        self.z3_indicators_master.to_csv(f'{z3_daily}/z3_indicators_{self.report_type.lower()}.csv', index=False)
        self.z3_latest_date_master.to_csv(f'{z3_daily}/z3_latest_date_{self.report_type.lower()}.csv', index=False)







