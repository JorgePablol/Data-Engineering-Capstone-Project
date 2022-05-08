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
from dotenv import find_dotenv, load_dotenv
import pytest

from base.constants import z3_daily
from base.constants import columns_quality_checks
from base.z3_interface import z3Interface
from queries.queries import quality_checks
from engineering.engineering import change_column_datatype
from engineering.engineering import create_date_yyyy_mm_dd
from engineering.engineering import create_date_yyyy_mm_dd_custom
from load.sql_queries import (insert_table_queries_dict, drop_table_queries, create_table_queries)


class z3Base(z3Interface):
    DAILY_FORMAT: str = "%Y/%m/%d"

    def __init__(self):
        self.z3_tables_dictionary: Dict[str, str] = {}
        load_dotenv(find_dotenv())

        self.client_ids = ast.literal_eval(os.getenv("CLIENT_IDS"))
        self.provider_ids = ast.literal_eval(os.getenv("PROVIDER_IDS"))
        self.report_ids = ast.literal_eval(os.getenv("REPORT_IDS"))

        self.z3_indicators_master: DataFrame = pd.DataFrame()
        self.z3_dates_table: DataFrame = pd.DataFrame()
        self.z3_reports_table: DataFrame = pd.DataFrame()
        self.z3_clients_table: DataFrame = pd.DataFrame()
        self.z3_providers_table: DataFrame = pd.DataFrame()
        self.z3_indicators_table: DataFrame = pd.DataFrame()
        self.z3_executions_table: DataFrame = pd.DataFrame()

        self.z3_results_password_db: str = os.getenv("RESULTS_PASSWORD")
        self.z3_results_user_db: str = os.getenv("RESULTS_USER_DB")
        self.z3_results_host_db: str = os.getenv("RESULTS_HOST_DB")

    def _get_date_range(self) -> Tuple[str, str]:
        tz = pytz.timezone('America/Mexico_City')
        today_mx: dt.date = dt.datetime.now(tz=tz).today()
        yesterday: dt.date = (today_mx - dt.timedelta(days=1))
        thirty_days_ago: dt.date = (yesterday - dt.timedelta(days=10))  # TODO DEFINE IF ITS GOING TO BE 10, 20 OR 30
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
        df['provider'] = self.provider
        empty_df: int = df.shape[0]
        empty_df: bool = empty_df == 0
        return df, empty_df

    def _transform_quantitative_indicators(self, df: DataFrame, column: str) -> Tuple[DataFrame, bool]:
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
        df_fails['report_type'] = self.report_type

        return df_fails, empty_df

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

        return z3_unified, z3_unified_empty

    def extract_and_transform_each_provider(self):
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
                    z3_indicators, z3_indicators_empty = self.transform(z3_df=z3_df)
                    if not z3_indicators_empty:
                        self.z3_indicators_master = pd.concat([z3_indicators, self.z3_indicators_master])

        tz = pytz.timezone('America/Mexico_City')
        today_mx: dt.date = dt.datetime.now(tz=tz).today()
        today_str: str = today_mx.strftime('%Y/%m/%d-%H:%M')
        self.z3_indicators_master['execution_date'] = today_str

    def load(self):
        self.create_z3_indicators_dataframe_ids()
        self.create_star_schema_tables()
        self.z3_tables_dictionary: Dict[str, DataFrame] = {
            'DATES': self.z3_dates_table,
            'REPORTS': self.z3_reports_table,
            'CLIENTS': self.z3_clients_table,
            'PROVIDERS': self.z3_providers_table,
            'INDICATORS': self.z3_indicators_table,
            'EXECUTIONS': self.z3_executions_table
        }
        self.perform_load_queries()

    def create_z3_indicators_dataframe_ids(self):
        self._create_daily_id()
        self._create_client_id()
        self._create_report_id()
        self._create_provider_id()
        self._create_execution_id()

    def _create_daily_id(self):
        self.z3_indicators_master['daily_id'] = self.z3_indicators_master['daily'].copy()
        change_column_datatype(self.z3_indicators_master, 'daily_id', 'str')
        self.z3_indicators_master['daily_id'] = self.z3_indicators_master['daily_id'].apply(
            lambda x: x.replace('/', ''))
        change_column_datatype(self.z3_indicators_master, 'daily_id', 'int')

    def _get_client_id(self, client: str) -> int:
        return self.client_ids[client]

    def _create_client_id(self):
        self.z3_indicators_master['client_id'] = self.z3_indicators_master['client'].copy()
        self.z3_indicators_master['client_id'] = self.z3_indicators_master['client_id'].apply(self._get_client_id)
        change_column_datatype(self.z3_indicators_master, 'client_id', 'int')

    def _get_provider_id(self, provider: str) -> int:
        return self.provider_ids[provider]

    def _create_provider_id(self):
        self.z3_indicators_master['provider_id'] = self.z3_indicators_master['provider'].copy()
        self.z3_indicators_master['provider_id'] = self.z3_indicators_master['provider_id'].apply(self._get_provider_id)
        change_column_datatype(self.z3_indicators_master, 'provider_id', 'int')

    def _get_report_id(self, type_report: str) -> int:
        return self.report_ids[type_report]

    def _create_report_id(self):
        self.z3_indicators_master['report_id'] = self.z3_indicators_master['report_type'].copy()
        self.z3_indicators_master['report_id'] = self.z3_indicators_master['report_id'].apply(self._get_report_id)
        change_column_datatype(self.z3_indicators_master, 'report_id', 'int')

    def _create_execution_id(self):
        self.z3_indicators_master['execution_id'] = self.z3_indicators_master['execution_date'].copy()
        change_column_datatype(self.z3_indicators_master, 'execution_id', 'str')
        self.z3_indicators_master['execution_id'] = self.z3_indicators_master['execution_id'].apply(
            lambda x: x.replace('/', ''))
        self.z3_indicators_master['execution_id'] = self.z3_indicators_master['execution_id'].apply(
            lambda x: x.replace(':', ''))
        self.z3_indicators_master['execution_id'] = self.z3_indicators_master['execution_id'].apply(
            lambda x: x.replace('-', ''))

        change_column_datatype(self.z3_indicators_master, 'execution_id', 'int')

    def create_star_schema_tables(self):
        self._get_z3_dates_table()
        self._get_z3_reports_table()
        self._get_z3_clients_table()
        self._get_z3_providers_table()
        self._get_z3_indicators_table()
        self._get_z3_executions_table()

    def _get_z3_dates_table(self):
        self.z3_dates_table: DataFrame = self.z3_indicators_master.copy()
        self.z3_dates_table: DataFrame = self.z3_dates_table[['daily_id', 'daily']]
        self.z3_dates_table: DataFrame = create_date_yyyy_mm_dd(self.z3_dates_table)
        self.z3_dates_table = self.z3_dates_table.drop_duplicates(subset='daily_id')

    def _get_z3_reports_table(self):
        self.z3_reports_table: DataFrame = self.z3_indicators_master.copy()
        self.z3_reports_table = self.z3_reports_table[['report_id', 'report_type']]
        self.z3_reports_table = self.z3_reports_table.drop_duplicates(subset='report_id')

    def _get_z3_clients_table(self):
        self.z3_clients_table: DataFrame = self.z3_indicators_master.copy()
        self.z3_clients_table = self.z3_clients_table[['client_id', 'client']]
        self.z3_clients_table = self.z3_clients_table.drop_duplicates(subset='client_id')

    def _get_z3_providers_table(self):
        self.z3_providers_table: DataFrame = self.z3_indicators_master.copy()
        self.z3_providers_table = self.z3_providers_table[['provider_id', 'provider']]
        self.z3_providers_table = self.z3_providers_table.drop_duplicates(subset='provider_id')

    def _get_z3_indicators_table(self):
        self.z3_indicators_table: DataFrame = self.z3_indicators_master.copy()
        if 'scrapper_pos_qty' not in list(self.z3_indicators_table.columns):
            self.z3_indicators_table['scrapper_pos_qty'] = 0
            self.z3_indicators_table['scrapper_pos_sales'] = 0
        elif 'scrapper_curr_on_hand_qty' not in list(self.z3_indicators_table.columns):
            self.z3_indicators_table['scrapper_curr_on_hand_qty'] = 0

        self.z3_indicators_table = self.z3_indicators_table[
            ['client_id', 'report_id', 'provider_id', 'execution_id', 'daily_id', 'scrapper_pos_qty',
             'scrapper_pos_sales', 'scrapper_curr_on_hand_qty', 'scrapper_rows']]

    def _get_z3_executions_table(self):
        self.z3_executions_table: DataFrame = self.z3_indicators_master.copy()
        if 'scrapper_pos_qty' not in list(self.z3_executions_table.columns):
            self.z3_executions_table['scrapper_pos_qty'] = 0
            self.z3_executions_table['scrapper_pos_sales'] = 0
        elif 'scrapper_curr_on_hand_qty' not in list(self.z3_executions_table.columns):
            self.z3_executions_table['scrapper_curr_on_hand_qty'] = 0

        self.z3_executions_table = self.z3_executions_table[['execution_id', 'execution_date']]
        self.z3_executions_table: DataFrame = create_date_yyyy_mm_dd_custom(self.z3_executions_table, 'execution_date')

    def perform_load_queries(self) -> DataFrame:
        db_name: str = "z3_results"
        connection: str = f"host={self.z3_results_host_db} dbname={db_name} user={self.z3_results_user_db} password={self.z3_results_password_db}"
        conn = psycopg2.connect(connection)
        try:
            conn.set_session(autocommit=True, readonly=False)
            cur = conn.cursor()

            self._create_tables(cur)
            self._insert_tables(cur)

        except psycopg2.Error as e:
            logging.info(e)

        finally:
            conn.close()

    @staticmethod
    def _create_tables(cur):
        """Runs the creating tables queries
        @cur: database cursor
        @conn: database connection"""
        for query in create_table_queries:
            cur.execute(query)

    def _insert_tables(self, cur):
        """Distributes the information loadad by load_staging_tables function into
        5 tables
        @cur: database cursor
        @conn: database connection"""
        for key in list(insert_table_queries_dict.keys()):
            query: str = insert_table_queries_dict[key]
            dataframe: DataFrame = self.z3_tables_dictionary[key]
            for _, row in dataframe.iterrows():
                try:
                    cur.execute(query, list(row))
                except:
                    pass

    @staticmethod
    def drop_tables():
        load_dotenv(find_dotenv())
        z3_results_password_db: str = os.getenv("RESULTS_PASSWORD")
        z3_results_user_db: str = os.getenv("RESULTS_USER_DB")
        z3_results_host_db: str = os.getenv("RESULTS_HOST_DB")
        db_name: str = "z3_results"
        connection: str = f"host={z3_results_host_db} dbname={db_name} " \
                          f"user={z3_results_user_db} password={z3_results_password_db}"
        conn = psycopg2.connect(connection)
        try:
            conn.set_session(autocommit=True, readonly=False)
            cur = conn.cursor()
            for query in drop_table_queries:
                cur.execute(query)
        except psycopg2.Error as e:
            logging.info(e)

        finally:
            conn.close()

    def data_quality_checks(self):
        database_result: DataFrame = self._data_quality_query()
        database_pos_qty: float = database_result['database_pos_qty'].sum()
        database_pos_sales: float = database_result['database_pos_sales'].sum()
        database_curr_on_hand_qty: float = database_result['database_curr_on_hand_qty'].sum()
        database_rows: float = database_result['database_rows'].sum()
        database_dailys: List[str] = database_result['daily'].unique()

        scrapper_pos_qty: float = self.z3_indicators_table['scrapper_pos_qty'].sum()
        scrapper_pos_sales: float = self.z3_indicators_table['scrapper_pos_sales'].sum()
        scrapper_rows: float = self.z3_indicators_master['scrapper_rows'].sum()
        scrapper_curr_on_hand_qty: float = self.z3_indicators_table['scrapper_curr_on_hand_qty'].sum()
        scrapper_dailys: List[str] = self.z3_dates_table['daily'].unique()

        assert database_pos_qty == pytest.approx(scrapper_pos_qty, 0.2)
        assert database_pos_sales == pytest.approx(scrapper_pos_sales, 0.2)
        assert database_curr_on_hand_qty == pytest.approx(scrapper_curr_on_hand_qty, 0.2)
        assert database_rows == pytest.approx(scrapper_rows, 0.2)
        assert all(record in scrapper_dailys for record in database_dailys)

    def _data_quality_query(self) -> DataFrame:

        db_name: str = "z3_results"
        connection: str = f"host={self.z3_results_host_db} dbname={db_name} user={self.z3_results_user_db} " \
                          f"password={self.z3_results_password_db}"
        df_quality: DataFrame = pd.DataFrame()
        conn = psycopg2.connect(connection)
        try:
            conn.set_session(autocommit=True, readonly=True)
            cur = conn.cursor()
            cur.execute(quality_checks)

            df_quality: DataFrame = DataFrame(
                cur.fetchall(),
                columns=columns_quality_checks,
            )
        except psycopg2.Error as e:
            logging.info(e)

        finally:
            conn.close()

        return df_quality

    def main(self):
        self.extract_and_transform_each_provider()
        self.z3_indicators_master.to_csv(f'{z3_daily}/z3_indicators_{self.report_type.lower()}.csv', index=False)
        self.load()
        self.data_quality_checks()
