import ast
import datetime as dt
import logging
import os
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import psycopg2
import pytest
import pytz
from dotenv import find_dotenv, load_dotenv
from pandas import DataFrame

from base.constants import columns_quality_checks
from base.z3_interface import z3Interface
from engineering.engineering import (change_column_datatype,
                                     create_date_yyyy_mm_dd,
                                     create_date_yyyy_mm_dd_hh_mins)
from extract_and_quality.extract_and_quality_queries import quality_checks
from load.load_queries import (create_table_queries, drop_table_queries,
                               insert_table_queries_dict)


class z3Base(z3Interface):
    """Class that defines and directs the execution of the etl process."""
    DAILY_FORMAT: str = "%Y/%m/%d"

    def __init__(self):
        """Defines the variables that will be used on the etl process."""
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

        self.z3_raw_data: DataFrame = pd.DataFrame()

        self.z3_results_password_db: str = os.getenv("RESULTS_PASSWORD")
        self.z3_results_user_db: str = os.getenv("RESULTS_USER_DB")
        self.z3_results_host_db: str = os.getenv("RESULTS_HOST_DB")

    def extract_and_transform_each_provider_and_client(self):
        """Performs the extraction that queries each database for each client
        then process the result of the query to filter the data and get only
        the extreme values that could be an error from the site."""
        for self.client in self.clients:
            provider_and_config_report_id: Dict[str, Dict] = ast.literal_eval(
                os.getenv(self.client.upper().strip()))
            provider_and_config_report_id: Dict[str, str] = provider_and_config_report_id.get(
                self.report_type)
            providers: List[str] = list(provider_and_config_report_id.keys())

            self.password_db: str = os.getenv("PASSWORD")
            self.user_db: str = os.getenv("USER_DB")
            self.host_db: str = os.getenv("HOST_DB") if self.client.upper() not in os.getenv(
                "CLIENTS_DB2") else os.getenv(
                "HOST_DB_2")

            for self.provider in providers:
                self.config_report: int = provider_and_config_report_id.get(
                    self.provider)
                z3_df, empty_df = self._extract()
                if not empty_df:
                    z3_indicators, z3_indicators_empty = self._transform(
                        z3_df=z3_df)
                    if not z3_indicators_empty:
                        self.z3_indicators_master = pd.concat(
                            [z3_indicators, self.z3_indicators_master])

        self.z3_raw_data.to_csv(f'raw_data_{self.report_type}.csv', index=False)
        tz = pytz.timezone('America/Mexico_City')
        today_mx: dt.date = dt.datetime.now(tz=tz).today()
        today_str: str = today_mx.strftime('%Y/%m/%d-%H:%M')
        self.z3_indicators_master['execution_date'] = today_str

    def _get_date_range(self) -> Tuple[str, str]:
        """Defines tha date range that will be queried amongst the databases.

        @return thirty_days_ago: the date that corresponds thirty days ago before yesterday.
        @return yesterday: the date that corresponds to the date before today.
        """
        tz = pytz.timezone('America/Mexico_City')
        today_mx: dt.date = dt.datetime.now(tz=tz).today()
        yesterday: dt.date = (today_mx - dt.timedelta(days=1))
        thirty_days_ago: dt.date = (yesterday - dt.timedelta(days=30))
        yesterday: str = yesterday.strftime(self.DAILY_FORMAT)
        thirty_days_ago: str = thirty_days_ago.strftime(self.DAILY_FORMAT)

        return thirty_days_ago, yesterday

    def _extract(self) -> Tuple[DataFrame, bool]:
        """Performs the extraction query and inserts the client, and provider
        of each request.

        @return df: the dataframe that is the result of the query.
        @return empty_df: a boolean that tells you if the dataframe is empty or not.
        """
        df: DataFrame = self._perform_extract_query()
        self.z3_raw_data = pd.concat([df, self.z3_raw_data])
        df['scrapper_rows'] = 1
        if self.report_type == 'INVENTORY':
            change_column_datatype(df, 'scrapper_curr_on_hand_qty', 'float')
        else:
            change_column_datatype(df, 'scrapper_pos_sales', 'float')
            change_column_datatype(df, 'scrapper_pos_qty', 'float')

        df: DataFrame = df.groupby('daily').aggregate('sum')
        df: DataFrame = pd.DataFrame(df.reset_index())
        df['client'] = self.client
        df['provider'] = self.provider
        empty_df: int = df.shape[0]
        empty_df: bool = empty_df == 0
        return df, empty_df

    def _perform_extract_query(self) -> DataFrame:
        """Connects to the database selected for each client and does the
        query, also gets the result into a dataframe format.

        @return df: the data frame that is the result of the query.
        """
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

    def _transform(self, z3_df: DataFrame) -> Tuple[DataFrame, bool]:
        """Once we have the raw data this method will filter to look for really
        extreme values.

        @param z3_df: the raw data from the database.
        @return z3_unified: since the data frame is filtered by extreme results for 3 different columns
            this dataframe corresponds to each result on the columns altogether in one dataframe.
        """
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

        z3_unified_empty: bool = bool(
            indicator_2_emptiness and indicator_1_emptiness)

        return z3_unified, z3_unified_empty

    def _transform_quantitative_indicators(self, df: DataFrame, column: str) -> Tuple[DataFrame, bool]:
        """
        Receives the raw dataframe and filters the column for its extreme values, using iqr * 3 to look
        for extreme outliers.
        @param df: the raw dataframe.
        @param column: the column to be filtered quantitatively
        @return df_fails: the result of the filter that are the possible fails from the website scrapped.
        @return empty_df: a boolean that shows if there wasnt any extreme results.
        """
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

    def load(self):
        """Slices the z3_indicators_master dataframe that agglomerates the data
        from all the clients and providers into each table of the star schema
        format, for that it creates the id for each table.

        After that loads the tables into the z3_results database.
        """
        self._create_z3_indicators_dataframe_ids()
        self._create_star_schema_tables()
        self.z3_tables_dictionary: Dict[str, DataFrame] = {
            'DATES': self.z3_dates_table,
            'REPORTS': self.z3_reports_table,
            'CLIENTS': self.z3_clients_table,
            'PROVIDERS': self.z3_providers_table,
            'INDICATORS': self.z3_indicators_table,
            'EXECUTIONS': self.z3_executions_table
        }
        self._perform_load_queries()

    def _create_z3_indicators_dataframe_ids(self):
        """Executes the method to create the ids for each table."""
        self._create_daily_id()
        self._create_client_id()
        self._create_report_id()
        self._create_provider_id()
        self._create_execution_id()

    def _create_daily_id(self):
        """Takes the master data frame, gets the daily id based on its
        value."""
        self.z3_indicators_master['daily_id'] = self.z3_indicators_master['daily'].copy(
        )
        change_column_datatype(self.z3_indicators_master, 'daily_id', 'str')
        self.z3_indicators_master['daily_id'] = self.z3_indicators_master['daily_id'].apply(
            lambda x: x.replace('/', ''))
        change_column_datatype(self.z3_indicators_master, 'daily_id', 'int')

    def _get_client_id(self, client: str) -> int:
        """Receives the client name and returns its id.

        @param client: the client name.
        @return client_id: the client id as an integer.
        """
        return self.client_ids[client]

    def _create_client_id(self):
        """Creates the client_id column, by executing the get_client_id into a
        copy of the client names column."""
        self.z3_indicators_master['client_id'] = self.z3_indicators_master['client'].copy(
        )
        self.z3_indicators_master['client_id'] = self.z3_indicators_master['client_id'].apply(
            self._get_client_id)
        change_column_datatype(self.z3_indicators_master, 'client_id', 'int')

    def _get_provider_id(self, provider: str) -> int:
        """Receives the provider name and returns its id.

        @param provider: provider name.
        @return: the id as an integer.
        """
        return self.provider_ids[provider]

    def _create_provider_id(self):
        """Creates the provider_id column."""
        self.z3_indicators_master['provider_id'] = self.z3_indicators_master['provider'].copy(
        )
        self.z3_indicators_master['provider_id'] = self.z3_indicators_master['provider_id'].apply(
            self._get_provider_id)
        change_column_datatype(self.z3_indicators_master, 'provider_id', 'int')

    def _get_report_id(self, type_report: str) -> int:
        """Receives the report type and returns its id.

        :param type_report: the report type.
        :return report_id: the report id as an integer.
        """
        return self.report_ids[type_report]

    def _create_report_id(self):
        """Executes the _get_report_id method into the report_id column to get
        the ids."""
        self.z3_indicators_master['report_id'] = self.z3_indicators_master['report_type'].copy(
        )
        self.z3_indicators_master['report_id'] = self.z3_indicators_master['report_id'].apply(
            self._get_report_id)
        change_column_datatype(self.z3_indicators_master, 'report_id', 'int')

    def _create_execution_id(self):
        """Creates the execution_id column."""
        self.z3_indicators_master['execution_id'] = self.z3_indicators_master['execution_date'].copy(
        )
        change_column_datatype(self.z3_indicators_master,
                               'execution_id', 'str')
        self.z3_indicators_master['execution_id'] = self.z3_indicators_master['execution_id'].apply(
            lambda x: x.replace('/', ''))
        self.z3_indicators_master['execution_id'] = self.z3_indicators_master['execution_id'].apply(
            lambda x: x.replace(':', ''))
        self.z3_indicators_master['execution_id'] = self.z3_indicators_master['execution_id'].apply(
            lambda x: x.replace('-', ''))

        change_column_datatype(self.z3_indicators_master,
                               'execution_id', 'int')

    def _create_star_schema_tables(self):
        """Slices the z3_indicators_master dataframe that agglomerates the data
        from all the clients and providers into each table of the star schema
        format."""
        self._get_z3_dates_table()
        self._get_z3_reports_table()
        self._get_z3_clients_table()
        self._get_z3_providers_table()
        self._get_z3_indicators_table()
        self._get_z3_executions_table()

    def _get_z3_dates_table(self):
        """Copies the data from the master file into another dataframe that
        will correspond to the dates table, it filters the column to the only
        needed according to each table from the star schema defined."""
        self.z3_dates_table: DataFrame = self.z3_indicators_master.copy()
        self.z3_dates_table: DataFrame = self.z3_dates_table[[
            'daily_id', 'daily']]
        self.z3_dates_table: DataFrame = create_date_yyyy_mm_dd(
            self.z3_dates_table)
        self.z3_dates_table = self.z3_dates_table.drop_duplicates(
            subset='daily_id')

    def _get_z3_reports_table(self):
        """Copies the data from the master file into another dataframe that
        will correspond to the reports table, it filters the column to the only
        needed according to each table from the star schema defined."""
        self.z3_reports_table: DataFrame = self.z3_indicators_master.copy()
        self.z3_reports_table = self.z3_reports_table[[
            'report_id', 'report_type']]
        self.z3_reports_table = self.z3_reports_table.drop_duplicates(
            subset='report_id')

    def _get_z3_clients_table(self):
        """Copies the data from the master file into another dataframe that
        will correspond to the clients table, it filters the column to the only
        needed according to each table from the star schema defined."""
        self.z3_clients_table: DataFrame = self.z3_indicators_master.copy()
        self.z3_clients_table = self.z3_clients_table[['client_id', 'client']]
        self.z3_clients_table = self.z3_clients_table.drop_duplicates(
            subset='client_id')

    def _get_z3_providers_table(self):
        """Copies the data from the master file into another dataframe that
        will correspond to the providers table, it filters the column to the
        only needed according to each table from the star schema defined."""
        self.z3_providers_table: DataFrame = self.z3_indicators_master.copy()
        self.z3_providers_table = self.z3_providers_table[[
            'provider_id', 'provider']]
        self.z3_providers_table = self.z3_providers_table.drop_duplicates(
            subset='provider_id')

    def _get_z3_indicators_table(self):
        """Copies the data from the master file into another dataframe that
        will correspond to the indicators table, it filters the column to the
        only needed according to each table from the star schema defined."""
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
        """Copies the data from the master file into another dataframe that
        will correspond to the executions table, it filters the column to the
        only needed according to each table from the star schema defined."""
        self.z3_executions_table: DataFrame = self.z3_indicators_master.copy()
        if 'scrapper_pos_qty' not in list(self.z3_executions_table.columns):
            self.z3_executions_table['scrapper_pos_qty'] = 0
            self.z3_executions_table['scrapper_pos_sales'] = 0
        elif 'scrapper_curr_on_hand_qty' not in list(self.z3_executions_table.columns):
            self.z3_executions_table['scrapper_curr_on_hand_qty'] = 0

        self.z3_executions_table = self.z3_executions_table[[
            'execution_id', 'execution_date']]
        self.z3_executions_table: DataFrame = create_date_yyyy_mm_dd_hh_mins(
            self.z3_executions_table, 'execution_date')

    def _perform_load_queries(self) -> DataFrame:
        """Creates the tables and inserts the data into the z3_results
        database."""
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
        """Runs the creating tables extract_and_quality.

        @cur: database cursor
        @conn: database connection
        """
        for query in create_table_queries:
            cur.execute(query)

    def _insert_tables(self, cur):
        """Distributes the information into each table from the star schema.

        @cur: database cursor
        @conn: database connection
        """
        for key in list(insert_table_queries_dict.keys()):
            query: str = insert_table_queries_dict[key]
            dataframe: DataFrame = self.z3_tables_dictionary[key]
            for _, row in dataframe.iterrows():
                cur.execute(query, list(row))

    def data_quality_checks(self):
        """Tests if the data processed corresponds to the data loaded into the
        database."""
        database_result: DataFrame = self._perform_data_quality_query()
        database_pos_qty: float = database_result['database_pos_qty'].sum()
        database_pos_sales: float = database_result['database_pos_sales'].sum()
        database_curr_on_hand_qty: float = database_result['database_curr_on_hand_qty'].sum(
        )
        database_rows: float = database_result['database_rows'].sum()
        database_dailys: List[str] = database_result['daily'].unique()

        scrapper_pos_qty: float = self.z3_indicators_table['scrapper_pos_qty'].sum(
        )
        scrapper_pos_sales: float = self.z3_indicators_table['scrapper_pos_sales'].sum(
        )
        scrapper_rows: float = self.z3_indicators_master['scrapper_rows'].sum()
        scrapper_curr_on_hand_qty: float = self.z3_indicators_table['scrapper_curr_on_hand_qty'].sum(
        )
        scrapper_dailys: List[str] = self.z3_dates_table['daily'].unique()

        assert database_pos_qty == pytest.approx(scrapper_pos_qty, 0.2)
        assert database_pos_sales == pytest.approx(scrapper_pos_sales, 0.2)
        assert database_curr_on_hand_qty == pytest.approx(
            scrapper_curr_on_hand_qty, 0.2)
        assert database_rows == pytest.approx(scrapper_rows, 0.2)
        assert all(record in scrapper_dailys for record in database_dailys)

    def _perform_data_quality_query(self) -> DataFrame:
        """Queries the z3_results database, and takes the result into a
        dataframe."""
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

    @staticmethod
    def drop_tables():
        """Drops all the tables in the z3_results database."""
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

    def main(self):
        """Directs the execution order of each method."""
        self.extract_and_transform_each_provider_and_client()
        self.load()
        self.data_quality_checks()
