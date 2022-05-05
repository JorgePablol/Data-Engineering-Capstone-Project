import os
import ast
from dotenv import find_dotenv, load_dotenv

import pandas as pd
from pandas import DataFrame
from queries.queries import sellout

from base.constants import columns_sellout
from base.z3_base import z3Base


class z3Sellout(z3Base):

    def __init__(self, report_type='SELLOUT'):
        super().__init__()
        load_dotenv(find_dotenv())

        self.client: str = ''
        self.provider: str = ''
        self.report_type: str = report_type
        self.clients = ast.literal_eval(os.getenv("CLIENTS"))

        self.password_db: str = ''
        self.user_db: str = ''
        self.host_db: str = ''
        self.config_report: int = 0
        self.client: str = ''
        self.query_db: str = sellout
        self.columns: str = columns_sellout

        self.key_performance_indicator_1: str = 'scrapper_pos_qty'
        self.key_performance_indicator_2: str = 'scrapper_pos_sales'
        self.z3_latest_date_master: DataFrame = pd.DataFrame()
        self.z3_indicators_master: DataFrame = pd.DataFrame()

        self.main()
