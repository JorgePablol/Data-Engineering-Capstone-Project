import ast
import os

import pandas as pd
from dotenv import find_dotenv, load_dotenv
from pandas import DataFrame

from base.constants import columns_inventory
from base.z3_base import z3Base
from extract_and_quality_queries.extract_and_quality_queries import inventory


class z3Inventory(z3Base):
    """Class that performs the etl process for the inventory data from
    scrappers."""

    def __init__(self):
        super().__init__()
        load_dotenv(find_dotenv())

        self.client: str = ''
        self.provider: str = ''
        self.report_type: str = 'INVENTORY'
        self.clients = ast.literal_eval(os.getenv("CLIENTS"))

        self.password_db: str = ''
        self.user_db: str = ''
        self.host_db: str = ''
        self.config_report: int = 0
        self.client: str = ''
        self.query_db: str = inventory
        self.columns: str = columns_inventory

        self.key_performance_indicator_1: str = 'scrapper_curr_on_hand_qty'
        self.key_performance_indicator_2: None = None
        self.z3_latest_date_master: DataFrame = pd.DataFrame()
        self.z3_indicators_master: DataFrame = pd.DataFrame()

        self.main()
