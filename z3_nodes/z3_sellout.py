import ast
import os

from dotenv import find_dotenv, load_dotenv

from base.constants import columns_sellout
from base.z3_base import z3Base
from queries.queries import sellout


class z3Sellout(z3Base):

    def __init__(self):
        super().__init__()
        load_dotenv(find_dotenv())

        self.client: str = ''
        self.provider: str = ''
        self.report_type: str = 'SELLOUT'
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
        self.main()
