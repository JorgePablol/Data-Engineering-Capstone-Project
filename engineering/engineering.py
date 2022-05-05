import pandas as pd
import psycopg2
from pandas import DataFrame
from typing import List


def perform_query(
    query: str,
    columns_query: List[str],
) -> DataFrame:
    db_name: str = f"scrappers_{client.lower()}"
    connection: str = f"host={host_db} dbname={db_name} user={user_db} password={password_db}"
    df: DataFrame = pd.DataFrame()
    conn = psycopg2.connect(connection)
    try:
        conn.set_session(autocommit=True, readonly=True)
        cur = conn.cursor()
        if query == historical_execution_raw_files:
            query_tuple: Tuple = (config_report,
                                  daily_start,
                                  daily_end)
        else:
            query_tuple: Tuple = (config_report,
                                  daily_start,
                                  daily_end,
                                  daily_start,
                                  daily_end)

        cur.execute(query, query_tuple)
        df: DataFrame = DataFrame(
            cur.fetchall(),
            columns=columns_query,
        )
    except psycopg2.Error as e:
        logging.info(e)

    finally:
        conn.close()
    return df