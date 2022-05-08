# DROP TABLES
from typing import Dict

indicators_table_drop = "DROP TABLE IF EXISTS indicators;"
providers_table_drop = "DROP TABLE IF EXISTS providers;"
daily_table_drop = "DROP TABLE IF EXISTS daily;"
reports_table_drop = "DROP TABLE IF EXISTS reports;"
clients_table_drop = "DROP TABLE IF EXISTS clients;"
executions_table_drop = "DROP TABLE IF EXISTS executions;"

# CREATE TABLES
indicators_table_create = ("""
CREATE TABLE IF NOT EXISTS indicators
    (
        id SERIAL PRIMARY KEY,
        client_id INTEGER,
        report_id INTEGER,
        provider_id INTEGER,
        execution_id BIGINT,
        daily_id BIGINT,
        scrapper_pos_qty FLOAT,
        scrapper_pos_sales FLOAT,
        scrapper_curr_on_hand_qty FLOAT,
        scrapper_rows FLOAT
    );
""")

daily_table_create = ("""
    CREATE TABLE IF NOT EXISTS daily
    (
        daily_id INTEGER PRIMARY KEY,
        daily VARCHAR(20),
        daily_year INTEGER,
        daily_month INTEGER,
        daily_day INTEGER
    );
""")

clients_table_create = ("""
    CREATE TABLE IF NOT EXISTS clients
    (
        client_id INTEGER PRIMARY KEY,
        client VARCHAR(50)
    );
""")

reports_table_create = ("""
    CREATE TABLE IF NOT EXISTS reports
    (
        report_id INTEGER PRIMARY KEY,
        report_type VARCHAR(50)
    );
""")

providers_table_create = ("""
    CREATE TABLE IF NOT EXISTS providers
    (
        provider_id INTEGER PRIMARY KEY,
        provider VARCHAR(50)
    );
""")

executions_table_create = ("""
    CREATE TABLE IF NOT EXISTS executions
    (
        execution_id BIGINT PRIMARY KEY,
        execution_date VARCHAR(40),
        execution_year INTEGER,
        execution_month INTEGER,
        execution_day INTEGER,
        execution_hour INTEGER,
        execution_minute INTEGER
    )
""")

# INSERT TABLES
indicators_table_insert = ("""
    INSERT INTO indicators
            (
            client_id,
            report_id,
            provider_id,
            execution_id,
            daily_id,
            scrapper_pos_qty,
            scrapper_pos_sales,
            scrapper_curr_on_hand_qty,
            scrapper_rows
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
""")

daily_table_insert = ("""
    INSERT INTO daily
            (
            daily_id,
            daily,
            daily_year,
            daily_month,
            daily_day
    )
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (daily_id) DO NOTHING;
""")

clients_table_insert = ("""
    INSERT INTO clients
            (
            client_id,
            client
    )
    VALUES (%s, %s)
    ON CONFLICT (client_id) DO NOTHING;
""")

reports_table_insert = ("""
    INSERT INTO reports
            (
            report_id,
            report_type
    )
    VALUES (%s, %s)
    ON CONFLICT (report_id) DO NOTHING;
""")

providers_table_insert = ("""
    INSERT INTO providers
            (
            provider_id,
            provider
    )
    VALUES (%s, %s)
    ON CONFLICT (provider_id) DO NOTHING;
""")

executions_table_insert = ("""
    INSERT INTO executions
            (
            execution_id,
            execution_date,
            execution_year,
            execution_month,
            execution_day,
            execution_hour,
            execution_minute
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (execution_id) DO NOTHING;
""")

# QUERY LISTS

create_table_queries = [indicators_table_create, daily_table_create, clients_table_create,
                        reports_table_create, providers_table_create, executions_table_create]
drop_table_queries = [indicators_table_drop, providers_table_drop, daily_table_drop,
                      reports_table_drop, clients_table_drop, executions_table_drop]
insert_table_queries_dict = {
    'INDICATORS': indicators_table_insert,
    'DATES': daily_table_insert,
    'CLIENTS': clients_table_insert,
    'REPORTS': reports_table_insert,
    'PROVIDERS': providers_table_insert,
    'EXECUTIONS': executions_table_insert
}
