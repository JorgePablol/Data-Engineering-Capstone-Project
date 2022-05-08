import configparser
import psycopg2
from sql_queries import insert_table_queries

def insert_tables(cur, conn):
    """Distributes the information loadad by load_staging_tables function into
    5 tables
    @cur: database cursor
    @conn: database connection"""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Executes insert_tables and load_staging_tables"""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()