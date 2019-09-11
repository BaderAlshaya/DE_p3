import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Load data from S3 to staging tables on Redshift"""
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Load data from staging tables to analytics tables on Redshift"""
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    """Load data from S3 to staging tables and then from staging tables to analytics tables"""
    # read config
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # connect to the Redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # load data from S3 to staging tables on Redshift
    load_staging_tables(cur, conn)

    # load data from staging tables to analytics tables on Redshift
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()