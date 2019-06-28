import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    
    """function executes the copy command to load files from s3 to stage tables."""
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    
    """function runs the insert table queries to load data into target tables."""
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    
    # read connection details from the config file
    config.read('dwh.cfg')
    
    # connect to the redshift cluster and database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # load staging tables
    load_staging_tables(cur, conn)
    
    # load target tables
    insert_tables(cur, conn)
    
    # close the connection to the database
    conn.close()


if __name__ == "__main__":
    main()