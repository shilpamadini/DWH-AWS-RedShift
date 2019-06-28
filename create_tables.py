import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    
    """function executes drop table statements."""
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    
    """function executes create table statements."""
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    
    # read connection details from the config file
    config.read('dwh.cfg')
    
    # connect to the redshift cluster and database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # drops all the tables
    drop_tables(cur, conn)
    
    # create the necessary tables
    create_tables(cur, conn)

    # close the connection to the database
    conn.close()


if __name__ == "__main__":
    main()