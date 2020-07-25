import logging

import psycopg2
from sql_queries import * 
from config import get_config

logging.basicConfig(level=logging.INFO)

config = get_config()

def create_database():
    """
    This function creates bnhlaptop database,
    and return connection obejct and cursor variable
    
    input: None
    return: None
    """
    try:
        # connect to default database
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DB_DEFAULT'].values()))
        conn.set_session(autocommit=True)

        # get cursor variable 
        cur = conn.cursor()

        # drop database bnhlaptop if exists
        cur.execute(bnhlaptop_db_drop)

        # create new database bnhlapopt
        cur.execute(bnhlaptop_db_create)

        # close database connection
        conn.close()
    except Exception as e:
        logging.info("Error occurred when creating/dropping database bnhlaptop.")
        print(e)
    
    logging.info("Created new database bnhlaptop.")
    
    try:
        # connect to new database
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DB'].values()))
        cur = conn.cursor()
    except Exception as e:
        logging.info("Error occurred when connecting to new database.")
        print(e)
        
    return cur, conn


def drop_tables(cur, conn):
    """
    Drop tables with queries from drop_table_quries list.
    
    input:
    cur - cursor variable
    conn - database connection object
    
    return: None
    """
    try:
        # loop drop_table_queries
        for query in drop_table_queries:
            cur.execute(query)
            conn.commit()
    except Exception as e:
        logging.info("Error occurred dropping related tables.")
        print(e)

    logging.info('Dropped all tables in database bnhcomputer.')
    
    
def create_tables(cur, conn):
    """
    Create tables with quries from create_table_quries list
    
    input:
    cur - cursor variable
    conn - database connection object
    
    return: None
    """
    try:
        # loop over create_table_queries list
        for query in create_table_queries:
            cur.execute(query)
            conn.commit()
    except Exception as e:
        logging.info("Error occurred creating related tables.")
        print(e)
    
    logging.info('Created new tables in database bnhcomputer.')
    
        
def main():
    """
    -- create database
    
    -- drop tables if exists
    
    -- create tables
    
    -- close database connection
    
    """
    # create table and return connection object
    cur, conn = create_database()
    
    # drop & create tables
    drop_tables(cur, conn)
    create_tables(cur, conn)
    
    # close database connection
    conn.close()
    

if __name__ == '__main__':
    main()
    