import logging

import psycopg2
from sql_queries import * 

logging.basicConfig(level=logging.INFO)

def create_database():
    """
    This function creates bnhlaptop database,
    and return connection obejct and cursor variable
    
    input: None
    return: None
    """
    # connect to default database
    conn = psycopg2.connect(host='localhost',
                            dbname='postgres', 
                            password='test' ,
                            port=5432, 
                            user='postgres')
    conn.set_session(autocommit=True)
    # get cursor variable 
    cur = conn.cursor()
    # drop database bnhlaptop if exists
    cur.execute("DROP DATABASE IF EXISTS bnhlaptop")
    # create new database bnhlapopt
    cur.execute("""CREATE DATABASE bnhlaptop 
                            WITH ENCODING 'utf8'
                            TEMPLATE template0""")
    # close database connection
    conn.close()
    
    logging.info("Created new database bnhlaptop.")
    
    # connect to new database
    conn = psycopg2.connect(host='localhost', 
                            dbname='bnhlaptop', 
                            password='test', 
                            port=5432, 
                            user='postgres'")
    cur = conn.cursor()
    return cur, conn


def drop_tables(cur, conn):
    """
    Drop tables with queries from drop_table_quries list.
    
    input:
    cur - cursor variable
    conn - database connection object
    
    return: None
    """
    # loop drop_table_queries
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        
    logging.info('Dropped all tables in database bnhcomputer.')
    
    
def create_tables(cur, conn):
    """
    Create tables with quries from create_table_quries list
    
    input:
    cur - cursor variable
    conn - database connection object
    
    return: None
    """
    # loop over create_table_queries list
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    
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
    