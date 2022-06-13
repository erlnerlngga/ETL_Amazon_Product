import psycopg2
import psycopg2.extras
import os
import pandas as pd

db_info = {
    'dbname': 'amazon_product_db',
    'user': 'airflow',
    'password': 'airflow',
    'host': 'localhost',
    'port': 5432
}

def connect_db():
    conn = psycopg2.connect(
            dbname=db_info['dbname'],
            user=db_info['user'],
            password=db_info['password'],
            host=db_info['host'],
            port=db_info['port']
            )
    return conn

def init_db():
    conn = connect_db()
    cur = conn.cursor()
    
    drop_sql_table = """DROP TABLE IF EXISTS amazon_product"""
    create_sql_table = """
                        CREATE TABLE IF NOT EXISTS amazon_product(
                            id_key SERIAL
                            Description TEXT NOT NULL,
                            Price NUMERIC NOT NULL,
                            Ratings NUMERIC NOT NULL,
                            Review_Count INTEGER NOT NULL,
                            Url TEXT NOT NULL,
                            PRIMARY KEY(id_key)
                        )
                        """
    cur.execute(drop_sql_table)
    cur.commit()
    cur.execute(create_sql_table)
    cur.commit()
    
def load(table):
    temp = '/opt/airflow/data/amazon_product.csv'
    # df.to_csv(temp, sep=';', index=False, header=False)

    
    conn = connect_db()
    cur = conn.cursor()
    
    try:
        with open(temp, 'r') as f:
            cur.copy_from(f, table, sep=";")
            conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        os.remove(temp)
        print(f"Error: {error}")
        conn.rollback()
        conn.close()
        return -1
    
    print('load-done')
    conn.close()
    os.remove(temp)