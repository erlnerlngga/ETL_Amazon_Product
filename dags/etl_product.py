import datetime
import psycopg2
import psycopg2.extras
import pandas as pd
import os

import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import datetime
import json


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
    
    drop_sql_table = """DROP TABLE IF EXISTS amazon_product;"""
    create_sql_table = """
                        CREATE TABLE IF NOT EXISTS amazon_product(
                            Key_id SERIAL,
                            Description TEXT NOT NULL,
                            Price NUMERIC(10,2) NOT NULL,
                            Ratings NUMERIC(10,2) NOT NULL,
                            Review_Count INTEGER NOT NULL,
                            Url TEXT NOT NULL,
                            PRIMARY KEY(Key_id)
                        )
                        """
    cur.execute(drop_sql_table)
    conn.commit()
    cur.execute(create_sql_table)
    conn.commit()
    
def load(table):
    temp = '/opt/airflow/data/amazon_product.csv'
    # df.to_csv(temp, sep=';', index=False, header=False)

    
    conn = connect_db()
    cur = conn.cursor()

    sl = r""" COPY amazon_product(Description, Price, Ratings, Review_Count, Url)
        FROM '/opt/airflow/data/amazon_product.csv'
        DELIMITER ';'
        CSV HEADER; """  
    
    try:
        # with open(temp, 'r') as f:
        #     cur.copy_from(f, table, sep=";")
        #     conn.commit()
        # csv_file_name = '/home/user/some_file.csv'
        cur.execute(sl)
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

def load_json(title):
    with open(title, encoding='utf-8') as f:
        return json.load(f)

def clean_laptop_phone_bed_furniture(df):
    df.drop_duplicates(ignore_index=True, inplace=True)
    
    revi= df.Review_Count.apply(lambda x: True if (len(x)==0) else False) #rating
    idx = list(df.loc[revi, :].index)
    df.loc[idx, 'Ratings'] = '0.0 out of 5 stars'
    df.loc[idx, 'Review_Count'] = '0'
    
    df.Price.replace(',', '' , regex=True, inplace=True)
    df.Review_Count.replace(',', '' , regex=True, inplace=True)
    df.Ratings = df.Ratings.apply(lambda x: x[:3])
    df = df.astype({'Price': 'float', 'Review_Count': 'int', 'Ratings': 'float'})
    return df

def clean_tablet_camera_cook(df):
    df.drop_duplicates(ignore_index=True, inplace=True)
    
    revi= df.Review_Count.apply(lambda x: True if (len(x)==0) else False) 
    rat = df.Ratings.apply(lambda x: True if (len(x)==0) else False) 
    idx_revi = list(df.loc[revi, :].index)
    idx_rat = list(df.loc[rat, :].index)
    
    df.loc[idx_rat, 'Ratings'] = '0.0 out of 5 stars'
    df.loc[idx_revi, 'Review_Count'] = '0'
    
    df.Price.replace(',', '' , regex=True, inplace=True)
    df.Review_Count.replace(',', '' , regex=True, inplace=True)
    df.Ratings = df.Ratings.apply(lambda x: x[:3])
    
    temp_idx = df.Ratings.str.isalpha()
    df.loc[temp_idx, 'Ratings'] = '0.0'
    df = df.astype({'Price': 'float', 'Review_Count': 'int', 'Ratings': 'float'})
                
    return df

def extract_data():
    url='https://raw.githubusercontent.com/erlnerlngga/ETL_Amazon_Product/master/data/'
    file_names=['laptop-computers-062022.json', 
                 'phones-electronics-062022.json', 
                 'bedroom-kitchen-062022.json', 
                 'furniture-kitchen-062022.json', 
                 'tablet-computers-062022.json', 
                 'camera-electronics-062022.json', 
                 'cookware-kitchen-062022.json']
    for file_name in file_names:
        if os.path.exists(f"/opt/airflow/data/{file_name}"):
            os.remove(f"/opt/airflow/data/{file_name}")
        os.system(f"cd /opt/airflow/data && curl -LJO {url+file_name}")

def transform_data():
    # file = glob.glob(f'/opt/airflow/data/*')

    laptop=load_json('/opt/airflow/data/laptop-computers-062022.json')
    phones=load_json('/opt/airflow/data/phones-electronics-062022.json')
    tablet=load_json('/opt/airflow/data/tablet-computers-062022.json')
    camera=load_json('/opt/airflow/data/camera-electronics-062022.json')
    bedroo=load_json('/opt/airflow/data/bedroom-kitchen-062022.json')
    furnit=load_json('/opt/airflow/data/furniture-kitchen-062022.json')
    cookwa=load_json('/opt/airflow/data/cookware-kitchen-062022.json')

    # for i in file:
    #     # print(i[40:46], end=' ')
    #     locals()[i[-6:]] = load_json(i)
    #     locals()[i[-6:]] = pd.DataFrame(locals()[i[-6:]])

    laptop_df=pd.DataFrame(laptop)
    phones_df=pd.DataFrame(phones)
    tablet_df=pd.DataFrame(tablet)
    camera_df=pd.DataFrame(camera)
    bedroo_df=pd.DataFrame(bedroo)
    furnit_df=pd.DataFrame(furnit)
    cookwa_df=pd.DataFrame(cookwa)

    # df_name = ['laptop', 'phones', 'bedroo', 'furnit', 'tablet', 'camera', 'cookwa']
    # for df in range(len(df_name)):
    #     if df > 3:
    #         locals()[df_name[df]] = clean_tablet_camera_cook(locals()[df_name[df]])
    #     else:
    #         locals()[df_name[df]] = clean_laptop_phone_bed_furniture(locals()[df_name[df]])

    laptop_new=clean_laptop_phone_bed_furniture(laptop_df)
    phones_new=clean_laptop_phone_bed_furniture(phones_df)
    tablet_new=clean_tablet_camera_cook(tablet_df)
    camera_new=clean_tablet_camera_cook(camera_df)
    bedroo_new=clean_laptop_phone_bed_furniture(bedroo_df)
    furnit_new=clean_laptop_phone_bed_furniture(furnit_df)
    cookwa_new=clean_tablet_camera_cook(cookwa_df)

    merge_df = pd.concat([laptop_new, phones_new, tablet_new, camera_new, bedroo_new, furnit_new, cookwa_new], ignore_index=True)
    temp = '/opt/airflow/data/amazon_product.csv'
    merge_df.to_csv(temp, index=False, sep=';')


def load_data():
    init_db()
    load('amazon_product')

default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2022, 6, 1),
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=30)
}

dag = DAG(
    'amazon_etl',
    default_args=default_args,
    schedule_interval='0 9 12 * *',
    catchup=False
)

extraction = PythonOperator(
    task_id='extract_data_product',
    python_callable=extract_data,
    dag=dag
)

transformation = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

loading = PythonOperator(
    task_id='load_data_to_db',
    python_callable=load_data,
    dag = dag
)

extraction >> transformation >> loading

