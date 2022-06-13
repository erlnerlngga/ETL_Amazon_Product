from db import init_db, load
from helper_transform import clean_laptop_phone_bed_furniture, clean_tablet_camera_cook

import psycopg2
import psycopg2.extras
import os
import glob
import json
import pandas as pd
import wget

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
        if os.path.exists(f"/opt/airflow/data/{file_name[:6]}"):
            os.remove(f"/opt/airflow/data/{file_name[:6]}")
        wget.download(url+file_name,f"/opt/airflow/data/{file_name[:6]}")

def transform():
    file = glob.glob(f'/opt/airflow/data/*')

    for i in file:
        # print(i[40:46], end=' ')
        locals()[i[-6:]] = load(i)
        locals()[i[-6:]] = pd.DataFrame(locals()[i[-6:]])

    df_name = ['laptop', 'phones', 'bedroo', 'furnit', 'tablet', 'camera', 'cookwa']
    for df in range(len(df_name)):
        if df > 3:
            locals()[df_name[df]] = clean_tablet_camera_cook(locals()[df_name[df]])
        else:
            locals()[df_name[df]] = clean_laptop_phone_bed_furniture(locals()[df_name[df]])

    merge_df = pd.concat([laptop, phones, tablet, camera, bedroo, furnit, cookwa], ignore_index=True)
    temp = '/opt/airflow/data/amazon_product.csv'
    df.to_csv(temp, sep=';', index=False, header=False)


def load_data():
    init_db()
    load(amazon_product)

# if __name__ == '__main__':
#     merge_df = transform()
#     load_data(merge_df)
