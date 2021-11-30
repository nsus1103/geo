import pandas as pd
import geopandas as gpd
from h3 import h3
import sys
from pandas.io.sql import table_exists
import psycopg2
import psycopg2.extras as extras
import heroku_config as config
import os
import src.dbutils as dbutils
from sqlalchemy import create_engine


def lat_lng_to_h3(row, h3_level=4):
    return h3.geo_to_h3(row.geometry.y, row.geometry.x, h3_level)



def load_data(engine):

    emissions_df = pd.DataFrame()
    cols = ['dates', 'h3', 'lat', 'lng']
    products = ['methane', 'carbonmonoxide', 'ozone', 'nitrogendioxide']

    for product in products:
        #limit 10000 because Heroku, pulling 2500 rows for each product
        url = f'https://api.v2.emissions-api.org/api/v2/{product}/geo.json?country=IND&begin=2021-01-01&end=2021-11-11&limit=2500&offset=0'

        print(f'Pulling records for {product}')
        product_data = gpd.read_file(url)
        print('Pull success')

        product_data['lng'] = product_data.geometry.x
        product_data['lat'] = product_data.geometry.y

        product_data['timestamp'] = pd.to_datetime(product_data['timestamp'])

        product_data['h3'] = product_data.apply(lat_lng_to_h3, axis=1)

        product_data['dates'] = pd.to_datetime(product_data['timestamp']).dt.date

        daily_data = product_data.groupby(['h3', 'dates'], as_index=False).mean('value')

        daily_data.set_index(['dates'])

        daily_data = daily_data.rename(columns={'value':f'{product}'})

        # cols = ['dates', 'h3', 'lat', 'lng']
        if emissions_df.empty:
            cols.extend([f'{product}'])
            emissions_df = daily_data.copy()
        else:
            cols.extend([f'{product}'])
            emissions_df = emissions_df.merge(daily_data[['dates','h3', f'{product}']], on=['dates', 'h3'])[cols]

    emissions_df.to_sql('emissions', con = engine, if_exists='append')
    return

DATABASE_URL = config.DATABASE_URL
engine = create_engine(config.DATABASE_URL, echo = False)

first_row = engine.execute('SELECT * FROM emissions').fetchone()

if first_row is None:
    print('Populating data')
    load_data(engine)
    print('Populate successful')
