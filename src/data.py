import pandas as pd
import geopandas as gpd
from h3 import h3
import sys
import psycopg2
import psycopg2.extras as extras
import heroku_config as config
import os
import dbutils as dbutils
from sqlalchemy import create_engine


#limit 10000 because Heroku
URL_HCHO = 'https://api.v2.emissions-api.org/api/v2/methane/geo.json?country=IND&begin=2021-01-01&end=2021-11-11&limit=10000&offset=0'

bharat_hcho = gpd.read_file(URL_HCHO)

bharat_hcho['lng'] = bharat_hcho.geometry.x
bharat_hcho['lat'] = bharat_hcho.geometry.y

bharat_hcho['timestamp'] = pd.to_datetime(bharat_hcho['timestamp'])


def lat_lng_to_h3(row, h3_level=4):
    return h3.geo_to_h3(row.geometry.y, row.geometry.x, h3_level)

bharat_hcho['h3'] = bharat_hcho.apply(lat_lng_to_h3, axis=1)
bharat_hcho.set_index(['timestamp'])

param_dic = {'host': config.DB_HOST, 'database': config.DB_NAME, 'user': config.DB_USER,
             'password': config.DB_PASS}

# DATABASE_URL = os.environ[config.DATABASE_URL]
DATABASE_URL = config.DATABASE_URL

conn = psycopg2.connect(config.DATABASE_URL, sslmode='require')

# conn = connect(param_dic)

# dbutils.execute_values(conn, bharat_hcho[['timestamp', 'value','lat','lng','h3']], 'hcho')
 
engine = create_engine(config.DATABASE_URL, echo = False)

bharat_hcho[['timestamp', 'value','lat','lng','h3']].to_sql('hcho', con = engine, if_exists='append')

print(engine.execute('SELECT * FROM hcho').fetchone())