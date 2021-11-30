import pandas as pd
import geopandas as gpd
from h3 import h3
import sys
import psycopg2
import psycopg2.extras as extras
import src.heroku_config as config
import os


def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')

        conn = psycopg2.connect(**params_dic)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    return conn


def create_db():
    pass

def execute_values(conn, df, table):
    """
    Using psycopg2.extras.execute_values() to insert the dataframe
    """
    # Create a list of tuples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_values() done")
    cursor.close()


def return_h3_value(h3_index):
    q = f"""SELECT value FROM hcho WHERE h3 = '{h3_index}';"""
    param_dic = {'host': config.DB_HOST, 'database': config.DB_NAME, 'user': config.DB_USER,
                 'password': config.DB_PASS}
    conn = connect(param_dic)
    cursor = conn.cursor()
    try:
        cursor.execute(q)
        data = cursor.fetchall()
        conn.commit()
        return data
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()

def return_value_from_df(h3_index, h4_level):
    hdf = bharat_hcho[bharat_hcho['h3'] == h3_index]
    d = pd.Series(hdf['value'].values, index=hdf.timestamp).to_dict()
    return {'data':{'h3_index':h3_index,'h4_level':h4_level, 'HCHO':d}}

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

DATABASE_URL = os.environ['DATABASE_URL']

conn = psycopg2.connect(config.DATABASE_URL, sslmode='require')

# conn = connect(param_dic)

execute_values(conn, bharat_hcho[['timestamp', 'value','lat','lng','h3']], 'hcho')
