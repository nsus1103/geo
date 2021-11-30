import pandas as pd
import geopandas as gpd
from h3 import h3
import sys
import psycopg2
import psycopg2.extras as extras
from sqlalchemy import create_engine
# import src.heroku_config as config
import heroku_config as config
import os


def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')

        # conn = psycopg2.connect(**params_dic)
        DATABASE_URL = os.environ.get('DATABASE_URL')
        conn = psycopg2.connect(config.DATABASE_URL, sslmode='require')
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
    
    conn = connect()
    cursor = conn.cursor()

    # table_exists_flag = cursor.execute('SELECT * FROM hcho').fetchone()
    # print(table_exists_flag)
    
    # q = f"""SELECT methane FROM emissions WHERE h3 = '{h3_index}';"""
    q = f"""SELECT *
            FROM (SELECT dates, h3, methane, carbonmonoxide,ozone,nitrogendioxide, row_number() OVER (PARTITION BY h3 ORDER BY CAST(dates AS DATE) DESC) AS date_rank
            FROM emissions
            WHERE h3 = '{h3_index}') t
            WHERE date_rank = 1;"""
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

# def return_value_from_df(h3_index, h4_level):
#     hdf = bharat_hcho[bharat_hcho['h3'] == h3_index]
#     d = pd.Series(hdf['value'].values, index=hdf.timestamp).to_dict()
#     return {'data':{'h3_index':h3_index,'h4_level':h4_level, 'HCHO':d}}
