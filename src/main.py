from h3 import h3
from fastapi import FastAPI
import uvicorn
import src.dbutils as dbutils
# import dbutils as dbutils

app = FastAPI()

@app.get('/')
def welcome():

    return {'Message':r'Use: /api/h3_index={h3_index}&h4_level={h4_level}',
            'Example': '/api/h3_index=84209b5ffffffff&h4_level=4'}

# h3_index = '84209b5ffffffff'
@app.get('/api/h3_index={h3_index}&h4_level={h4_level}')
def return_value(h3_index, h4_level:int):
    assert h3.h3_is_valid(h3_index), 'INVALID H3 INDEX'
    data = dbutils.return_h3_value(h3_index)
    # s = [x[0] for x in data]
    # return {'data':{'h3_index':h3_index,'h4_level':h4_level, 'HCHO':s}}
    # methane, carbonmonoxide,ozone,nitrogendioxide
    data_dict = {'data':{'date':data[0][0],'h3_index':data[0][1],'h4_level':h4_level, 'CH4':data[0][2],'CO':data[0][3], 'O3':data[0][4],'NO2':data[0][5]}}
    return data_dict
    # data = dbutils.return_value_from_df(h3_index, h4_level)
    # return data

# if __name__ == '__main__':
#     uvicorn.run(app)