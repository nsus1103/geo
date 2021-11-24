from h3 import h3
from fastapi import FastAPI
import uvicorn
import src.from_db as from_db

app = FastAPI()

@app.get('/')
def welcome():

    return {'Message':r'Use: /api/h3_index={h3_index}&h4_level={h4_level}'}

# h3_index = '843c123ffffffff'
@app.get('/api/h3_index={h3_index}&h4_level={h4_level}')
def return_value(h3_index, h4_level:int):
    assert h3.h3_is_valid(h3_index), 'INVALID H3 INDEX'
    data = from_db.return_h3_value(h3_index)
    s = [x[0] for x in data]
    return {'data':{'h3_index':h3_index,'h4_level':h4_level, 'HCHO':s}}
    # data = from_db.return_value_from_df(h3_index, h4_level)
    # return data

if __name__ == '__main__':
    uvicorn.run(app)