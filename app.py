
import json as js

import pandas as pd
import yaml as yml
from dicttoxml import dicttoxml
from flask import Flask, make_response
from flask_restful import Resource, Api, abort, reqparse
from sqlalchemy import create_engine


#############################################################################
# App flask
#############################################################################
app = Flask(__name__)
api = Api(app)

parser = reqparse.RequestParser()
parser.add_argument('format')

# @TODO, change this to good configs
user = 'neimv'
password = 'prueba_neimv'
host = 'localhost'
db = 'neimv'
engine = create_engine(
    f'postgresql+psycopg2://'
    f'{user}:{password}@{host}/{db}'
)


class DataframesList(Resource):
    def get(self):
        try:
            df = pd.read_sql_table('all_dataframes', engine)
            df['dataframe_registers'] = df['dataframe_registers'].apply(
                lambda x: x.lower()
            )
            response = df.to_dict(orient='records')
        except Exception as e:
            abort(500, custom=f"ERROR: {e}")

        return response


class DataFrameGet(Resource):
    def get(self, name: str):
        args = parser.parse_args()
        format_ = args.get('format', 'json')

        try:
            df = pd.read_sql_table(f'dataframe_{name}', engine)
            df.fillna(0, inplace=True)
            response = df.to_dict(orient='records')

            if format_ == 'xml':
                content_type = 'application/xml'
                response = dicttoxml(response, custom_root=name)
            elif format_ == 'json':
                content_type = 'application/json'
                response = js.dumps(response)
            else:
                content_type = 'application/json'
                response = js.dumps(response)
        except Exception as e:
            abort(404, custom=f'ERROR: {e}')

        response = make_response(response)
        response.headers['content-type'] = content_type

        return response


api.add_resource(DataframesList, '/')
api.add_resource(DataFrameGet, '/<string:name>')
