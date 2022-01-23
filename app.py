
import pandas as pd
from flask import Flask
from flask_restful import Resource, Api, abort
from sqlalchemy import create_engine


#############################################################################
# App flask
#############################################################################
app = Flask(__name__)
api = Api(app)

# @TODO, change this to good configs
user = 'neimv'
password = 'V:67012:h'
host = 'localhost'
db = 'pyllytics'
engine = create_engine(
    f'postgresql+psycopg2://'
    f'{user}:{password}@{host}/{db}'
)


class DataframesList(Resource):
    def get(self):
        try:
            df = pd.read_sql_table('all_dataframes', engine)
            raise Exception('bye')
            df['dataframe_registers'] = df['dataframe_registers'].apply(
                lambda x: x.replace('.', '_').replace(' ', '_').lower()
            )
            response = df.to_dict(orient='records')
        except:
            abort(500)

        return response


api.add_resource(DataframesList, '/')
