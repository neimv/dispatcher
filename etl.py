
import random
import sys
from os import walk

import pandas as pd
from sqlalchemy import create_engine


class ETL:
    def __init__(self):
        self.folder_name = 'datasets'
        # Check if use .env file to this variables
        self.user = 'neimv'
        self.password = 'V:67012:h'
        self.host = 'localhost'
        self.db = 'pyllytics'

    def main(self):
        self._extract()
        self._load()

    def _extract(self):
        self.filenames = next(walk(self.folder_name), (None, None, []))[2]
        # print(self.filenames)

    def _transform(self, df):
        clean_columns = [
            column.replace('.', '_').replace(' ', '_').lower()
            for column in df.columns
        ]

        df.columns = clean_columns

        return df

    def _load(self):
        self.samples_file = random.sample(self.filenames, 30)
        engine = create_engine(
            f'postgresql+psycopg2://'
            f'{self.user}:{self.password}@{self.host}/{self.db}'
        )
        # Check if exists the dataframes
        try:
            pd.read_sql_table('all_dataframes', engine)
            print("The tables exists writte the news? [y/n]")
            write = input(">>> ")

            if write != 'y':
                sys.exit()
        except:
            pass

        for file_name in self.samples_file:
            try:
                df = pd.read_csv(
                    f'{self.folder_name}/{file_name}',
                    compression='gzip'
                )
            except:
                continue

            df = self._transform(df)
            print(df.head())

            df.to_sql(
                f'dataframe_{file_name.lower()}',
                engine,
                if_exists='replace'
            )

        df_saving = pd.DataFrame(
            self.samples_file, columns=['dataframe_registers']
        )
        df_saving.to_sql('all_dataframes', engine)


if __name__ == '__main__':
    etl = ETL()
    etl.main()

