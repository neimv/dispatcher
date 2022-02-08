
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
        self.password = 'prueba_neimv'
        self.host = 'localhost'
        self.db = 'neimv'
        self.output_folder = "/var/dispatcher"

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
        self.samples_file = self.filenames
        engine_pg = create_engine(
            f'postgresql+psycopg2://'
            f'{self.user}:{self.password}@{self.host}/{self.db}'
        )
        engine_my = create_engine(
            f'mysql+pymysql://'
            f'{self.user}:{self.password}@{self.host}/{self.db}'
        )
        engine_maria = create_engine(
            f'mysql+pymysql://'
            f'{self.user}:{self.password}@{self.host}:3307/{self.db}'
        )

        for file_name in self.samples_file:
            try:
                df = pd.read_csv(
                    f'{self.folder_name}/{file_name}',
                    compression='gzip'
                )
            except Exception:
                continue

            df = self._transform(df)
            file_name = file_name.replace('.csv.tar.gz', '')
            print(file_name)

            if file_name == '1M':
                continue

            try:
                # Saving data
                df.to_sql(
                    f'dataframe_{file_name.lower()}',
                    engine_pg,
                    if_exists='replace'
                )
                df.to_sql(
                    f'dataframe_{file_name.lower()}',
                    engine_my,
                    if_exists='replace'
                )
                df.to_sql(
                    f'dataframe_{file_name.lower()}',
                    engine_maria,
                    if_exists='replace'
                )
                df.to_csv(
                    f'{self.output_folder}/csv/{file_name}.csv',
                    index=False
                )
                df.to_excel(
                    f'{self.output_folder}/excel/{file_name}.xlsx',
                    index=False
                )
                df.to_parquet(
                    f'{self.output_folder}/parquet/{file_name}.parquet',
                    index=False
                )
                df.to_json(
                    f'{self.output_folder}/json/{file_name}.json',
                    orient='records'
                )
            except Exception as e:
                print(e)

        df_saving = pd.DataFrame(
            self.samples_file, columns=['dataframe_registers']
        )
        df_saving.to_sql('all_dataframes', engine_pg)


if __name__ == '__main__':
    etl = ETL()
    etl.main()

