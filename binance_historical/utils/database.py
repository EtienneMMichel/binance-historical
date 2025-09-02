import sqlalchemy as sa
import os
import pandas as pd

class Database():
    def __init__(self, db_config_info):
        user = db_config_info.get("user", None)
        password = db_config_info.get("password", None)
        host = db_config_info.get("host", None)
        port = db_config_info.get("port", None)
        db_name = db_config_info.get("db_name", None)
        self.engine = sa.create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db_name}')

    def query(self, q:str):
        '''
        q  [str]: SQL query 
        '''
        with self.engine.connect() as connection:
            response = connection.execute(sa.text(q))
        return response
    
    def get_table(self, table:str, additional_query:str=""):
        '''
        table [str]: table name
        additional_query [str]: filters
        '''
        q = f"SELECT * FROM public.\"{table}\" " + additional_query
        results = self.query(q)
        df = pd.DataFrame(results)
        return df
    
    def count_elements_in_table(self, table:str, additional_query:str=""):
        q = f"SELECT COUNT(*) FROM public.\"{table}\" " + additional_query
        results = self.query(q)
        df = pd.DataFrame(results)
        return df

    
    def drop_table(self, table:str):
        '''
        table [str]: table to drop
        '''
        q = f"DROP TABLE IF EXISTS {table}"
        return self.query(q)

    def save_dataframe(self, df, table_name, if_exists='append'):
        with self.engine.begin() as connection:
            df.to_sql(table_name,con=connection, if_exists=if_exists)

    def already_in_table(self, path, date):
        return False