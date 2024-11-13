from dotenv import load_dotenv
import os
import pandas as pd
from clickhouse_driver import Client
from urllib import parse
import sqlalchemy as db
import clickhouse_connect
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy import dialects
import sqlalchemy

env_path = '.env'
load_dotenv(dotenv_path=env_path)

def get_data_cc(file_name):
    client = Client(host=os.getenv("CH_HOST"),
                    user=os.getenv("CH_USER"),
                    password=os.getenv("CH_PASSWORD"),
                    database=os.getenv("CH_DB"))
    with open(file_name, 'r') as f:
        query = f.read()
        result = client.query_dataframe(query)
    return result

def get_yaake_data(db_name, file_name):
    host = os.getenv("YAAKE_HOST")
    # print(host)
    username = os.getenv("YAAKE_USER")
    # print(username)
    password = parse.quote(os.getenv("YAAKE_PASSWORD"))
    # print(password)
    port = os.getenv("YAAKE_PORT")
    # print(port)

    db = 'postgresql://'+username+':'+password+'@'+host+':5432/'+ db_name
    engine = create_engine(db)
    # connection = engine.connect()
    with open(file_name, 'r') as f:
        read_query = f.read()
        # print(read_query)
        with engine.connect() as connection:
            return pd.DataFrame(connection.execute(text(read_query)))