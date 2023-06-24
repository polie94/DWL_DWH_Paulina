import sqlalchemy
from sqlalchemy import create_engine, text
import pandas as pd
import os
import datetime
import json
import psycopg2


def read_rds(datalake_engine, query_read):
    # Establish a connection to the data lake
    with datalake_engine.connect() as datalake_conn:
        # Extract data from the data lake
        select_query = text(query_read)
        datalake_result = datalake_conn.execute(select_query)

        # Get column names from result keys
        column_names = datalake_result.keys()

        # Transform and enrich the data
        df = pd.DataFrame(datalake_result.fetchall(), columns=column_names)

    return df


def write_rds(dwh_engine, query, df, tab):
    # Establish a connection to the data warehouse
    with dwh_engine.connect() as connection:
        connection.execute(query)
        df.to_sql(tab, connection, if_exists='replace', index=False)

    dwh_engine.dispose()


def lambda_handler(event, context):
    # Connect to the data lake
    datalake_engine = create_engine(os.environ["DATALAKE_ENGINE"])

    # Connect to the data warehouse
    dwh_engine = create_engine(os.environ["DWH_ENGINE"])

    query_in = "SELECT * FROM airpolution_aggregated"
    df = read_rds(datalake_engine, query_in)
    df.rename(columns={'desc': 'description'}, inplace=True)
    df.rename(columns={'Date': 'date'}, inplace=True)

    # Create new column as concatenation of county and date
    df['id_county_date'] = df['county'].str.strip() + '-' + df['date'].astype(str)
    print(df.head)

    # Drop unnecessary columns
    df = df.drop(columns=['lat', 'lon'])

    query = '''CREATE TABLE IF NOT EXISTS star_dim_air(id_county_date varchar PRIMARY KEY, county varchar, aqi float,co float,no float,no2 float,o3 float, so2 float, pm2_5 float,pm10 float, nh3 float,date date);'''

    table = "star_dim_air"
    write_rds(dwh_engine, query, df, table)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }