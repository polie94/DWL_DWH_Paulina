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
    # Connect to the data warehouse using the engine provided
    with dwh_engine.connect() as connection:
        # Begin a new transaction
        with connection.begin():
            # Run the query
            connection.execute(query)
            # Write the DataFrame to SQL
            df.to_sql(tab, connection, if_exists='replace', index=False)

    # Clean up the engine
    dwh_engine.dispose()


def lambda_handler(event, context):
    # Connect to the data lake
    datalake_engine = create_engine(os.environ["DATALAKE_ENGINE"])

    # Connect to the data warehouse
    dwh_engine = create_engine(os.environ["DWH_ENGINE"])

    query_in = "SELECT * FROM weather_data"
    df = read_rds(datalake_engine, query_in)

    # Create new column as concatenation of county and date
    df['id_county_date'] = df['county'] + '-' + df['date'].astype(str)
    print(df.head)

    query = '''CREATE TABLE IF NOT EXISTS star_dim_weather(id_county_date varchar PRIMARY KEY, county varchar,date date,temp_min float,temp_max float,app_temp_min float,app_temp_max float,precipitation float, precipitation_hours float, snowfall float,sunrise time, sunset time);'''

    table = "star_dim_weather"
    write_rds(dwh_engine, query, df, table)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
