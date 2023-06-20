import json 
import requests
import pandas as pd  
import boto3
import csv
import psycopg2
import os
from sqlalchemy import create_engine #sqlalchemy==1.4.46                                                       
import datetime



def read_rds(rds_user_in, rds_password_in, reds_host_in, db,query_read):
        conn_string = "postgresql://"+rds_user_in+":"+rds_password_in+"@"+reds_host_in+"/"+db
        db_sqlalchemy = create_engine(conn_string)
        conn_sqlalchemy = db_sqlalchemy.connect()
        connection = psycopg2.connect(user=rds_user_in,
                                      password=rds_password_in,
                                      host=reds_host_in,
                                      port="5432",
                                      database=db) 
        cursor= connection.cursor()
    
       
        df= pd.read_sql(query_read,db_sqlalchemy)
        conn_sqlalchemy.invalidate()
        db_sqlalchemy.dispose()
        connection.close()
        return df 
        
def write_rds(rds_user_out, rds_password_out, reds_host_out, db,query_read,df,table):
        conn_string = "postgresql://"+rds_user_out+":"+rds_password_out+"@"+reds_host_out+"/"+db
        db_sqlalchemy = create_engine(conn_string)
        conn_sqlalchemy = db_sqlalchemy.connect()
        connection = psycopg2.connect(user=rds_user_out,
                                      password=rds_password_out,
                                      host=reds_host_out,
                                      port="5432",
                                      database=db) 

        cursor= connection.cursor()
        connection.commit()
        cursor.execute(query_read)
        print(8)
        connection.commit()
        
        cursor.execute("Delete from "+table)
        print(8)
        connection.commit()
        print(12)
        print(df)
        
        #from https://naysan.ca/2020/08/02/pandas-to-postgresql-using-psycopg2-mogrify-then-execute/
        tuples = [tuple(x) for x in df.to_numpy()]
      
    # Comma-separated dataframe columns
        #cols = ','.join(list(df.columns))
        cols = ','.join(['"Date"', "weekday", "day", "month", "year"])
    # SQL quert to execute
        #cursor = connection.cursor()

        values = [cursor.mogrify("(%s,%s,%s,%s,%s)", tup).decode('utf8') for tup in tuples]
        query  = "INSERT INTO %s(%s) VALUES " % (table, cols) + ",".join(values)
        cursor.execute(query, tuples)
        connection.commit()
        
        conn_sqlalchemy.invalidate()
        db_sqlalchemy.dispose()
        connection.close()
        return "Done"

def lambda_handler(event, context):
    RDS_USER_IN=os.environ["RDS_USER_IN"]
    RDS_PASSWORD_IN=os.environ["RDS_PASSWORD_IN"]
    RDS_HOST_IN=os.environ["RDS_HOST_IN"]
    RDS_DB_IN=os.environ["RDS_DB_IN"]
    RDS_USER_OUT=os.environ["RDS_USER_OUT"]
    RDS_PASSWORD_OUT=os.environ["RDS_PASSWORD_OUT"]
    RDS_HOST_OUT=os.environ["RDS_HOST_OUT"]
    RDS_DB_OUT=os.environ["RDS_DB_OUT"]    
    
    query_in="select * from airpolution_aggregated"

    df=read_rds(RDS_USER_IN, RDS_PASSWORD_IN, RDS_HOST_IN, RDS_DB_IN,query_in)
    star=df[["Date"]]
# add date relevant information
    star["Date"] = pd.to_datetime(star["Date"])
    star["weekday"]=star["Date"].dt.dayofweek
    star["day"]=star["Date"].dt.day
    star["month"]=star["Date"].dt.month
    star["year"]=star["Date"].dt.year
 
    
    query_out='''CREATE TABLE IF NOT EXISTS star_dim_date ("Date" date, weekday int, day int, month int, year int);'''
    table="star_dim_date"
    write_rds(RDS_USER_OUT, RDS_PASSWORD_OUT, RDS_HOST_OUT, RDS_DB_OUT,query_out,star,table)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
