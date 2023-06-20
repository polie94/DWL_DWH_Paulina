import json 
import requests
import pandas as pd  
import boto3
import csv
import psycopg2
import os
from sqlalchemy import create_engine #sqlalchemy==1.4.46                                                       
import datetime


def read_s3_bucket(bucket,object_key): 
        client = boto3.client("s3")
        
        file_content =client.get_object(
            Bucket=bucket, Key=object_key)["Body"]
        airpol_history = pd.read_csv(file_content)
        
        return airpol_history
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

        connection = psycopg2.connect(user=rds_user_out,
                                      password=rds_password_out,
                                      host=reds_host_out,
                                      port="5432",
                                      database=db) 

        cursor= connection.cursor()
        connection.commit()
        #creat tab if not exist
        cursor.execute(query_read)
        connection.commit()
        #delete data
        cursor.execute("Delete from "+table)
        connection.commit()

        #from https://naysan.ca/2020/08/02/pandas-to-postgresql-using-psycopg2-mogrify-then-execute/
        tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
        cols = ','.join(list(df.columns))
    # SQL quert to execute

        values = [cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s)", tup).decode('utf8') for tup in tuples]
        query  = "INSERT INTO %s(%s) VALUES " % (table, cols) + ",".join(values)
        cursor.execute(query, tuples)
        connection.commit()

        connection.close()
        return "Done"

def lambda_handler(event, context):
    

    RDS_USER_OUT=os.environ["RDS_USER_OUT"]
    RDS_PASSWORD_OUT=os.environ["RDS_PASSWORD_OUT"]
    RDS_HOST_OUT=os.environ["RDS_HOST_OUT"]
    RDS_DB_OUT=os.environ["RDS_DB_OUT"]    
    S3_BUCKET_NAME=os.environ["S3_BUCKET_NAME"]
    
    tex_counties=read_s3_bucket(S3_BUCKET_NAME,"Texas_Counties_Centroid_Map.csv")
    us_cities=read_s3_bucket(S3_BUCKET_NAME,"uscities.csv")
    data_tx=us_cities[us_cities["state_name"]=="Texas"]
    #add county info
    df=data_tx.merge(tex_counties, left_on="county_name",right_on="CNTY_NM") 
    df=df[['city', 'state_name','county_name', 'lat', 'lng','X (Lat)', 'Y (Long)']]
    df=df.rename(columns={'X (Lat)':"lat_air", 'Y (Long)':"lng_air", 'city':'city_county'})
    #add index
    df["id_city"]=df["city_county"]
    #query creat tab
    query_out='''CREATE TABLE IF NOT EXISTS star_dim_geo (id_city varchar, city_county varchar, state_name varchar,county_name varchar, lat float, lng float, lat_air float , lng_air float, primary key(city_county,county_name));'''
    table="star_dim_geo"
    #write to rds
    write_rds(RDS_USER_OUT, RDS_PASSWORD_OUT, RDS_HOST_OUT, RDS_DB_OUT,query_out,df,table)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
