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
      #  conn_string = "postgresql://"+rds_user_out+":"+rds_password_out+"@"+reds_host_out+"/"+db
       # db_sqlalchemy = create_engine(conn_string)
       # conn_sqlalchemy = db_sqlalchemy.connect()
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
        cols = ','.join(list(df.columns))
        print(cols)
    # SQL quert to execute
        #cursor = connection.cursor()
        print("values")
        values = [cursor.mogrify("(%s,%s,%s,%s,%s,%s)", tup).decode('utf8') for tup in tuples]
        print("queries")
        query  = "INSERT INTO %s(%s) VALUES " % (table, cols) + ",".join(values)
        print(query)
        cursor.execute(query, tuples)
        connection.commit()
        print(13)
       # conn_sqlalchemy.invalidate()
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
    S3_BUCKET_NAME=os.environ["S3_BUCKET_NAME"]
    
    tex_counties=read_s3_bucket(S3_BUCKET_NAME,"Texas_Counties_Centroid_Map.csv")
    us_cities=read_s3_bucket(S3_BUCKET_NAME,"uscities.csv")
    data_tx=us_cities[us_cities["state_name"]=="Texas"]


    query_in="select * from gun_violance_texas"

    df=read_rds(RDS_USER_IN, RDS_PASSWORD_IN, RDS_HOST_IN, RDS_DB_IN,query_in)
    df=df.rename(columns={"Incident_Id":'incident_id',"Incident Date": 'incident date',"city-county":'county_city'})
    df=df.merge(data_tx,left_on="county_city", right_on="city")
    df=df[['incident_id', 'incident date', 'county_city', 'killed', 'injured',"county_name"]]
    df["incident date"]=df["incident date"].astype(str)
    print(df)
    df["id_county_date"]=df[["county_name", "incident date"]].apply("-".join, axis=1)
    df["id_date"]=df["incident date"]
    df["id_city"]=df["county_city"]
    df=df[["id_county_date","id_date","id_city",'incident_id', 'killed', 'injured']]  
    #df=df.rename(columns={"# killed":"killed",'# injured':'injured','incident id':'incident_id'})
    query_out='''CREATE TABLE IF NOT EXISTS star_fact_gun (id_county_date varchar, id_date date, id_city varchar, incident_id bigint primary key, killed int , injured int);'''
    table="star_fact_gun" 
    write_rds(RDS_USER_OUT, RDS_PASSWORD_OUT, RDS_HOST_OUT, RDS_DB_OUT,query_out,df,table)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
