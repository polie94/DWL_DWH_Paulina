import json 
import requests
import pandas as pd 
import boto3
import csv
import psycopg2
import os
from sqlalchemy import create_engine #sqlalchemy==1.4.46                                                       
import datetime


class S3ClientClass:
    def __init__(self, S3_BUCKET_NAME,OBJECT_KEY):
        self.client = boto3.client("s3")
        self.S3_BUCKET_NAME=S3_BUCKET_NAME
        self.OBJECT_KEY=OBJECT_KEY
    
    def read_s3_bucket(self): 
    
        file_content = self.client.get_object(
            Bucket=self.S3_BUCKET_NAME, Key=self.OBJECT_KEY)["Body"]
        airpol_history = pd.read_csv(file_content)
        
        return airpol_history
    
    
class RDSClass:
    def __init__(self, RDS_USER,RDS_PASSWORD,RDS_HOST):
        self.RDS_USER =RDS_USER
        self.RDS_PASSWORD=RDS_PASSWORD
        self.RDS_HOST=RDS_HOST
    def read_rds(self,db):
        conn_string = "postgresql://"+self.RDS_USER+":"+self.RDS_PASSWORD+"@"+self.RDS_HOST+"/"+db
        db_sqlalchemy = create_engine(conn_string)
        conn_sqlalchemy = db_sqlalchemy.connect()
        connection = psycopg2.connect(user=self.RDS_USER,
                                      password=self.RDS_PASSWORD,
                                      host=self.RDS_HOST,
                                      port="5432",
                                      database=db) 
        cursor= connection.cursor()
    
       
        airpol_current= pd.read_sql("select * from airpolution where unix_dt>1681171200",db_sqlalchemy)
        conn_sqlalchemy.invalidate()
        db_sqlalchemy.dispose()
        connection.close()
        return airpol_current 
    def write_rds(self,df, db):
        conn_string = "postgresql://"+self.RDS_USER+":"+self.RDS_PASSWORD+"@"+self.RDS_HOST+"/"+db
        print(1)
        db_sqlalchemy = create_engine(conn_string)
        print(2)
        conn_sqlalchemy = db_sqlalchemy.connect()
        print(3)
        connection = psycopg2.connect(user=self.RDS_USER,
                                      password=self.RDS_PASSWORD,
                                      host=self.RDS_HOST,
                                      port="5432",
                                      database=db) 
        print(4)
        cursor= connection.cursor()
        print(5)
        connection.commit()
        print(6)
        sql = '''CREATE TABLE IF NOT EXISTS airpolution_all_cleaned (lat numeric(7,2), lon numeric(7,2) ,county char(20), aqi numeric, co numeric,
        no numeric, no2 numeric, o3 numeric, so2 numeric, pm2_5 numeric,pm10 numeric, nh3 numeric, unix_dt bigint, "Datetime" timestamp, "Date" date, "Hour" time);'''
        print(7)
        cursor.execute(sql)
        print(8)
        connection.commit()
        print(9)
        sql_del = '''DELETE FROM airpolution_all_cleaned;'''
        print(10)
        cursor.execute(sql_del)
        print(11)
        connection.commit()
        print(12)
        print(df)
        df.to_sql("airpolution_all_cleaned", db_sqlalchemy,  if_exists= 'append', index=False)
        print(13)
        conn_sqlalchemy.invalidate()
        db_sqlalchemy.dispose()
        connection.close()
        return "hier"


def clean_data(df):
    df["Datetime"]=pd.to_datetime( df["unix_dt"],unit="s",utc=True)
    df["Datetime"]=df.Datetime.dt.tz_convert('America/Chicago')
    df["Datetime"]=df.Datetime.apply(lambda x : x.replace(tzinfo=None) )
    df["county"]=df.county.apply(lambda x : x.strip() )

    df['Date'] = df['Datetime'].dt.date
    df['Hour'] = df['Datetime'].dt.time
    df["Hour"] = df["Hour"].apply(lambda x:  datetime.time(x.hour,00))
    return df 
   
def lambda_handler(event, context):
    
    S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
    OBJECT_KEY = os.environ["OBJECT_KEY"]
##rds
    RDS_USER = os.environ["RDS_USER"]
    RDS_PASSWORD = os.environ["RDS_PASSWORD"]
    RDS_HOST=os.environ["RDS_HOST"]
    RDS_DB = os.environ["RDS_DB"]
   
    S3=S3ClientClass(S3_BUCKET_NAME,OBJECT_KEY)
    RDS=RDSClass(RDS_USER,RDS_PASSWORD,RDS_HOST)
    
    data_history=S3.read_s3_bucket()
   # data_history=data_history[data_history["Date"]>='2022-01-01']
    
    print("history done")
    data_current=RDS.read_rds(RDS_DB)
    print("current done")
    print(data_history.county.nunique())
    data=pd.concat([data_history,data_current],axis=0)
    print("concat done")
    data=clean_data(data)
    print("cleaning done")
   # data=data[data["Date"]>='2022-01-01']

    
    RDS.write_rds(data,RDS_DB)

    print(data.info())
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

