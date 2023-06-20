import json 
import requests
import pandas as pd 
import boto3
import csv
import psycopg2
import os
from sqlalchemy import create_engine #sqlalchemy==1.4.46                                                       
import datetime


    
class RDSClass:
    def __init__(self, RDS_USER,RDS_PASSWORD,RDS_HOST,RDS_DB):
        self.RDS_USER =RDS_USER
        self.RDS_PASSWORD=RDS_PASSWORD
        self.RDS_HOST=RDS_HOST
        self.RDS_DB=RDS_DB
        
    def read_rds(self):
        conn_string = "postgresql://"+self.RDS_USER+":"+self.RDS_PASSWORD+"@"+self.RDS_HOST+"/"+self.RDS_DB
        connection = psycopg2.connect(user=self.RDS_USER,
                                      password=self.RDS_PASSWORD,
                                      host=self.RDS_HOST,
                                      port="5432",
                                      database= self.RDS_DB) 
        db_sqlalchemy = create_engine(conn_string)
        conn_sqlalchemy = db_sqlalchemy.connect()
        cursor= connection.cursor()

        airpol_current= pd.read_sql("select * from airpolution_all_cleaned",db_sqlalchemy)
        conn_sqlalchemy.invalidate()
        db_sqlalchemy.dispose()
        connection.close()
        return airpol_current 
    def write_rds(self,df):
        conn_string = "postgresql://"+self.RDS_USER+":"+self.RDS_PASSWORD+"@"+self.RDS_HOST+"/"+self.RDS_DB
        connection = psycopg2.connect(user=self.RDS_USER,
                                      password=self.RDS_PASSWORD,
                                      host=self.RDS_HOST,
                                      port="5432",
                                      database= self.RDS_DB) 

        db_sqlalchemy = create_engine(conn_string)

        conn_sqlalchemy = db_sqlalchemy.connect()

        cursor= connection.cursor()

        connection.commit()

        sql = '''CREATE TABLE IF NOT EXISTS airpolution_aggregated(lat numeric(7,2), lon numeric(7,2) ,county char(20), aqi numeric, co numeric,
        no numeric, no2 numeric, o3 numeric, so2 numeric, pm2_5 numeric,pm10 numeric, nh3 numeric, "Date" date, "desc" char(20));'''

        cursor.execute(sql)
        connection.commit()

        cursor.execute("Delete from airpolution_aggregated")
        print(8)
        connection.commit()
        tuples = [tuple(x) for x in df.to_numpy()]
      
    # Comma-separated dataframe columns
        #cols = ','.join(list(df.columns))
        cols = ','.join(['lat', 'lon', 'county', 'aqi', 'co', 'no', 'no2', 'o3', 'so2', 'pm2_5', 'pm10', 'nh3',  '"Date"', '"desc"'])
    # SQL quert to execute
        #cursor = connection.cursor()

        values = [cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", tup).decode('utf8') for tup in tuples]
        query  = "INSERT INTO %s(%s) VALUES " % ("airpolution_aggregated", cols) + ",".join(values)
        cursor.execute(query, tuples)
        connection.commit()
        
        conn_sqlalchemy.invalidate()
        db_sqlalchemy.dispose()
        connection.close()
        return "hier"


def aqi_index_desc(x):
    aqi_desc=""
    if x == 1:
        aqi_desc="good"
    elif x == 2:
        aqi_desc="fair"
    elif x==3:
        aqi_desc="moderate"
    elif x==4 : 
        aqi_desc="poor"
    elif x ==5: 
        aqi_desc="very poor"
    return aqi_desc
        
    
def aggregate(df):
    agg=df[['lat', 'lon', 'county', 'Date', "aqi" ,'co', 'no', 'no2', 'o3', 'so2','pm2_5', 'pm10', 'nh3']]
    agg=agg.groupby(['lat', 'lon', 'county', 'Date']).mean().reset_index()
    print(1)
    agg["aqi"]=agg.aqi.apply(lambda x :int(round(x,0)))
    agg["co"]=agg.co.apply(lambda x :round(x,2))
    agg["no"]=agg.no.apply(lambda x :round(x,2))
    agg["no2"]=agg.no2.apply(lambda x :round(x,2))
    agg["o3"]=agg.o3.apply(lambda x :round(x,2))
    agg["so2"]=agg.so2.apply(lambda x :round(x,2))
    agg["pm2_5"]=agg.pm2_5.apply(lambda x :round(x,2))
    agg["pm10"]=agg.pm10.apply(lambda x :round(x,2))
    agg["nh3"]=agg.nh3.apply(lambda x :round(x,2))
        
    print(2)
    agg["desc"] = agg.aqi.apply(lambda x: aqi_index_desc(x))
    print(3)
    #agg["Hour"]=datetime.time(00,00)
    print(4)
    #agg["Datetime"]=agg.Date.apply(lambda x: datetime.datetime.combine(x, datetime.time(00,00)))
    print(5)
    #agg["unix_dt"]=agg.Datetime.apply(lambda x:x.timestamp() )
    print(6)
    agg=agg[['lat', 'lon', 'county', 'aqi', 'co', 'no', 'no2', 'o3', 'so2', 'pm2_5', 'pm10', 'nh3',  'Date', 'desc']]
    return agg
    
    
def lambda_handler(event, context):
    RDS_USER = os.environ["RDS_USER"]
    RDS_PASSWORD = os.environ["RDS_PASSWORD"]
    RDS_HOST=os.environ["RDS_HOST"]
    RDS_DB = os.environ["RDS_DB"]
   
    
    RDS=RDSClass(RDS_USER,RDS_PASSWORD,RDS_HOST,RDS_DB)
    data_in= RDS.read_rds()

    agg=aggregate(data_in)
    today=datetime.date.today()
    agg=agg[agg["Date"]<today]

    RDS.write_rds(agg) 
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
