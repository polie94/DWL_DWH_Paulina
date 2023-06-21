import json 
import requests
import pandas as pd
import boto3
import csv
import psycopg2
import os
from sqlalchemy import create_engine #sqlalchemy==1.4.46                                                       
from time import sleep


API_KEY=os.environ["API_KEY"]
S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
OBJECT_KEY = os.environ["OBJECT_KEY"]
##rds
RDS_USER = os.environ["RDS_USER"]
RDS_PASSWORD = os.environ["RDS_PASSWORD"]
RDS_HOST=os.environ["RDS_HOST"]
RDS_DB = os.environ["RDS_DB"]

s3_client = boto3.client("s3")
def write_rds(df, user=RDS_USER,password=RDS_PASSWORD,host=RDS_HOST, db=RDS_DB):
    conn_string = "postgresql://"+user+":"+password+"@"+host+"/"+db
    db_sqlalchemy = create_engine(conn_string)
    conn_sqlalchemy = db_sqlalchemy.connect()
    connection = psycopg2.connect(user=user,
                                  password=password,
                                  host=host,
                                  port="5432",
                                  database=db) 
    cursor= connection.cursor()
    #sq_delete = '''delete from airpolution2;'''
    #cursor.execute(sq_delete)
    connection.commit()
    sql = '''CREATE TABLE IF NOT EXISTS airpolution(lat numeric(7,2), lon numeric(7,2) ,county char(20), aqi numeric, co numeric,
    no numeric, no2 numeric, o3 numeric, so2 numeric, pm2_5 numeric,pm10 numeric, nh3 numeric, unix_dt bigint);'''
    cursor.execute(sql)
    connection.commit()
    sql1='''select * from airpolution;'''
    #print("hier")
    #cursor.execute(sql1)
   
    df.to_sql('airpolution', db_sqlalchemy,  if_exists= 'append', index=False)
    print( pd.read_sql(sql1,db_sqlalchemy))
    conn_sqlalchemy.invalidate()
    db_sqlalchemy.dispose()
    connection.close()
    return "hier"
    
def read_api(api_key, lat, lon, county):
    query = {'lat' : lat, 'lon': lon,'appid': API_KEY }
    print( "query ", query )
    sleep(1.1)
    response = requests.get('http://api.openweathermap.org/data/2.5/air_pollution',params=query)
    dictionary_temp=dict()
    json_string=response.json()
    print(json_string)
    print(json_string)
    aqi=json_string['list'][0]["main"]
   #print(aqi, type(aqi))
    conditions=json_string['list'][0]["components"]
    unix_dt=json_string['list'][0]["dt"]
    location={"lat":lat,"lon":lon, "county":county}
    dictionary_temp.update(location)
    dictionary_temp.update(aqi)
    dictionary_temp.update(conditions)
    dictionary_temp.update({"unix_dt":unix_dt})
    return dictionary_temp
    
def read_s3_bucket(bucket,object_key): 
    
    file_content = s3_client.get_object(
        Bucket=bucket, Key=object_key)["Body"]
    location = pd.read_csv(file_content)
    loc_info=location[['CNTY_NM','X (Lat)', 'Y (Long)']]
    loc_info["lat_round"]=loc_info['X (Lat)'].apply(lambda x:round(x,2))
    loc_info["lng_round"]=loc_info['Y (Long)'].apply(lambda x:round(x,2))
    return loc_info
    
def lambda_handler(event, context):
    location=read_s3_bucket(S3_BUCKET_NAME,OBJECT_KEY)
    #location=location[0:2]
    dictionaries=[]
    for index, row in location.iterrows():
       position = read_api(API_KEY,row["lat_round"],row["lng_round"],row["CNTY_NM"])
       dictionaries.append(position)
    df_airpolution=pd.DataFrame(dictionaries)
    print(df_airpolution)
    write_rds(df=df_airpolution)
    return {
            'statusCode': 200,
            'body': json.dumps("hello")
        }
