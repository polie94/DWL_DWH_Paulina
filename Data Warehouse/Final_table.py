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
        
        #cursor= connection.cursor()
    
       
        df= pd.read_sql(query_read,db_sqlalchemy)
        conn_sqlalchemy.invalidate()
        db_sqlalchemy.dispose()
        #connection.close()
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
        
        cols=','.join(["county","temp_max","temp_min",'"desc"',"temp_factor","temp_index","gun_date","killed","injured","county_name","lat","lng","lat_air","long_air","aqi_index"])
    # SQL quert to execute
        
        values = [cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", tup).decode('utf8') for tup in tuples]
        print(table)
        print(values)
        query  = "INSERT INTO %s(%s) VALUES " % (table, cols) + ",".join(values)
        
        cursor.execute(query, tuples)
        connection.commit()

        connection.close()
        return "Done"
def temp_index(x):
    if x in ['< -15', '-15 to -10', '-10 to -5', '-5 to 0', '0 to 5','5 to 10','10 to 15']:
        return 1
        
    if x in ['15 to 20', '20 to 25'] :
        return 2
        
    if x in ['25 to 30', '30 to 35'] :
        return 3
        
    if x in [ '35 to 40', '>40'] :
        return 4

def lambda_handler(event, context):
    

    RDS_USER_OUT=os.environ["RDS_USER_OUT"]
    RDS_PASSWORD_OUT=os.environ["RDS_PASSWORD_OUT"]
    RDS_HOST_OUT=os.environ["RDS_HOST_OUT"]
    RDS_DB_OUT=os.environ["RDS_DB_OUT"]    
    #read all tables
    gun=read_rds(RDS_USER_OUT, RDS_PASSWORD_OUT, RDS_HOST_OUT, RDS_DB_OUT,"select * from star_fact_gun")
    county=read_rds(RDS_USER_OUT, RDS_PASSWORD_OUT, RDS_HOST_OUT, RDS_DB_OUT,"select * from star_dim_geo")
    air=read_rds(RDS_USER_OUT, RDS_PASSWORD_OUT, RDS_HOST_OUT, RDS_DB_OUT,"select * from star_dim_air")
    weather=read_rds(RDS_USER_OUT, RDS_PASSWORD_OUT, RDS_HOST_OUT, RDS_DB_OUT,"select * from star_dim_weather")
    #merge all tables into one for BI
    df=pd.merge(gun,county, left_on="id_city",right_on="id_city")#    
   
    df=df[['id_county_date', 'id_date', 'id_city', 'incident_id', 'killed','injured', 'city_county', 'county_name', 'lat', 'lng','lat_air', 'lng_air']]
    df=pd.merge(df,air, left_on="id_county_date",right_on="id_county_date")
    df=pd.merge(df,weather, left_on="id_county_date",right_on="id_county_date")

    df=df[["city_county","temp_max","temp_min","description","id_date","killed","injured","county_name","lat","lng","lat_air","lng_air","aqi"]]
    
    #add columnd for temperature factors and temperature index
    df['temp_factor'] = pd.cut(df['temp_max'].astype(float), bins=range(-20, 50, 5),
                                  labels=['< -15', '-15 to -10', '-10 to -5', '-5 to 0', '0 to 5',
                                          '5 to 10', '10 to 15', '15 to 20', '20 to 25', '25 to 30',
                                          '30 to 35', '35 to 40', '>40'])
    df["temp_index"]=df.temp_factor.apply(lambda x:temp_index(x))
    #rename columns
    df=df.rename(columns={"city_county":"county","description":"desc","id_date":"gun_date","lng_air":"long_air","aqi":"aqi_index"})
    df = df.reindex(columns=['county','temp_max','temp_min','desc','temp_factor','temp_index','gun_date','killed','injured','county_name','lat','lng','lat_air','long_air','aqi_index'])
    #query creat tab
    query_out='''CREATE TABLE IF NOT EXISTS final_tab(county varchar(20),temp_max float,temp_min float ,"desc" varchar(10),temp_factor varchar(10),temp_index int ,gun_date date,killed bigint,injured bigint,county_name varchar(20),lat float,lng float,lat_air float,long_air float,aqi_index int);'''
    table="final_tab"
    #write to rds
    print(df)
    write_rds(RDS_USER_OUT, RDS_PASSWORD_OUT, RDS_HOST_OUT, RDS_DB_OUT,query_out,df,table)
    return {
        'statusCode': 200,
        'body': json.dumps('Done')
    }
